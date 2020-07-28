#include <linux/module.h>
#include <linux/delay.h>
#include <linux/bitmap.h>
#include <linux/seq_file.h>
#include <linux/atomic.h>
#include <linux/proc_fs.h>
#include <linux/kthread.h>
#include <linux/random.h>
#include <linux/io.h>
#include <linux/string.h>
#include <linux/kernel.h>
#include <linux/sched_clock.h>
#include <linux/jiffies.h>

#include <rdma/rdma_cm.h>
#include <rdma/ib_verbs.h>

#include "rpc.h"
#include "main.h"
#include "ring_buffer.h"

extern int cr_on;
extern int debug;
extern struct rdma_handle *rdma_handles[];
extern struct rdma_handle *backup_rh[];

static struct rdma_work *__get_rdma_work(struct rdma_handle *rh, dma_addr_t dma_addr, size_t size, dma_addr_t rdma_addr, u32 rdma_key);
static struct rdma_work *__get_rdma_work_nonsleep(struct rdma_handle *rh, dma_addr_t dma_addr, size_t size, dma_addr_t rdma_addr, u32 rdma_key);
//static void __put_rdma_work(struct rdma_handle *rh, struct rdma_work *rw);
static void __put_rdma_work_nonsleep(struct rdma_handle *rh, struct rdma_work *rw);

static struct rdma_work *__get_rdma_work(struct rdma_handle *rh, dma_addr_t dma_addr, size_t size, dma_addr_t rdma_addr, u32 rdma_key)
{
	struct rdma_work *rw;
	might_sleep();

	spin_lock(&rh->rdma_work_head_lock);

	if (!rh->rdma_work_head) {
		spin_unlock(&rh->rdma_work_head_lock);
		return NULL;
	}

	rw = rh->rdma_work_head;
	rh->rdma_work_head = rh->rdma_work_head->next;
	spin_unlock(&rh->rdma_work_head_lock);

	rw->sgl.addr = dma_addr;
	rw->sgl.length = size;

	rw->wr.remote_addr = rdma_addr;
	rw->wr.rkey = rdma_key;
	return rw;
}

static struct rdma_work *__get_rdma_work_nonsleep(struct rdma_handle *rh, dma_addr_t dma_addr, 
		size_t size, dma_addr_t rdma_addr, u32 rdma_key)
{
	struct rdma_work *rw;
#ifdef CONFIG_MCOS_IRQ_LOCK
	unsigned long flags;
#endif

#ifdef CONFIG_MCOS_IRQ_LOCK
	spin_lock_irqsave(&rh->rdma_work_head_lock, flags);
#else
	spin_lock(&rh->rdma_work_head_lock);
#endif

	if (!rh->rdma_work_head) {
#ifdef CONFIG_MCOS_IRQ_LOCK
		spin_unlock_irqrestore(&rh->rdma_work_head_lock, flags);
#else
		spin_unlock(&rh->rdma_work_head_lock);
#endif
		return NULL;
	}

	rw = rh->rdma_work_head;
	rh->rdma_work_head = rh->rdma_work_head->next;
#ifdef CONFIG_MCOS_IRQ_LOCK
	spin_unlock_irqrestore(&rh->rdma_work_head_lock, flags);
#else
	spin_unlock(&rh->rdma_work_head_lock);
#endif

	rw->sgl.addr = dma_addr;
	rw->sgl.length = size;

	rw->wr.remote_addr = rdma_addr;
	rw->wr.rkey = rdma_key;
	return rw;
}

/*
static void __put_rdma_work(struct rdma_handle *rh, struct rdma_work *rw)
{
	might_sleep();
	spin_lock(&rh->rdma_work_head_lock);
	rw->next = rh->rdma_work_head;
	rh->rdma_work_head = rw;
	spin_unlock(&rh->rdma_work_head_lock);
}
*/

static void __put_rdma_work_nonsleep(struct rdma_handle *rh, struct rdma_work *rw)
{
#ifdef CONFIG_MCOS_IRQ_LOCK
	unsigned long flags;
#endif

#ifdef CONFIG_MCOS_IRQ_LOCK
	spin_lock_irqsave(&rh->rdma_work_head_lock, flags);
#else
	spin_lock(&rh->rdma_work_head_lock);
#endif
	rw->next = rh->rdma_work_head;
	rh->rdma_work_head = rw;
#ifdef CONFIG_MCOS_IRQ_LOCK
	spin_unlock_irqrestore(&rh->rdma_work_head_lock, flags);
#else
	spin_unlock(&rh->rdma_work_head_lock);
#endif
}

int rmm_alloc(int nid, u64 vaddr)
{
	int offset, ret = 0;
	uint8_t *dma_buffer, *args;
	struct rpc_header *rhp;
	dma_addr_t dma_addr, remote_dma_addr;
	struct rdma_handle *rh;	
	struct rdma_work *rw;
	const struct ib_send_wr *bad_wr = NULL;
	int *done;

	/*
	------------------------------------------
	|rpc_header| vaddr(8bytes) | done(4bytes)|
	------------------------------------------
	*/
	int buffer_size = sizeof(struct rpc_header) + 12;
	int payload_size = sizeof(struct rpc_header) + 8;
	int index = nid_to_rh(nid);

#ifdef CONFIG_RM
	rh = backup_rh[index];
#else
	rh = rdma_handles[index];
#endif

	dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size , &dma_addr);
	if (!dma_buffer) {
		printk(KERN_ALERT PFX "buffer overun in %s\n", __func__);
		return -ENOMEM;
	}
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	/* We need to pass virtual address since using INLINE flag */
	rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		printk(KERN_ALERT PFX "work pool overun in %s\n", __func__);
		ret = -ENOMEM;
		goto put_buffer;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE;

	rw->rh = rh;

	rhp = (struct rpc_header *) dma_buffer;
	rhp->nid = nid;
	rhp->op = RPC_OP_ALLOC;
	rhp->async = false;

	/*copy rpc args to buffer */
	args = dma_buffer + sizeof(struct rpc_header);
	*((uint64_t *) args) = vaddr;
	done = (int *) (args + 8);
	*done = 0;

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put_buffer;
	}

//	DEBUG_LOG(PFX "wait %p\n", rw);

	while(!(ret = *done))
		cpu_relax();

	ret = *((int *) (args + 4));

//	DEBUG_LOG(PFX "alloc done %d\n", ret);

put_buffer:
	memset(dma_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, dma_buffer);

	return ret;
}

int rmm_alloc_async(int nid, u64 vaddr, unsigned long *rpage_flags)
{
	int offset, ret = 0;
	uint8_t *dma_buffer, *args;
	struct rpc_header *rhp;
	dma_addr_t dma_addr, remote_dma_addr;
	struct rdma_handle *rh;	
	struct rdma_work *rw;
	const struct ib_send_wr *bad_wr = NULL;

	/*
	-------------------------------------------------
	|rpc_header| vaddr(8bytes) | rpage_flags(8bytes)|
	-------------------------------------------------
	*/
	int buffer_size = sizeof(struct rpc_header) + 16;
	int payload_size = sizeof(struct rpc_header) + 8;
	int index = nid_to_rh(nid);

#ifdef CONFIG_RM
	rh = backup_rh[index];
#else
	rh = rdma_handles[index];
#endif

	dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size , &dma_addr);
	if (!dma_buffer)
		return -ENOMEM;
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		ret = -ENOMEM;
		goto put_buffer;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE;

	rw->rh = rh;

	rhp = (struct rpc_header *) dma_buffer;
	rhp->nid = nid;
	rhp->op = RPC_OP_ALLOC;
	rhp->async = true;

	/*copy rpc args to buffer */
	args = dma_buffer + sizeof(struct rpc_header);
	*((uint64_t *) args) = vaddr;
	*((uint64_t *) (args + 8)) = (uint64_t) rpage_flags;

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put_buffer;
	}

	return ret;


put_buffer:
	memset(dma_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, dma_buffer);

	return ret;
}

int rmm_free(int nid, u64 vaddr)
{
	int offset, ret = 0;
	uint8_t *dma_buffer, *args;
	dma_addr_t dma_addr, remote_dma_addr;
	struct rdma_handle *rh;	
	struct rdma_work *rw;
	struct rpc_header *rhp;
	const struct ib_send_wr *bad_wr = NULL;
	int *done;

	/*
	------------------------------------------
	|rpc_header| vaddr(8bytes) | done(4bytes)|
	------------------------------------------
	*/
	int buffer_size = sizeof(struct rpc_header) + 12; 
	int payload_size = sizeof(struct rpc_header) + 8;
	int index = nid_to_rh(nid);

#ifdef CONFIG_RM
	rh = backup_rh[index];
#else
	rh = rdma_handles[index];
#endif

	dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size , &dma_addr);
	if (!dma_buffer) {
		printk(KERN_ALERT PFX "buffer overun in %s\n", __func__);
		return -ENOMEM;
	}
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		printk(KERN_ALERT PFX "work pool overun in %s\n", __func__);
		ret = -ENOMEM;
		goto put_buffer;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE;

	rw->rh = rh;

	rhp = (struct rpc_header *) dma_buffer;
	rhp->nid = nid;
	rhp->op = RPC_OP_FREE;
	rhp->async = false;

	/*copy rpc args to buffer */
	args = dma_buffer + sizeof(struct rpc_header);
	*((uint64_t *) args) = vaddr;
	done = (int *) (args + 8);
	*done = 0;

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put_buffer;
	}

//	DEBUG_LOG(PFX "wait %p\n", rw);

	while(!(ret = *done))
		cpu_relax();

	ret = *((int *) (args + 4));
//	DEBUG_LOG(PFX "free done %d\n", ret);

put_buffer:
	memset(dma_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, dma_buffer);

	return ret;
}

int rmm_free_async(int nid, u64 vaddr, unsigned long *rpage_flags)
{
	int offset, ret = 0;
	uint8_t *dma_buffer, *args;
	dma_addr_t dma_addr, remote_dma_addr;
	struct rdma_handle *rh;	
	struct rdma_work *rw;
	struct rpc_header *rhp;
	const struct ib_send_wr *bad_wr = NULL;

	int buffer_size = sizeof(struct rpc_header) + 16;
	int payload_size = sizeof(struct rpc_header) + 8;
	int index = nid_to_rh(nid);

#ifdef CONFIG_RM
	rh = backup_rh[index];
#else
	rh = rdma_handles[index];
#endif

	dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size , &dma_addr);
	if (!dma_buffer)
		return -ENOMEM;
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		ret = -ENOMEM;
		goto put_buffer;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE;

	rw->rh = rh;

	rhp = (struct rpc_header *) dma_buffer;
	rhp->nid = nid;
	rhp->op = RPC_OP_FREE;
	rhp->async = true;

	/*copy rpc args to buffer */
	args = dma_buffer + sizeof(struct rpc_header);
	*((uint64_t *) args) = vaddr;
	*((uint64_t *) (args + 8)) = (uint64_t) rpage_flags;

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put_buffer;
	}

	return ret;

put_buffer:
	memset(dma_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, dma_buffer);

	return ret;
}


//static unsigned long elapsed_fetch = 0;

int rmm_fetch(int nid, void *l_vaddr, void * r_vaddr, unsigned int order)
{
	int offset, ret = 0;
	uint8_t *dma_buffer;
	dma_addr_t dma_addr, remote_dma_addr;

	struct rdma_handle *rh;
	struct rpc_header *rhp;
	struct rdma_work *rw;
	struct fetch_args *fap;
	struct fetch_aux *aux;

	const struct ib_send_wr *bad_wr = NULL;

	/*
	----------------------------------------------------------------------------------------
	|rpc_header| r_vaddr(8bytes) | order(4bytes)| struct fetch_aux |reserved area for pages|
	---------------------------------------------------------------------------------------
	*/
	int nr_pages = 1 << order;
	int buffer_size = sizeof(struct rpc_header) + sizeof(struct fetch_args) +
			sizeof (struct fetch_aux) + (nr_pages * PAGE_SIZE);
	int payload_size = sizeof(struct rpc_header) + sizeof(struct fetch_args);
	int index = nid_to_rh(nid);

	rh = rdma_handles[index];

	dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size, &dma_addr);
	if (!dma_buffer) {
		printk(KERN_ALERT PFX "buffer overun in %s\n", __func__);
		return -ENOMEM;
	}
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, 
			remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		printk(KERN_ALERT PFX "work pool overun in %s\n", __func__);
		ret = -ENOMEM;
		goto put_buffer;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE;

	rw->rh = rh;

	rhp = (struct rpc_header *) dma_buffer;
	rhp->nid = nid;
	rhp->op = RPC_OP_FETCH;
	rhp->async = false;

	/*copy rpc args to buffer */
	fap = (struct fetch_args *) (rhp + 1);
	fap->r_vaddr = (u64) r_vaddr;
	fap->order = (u32) order;

	aux = (struct fetch_aux *) (fap + 1);
	aux->l_vaddr = (uint64_t) l_vaddr;
	aux->async.done = 0;

	/*
	getnstimeofday(&end_tv);
	elapsed_fetch += (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
		(end_tv.tv_nsec - start_tv.tv_nsec);
		*/

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ALERT PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put_buffer;
	}


	while(!(ret = aux->async.done))
		cpu_relax();


put_buffer:
	memset(dma_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, dma_buffer);

	return ret;
}

int rmm_fetch_async(int nid, void *l_vaddr, void * r_vaddr, unsigned int order, unsigned long *rpage_flags)
{
	int offset, ret = 0;
	uint8_t *dma_buffer;
	dma_addr_t dma_addr, remote_dma_addr;

	struct rdma_handle *rh;
	struct rpc_header *rhp;
	struct rdma_work *rw;
	struct fetch_args *fap;
	struct fetch_aux *aux;

	const struct ib_send_wr *bad_wr = NULL;

	//struct timespec start_tv, end_tv;
	/*
	   ----------------------------------------------------------------------------------------
	   |rpc_header| r_vaddr(8bytes) | order(4bytes)| struct fetch_aux |reserved area for pages|
	   ---------------------------------------------------------------------------------------
	 */
	int nr_pages = 1 << order;
	int buffer_size = sizeof(struct rpc_header) + sizeof(struct fetch_args) +
		sizeof (struct fetch_aux) + (nr_pages * PAGE_SIZE);
	int payload_size = sizeof(struct rpc_header) + sizeof(struct fetch_args);
	int index = nid_to_rh(nid);


	rh = rdma_handles[index];

	dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size, &dma_addr);
	if (!dma_buffer)
		return -ENOMEM;
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, 
			remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		ret = -ENOMEM;
		goto put_buffer;
	}

	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE;

	rw->rh = rh;

	rhp = (struct rpc_header *) dma_buffer;
	rhp->nid = nid;
	rhp->op = RPC_OP_FETCH;
	rhp->async = true;

	/*copy rpc args to buffer */
	fap = (struct fetch_args *) (rhp + 1);
	fap->r_vaddr = (u64) r_vaddr;
	fap->order = (u32) order;

	aux = (struct fetch_aux *) (fap + 1);
	aux->l_vaddr = (uint64_t) l_vaddr;
	aux->async.rpage_flags = (uint64_t) rpage_flags;

	/*
	   getnstimeofday(&end_tv);
	   elapsed_fetch += (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
	   (end_tv.tv_nsec - start_tv.tv_nsec);
	 */

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put_buffer;
	}

	return ret;


put_buffer:
	memset(dma_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, dma_buffer);

	return ret;
}

/* evict_list is a pointer to list_head */
int rmm_evict(int nid, struct list_head *evict_list, int num_page)
{
	int offset;
	uint8_t *evict_buffer = NULL, *temp = NULL, *args;
	dma_addr_t evict_dma_addr, remote_evict_dma_addr;
	struct rdma_work *rw;
	struct rdma_handle *rh;	
	struct rpc_header *rhp;
	struct list_head *l;
	int *done;

	int ret = 0;

	/*
	   ----------------------------------------------------------------
	   |rpc_header| num_page(4byte) | (r_vaddr, page)...| done(4bytes)|
	   ----------------------------------------------------------------
	 */
	int buffer_size = sizeof(struct rpc_header) + 8 + ((8 + PAGE_SIZE) * num_page);
	int payload_size = sizeof(struct rpc_header) + 4 + ((8 + PAGE_SIZE) * num_page);
	const struct ib_send_wr *bad_wr = NULL;
	int index = nid_to_rh(nid) + 1;

#ifdef CONFIG_RM
	rh = backup_rh[index];
#else
	rh = rdma_handles[index];
#endif
	evict_buffer = ring_buffer_get_mapped(rh->rb, buffer_size , &evict_dma_addr);
	if (!evict_buffer) {
		printk(KERN_ALERT PFX "buffer overun in %s\n", __func__);
		return -ENOMEM;
	}
	offset = evict_buffer - (uint8_t *) rh->evict_buffer;
	remote_evict_dma_addr = rh->remote_rpc_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, evict_dma_addr, payload_size, remote_evict_dma_addr, rh->rpc_rkey);
	if (!rw) {
		printk(KERN_ALERT PFX "work pool overun in %s\n", __func__);
		ret = -ENOMEM;
		goto put;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED;

	rw->rh = rh;

	rhp = (struct rpc_header *) evict_buffer;
	rhp->nid = nid;
	rhp->op = RPC_OP_EVICT;
	rhp->async = false;

	/*copy rpc args to buffer */
	DEBUG_LOG(PFX "copy args, %s, %p\n", __func__, evict_buffer);
	args = evict_buffer + sizeof(struct rpc_header);
	DEBUG_LOG(PFX "args: %p\n", args);

	*((int32_t *) args) = num_page;
	temp = args + 4;

	list_for_each(l, evict_list) {
		struct evict_info *e = list_entry(l, struct evict_info, next);
		DEBUG_LOG(PFX "iterate list r_vaddr: %llx l_vaddr: %llx\n",  e->r_vaddr, e->l_vaddr);
		*((uint64_t *) (temp)) = (uint64_t) e->r_vaddr;
		memcpy(temp + 8, (void *) e->l_vaddr, PAGE_SIZE);
		temp += (8 + PAGE_SIZE);
	}
	done = (int *) temp;
	*done = 0;
	DEBUG_LOG(PFX "iterate done\n");

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ALERT PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put;
	}

	while(!(ret = *done))
		cpu_relax();

put:
	memset(evict_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, evict_buffer);

	return ret;
}

int rmm_evict_forward(int nid, void *src_buffer, int payload_size, int *done)
{
	int offset;
	uint8_t *evict_buffer = NULL;
	dma_addr_t evict_dma_addr, remote_evict_dma_addr;
	struct rdma_work *rw;
	struct rdma_handle *rh;	
	struct rpc_header *rhp;

	int ret = 0;

	/*
	   ------------------------------------------------------------------------
	   |rpc_header| num_page(4byte) | (r_vaddr, page)...| done_pointer(8bytes)|
	   ------------------------------------------------------------------------
	 */
	int buffer_size = payload_size + 8;
	const struct ib_send_wr *bad_wr = NULL;
	int index = nid_to_rh(nid) + 1;

	buffer_size += 4;
	rh = backup_rh[index];

	evict_buffer = ring_buffer_get_mapped(rh->rb, buffer_size , &evict_dma_addr);
	if (!evict_buffer) {
		printk(KERN_ALERT PFX "buffer overun in %s\n", __func__);
		return -ENOMEM;
	}
	offset = evict_buffer - (uint8_t *) rh->evict_buffer;
	remote_evict_dma_addr = rh->remote_rpc_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, evict_dma_addr, payload_size, remote_evict_dma_addr, rh->rpc_rkey);
	if (!rw) {
		printk(KERN_ALERT PFX "work pool overun in %s\n", __func__);
		ret = -ENOMEM;
		goto put;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED;

	rw->rh = rh;

	memcpy(evict_buffer, src_buffer, payload_size);
	rhp = (struct rpc_header *) evict_buffer;
	rhp->async = true;
	*((int **) (evict_buffer + payload_size)) = done;

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ALERT PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put;
	}

	return ret;

put:
	memset(evict_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, evict_buffer);

	return ret;
}

static int rpc_handle_fetch_mem(struct rdma_handle *rh, uint32_t offset)
{
	int ret = 0;
	int payload_size;
	void *src;
	void *dest_addr;
	dma_addr_t dest_dma_addr, remote_dest_dma_addr;
	unsigned int order;
	uint8_t *rpc_buffer;

	struct rdma_work *rw;
	struct rpc_header *rhp;
	struct fetch_args *fap;

	const struct ib_send_wr *bad_wr = NULL;

	rhp = (struct rpc_header *) (rh->rpc_buffer + offset);

	rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));
	fap = (struct fetch_args *) rpc_buffer;
	src = (void *) (fap->r_vaddr + rh->vaddr_start);

//	src = my_data[0] + (u64) fap->r_vaddr;

	order = fap->order;
	payload_size = (1 << order) * PAGE_SIZE;

	dest_addr = rpc_buffer + sizeof(struct fetch_args) + sizeof(struct fetch_aux);
	dest_dma_addr = rh->rpc_dma_addr + offset + 
		sizeof(struct rpc_header) + sizeof(struct fetch_args) + sizeof(struct fetch_aux);
	remote_dest_dma_addr = rh->remote_rpc_dma_addr + offset + 
		sizeof(struct rpc_header) + sizeof(struct fetch_args) + sizeof(struct fetch_aux);

	DEBUG_LOG(PFX "src: %p, order: %u, payload size: %d", src, order, payload_size);

	/*FIXME: busy waiting */
	rw = __get_rdma_work(rh, dest_dma_addr, payload_size, remote_dest_dma_addr, rh->rpc_rkey);
	if (!rw) {
		return -ENOMEM;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED;
	rw->rh = rh;

	memcpy(dest_addr, src, payload_size);

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p %s\n", ret, bad_wr, __func__);
		if (bad_wr)
			ret = -EINVAL;
	}

	return ret;
}

static int rpc_handle_alloc_free_mem(struct rdma_handle *rh, uint32_t offset)
{
	int ret = 0;
	uint16_t nid = 0;
	uint16_t op = 0;
	uint64_t vaddr;
	dma_addr_t rpc_dma_addr, remote_rpc_dma_addr;
	struct rdma_work *rw;
	struct rpc_header *rhp;
	const struct ib_send_wr *bad_wr = NULL;
	char *rpc_buffer;

	rhp = (struct rpc_header *) (rh->rpc_buffer + offset);

	rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));
	rpc_dma_addr = rh->rpc_dma_addr + (rpc_buffer - (char *) rh->rpc_buffer);
	remote_rpc_dma_addr = rh->remote_rpc_dma_addr + (rpc_buffer - (char *) rh->rpc_buffer);

	nid = rhp->nid;
	op = rhp->op; 
	/*Add offset */
	if (!rh->backup)
		vaddr = (*(uint64_t *) (rpc_buffer)) + (rh->vaddr_start);
	else 
		vaddr = (*(uint64_t *) (rpc_buffer));

	rw = __get_rdma_work(rh, rpc_dma_addr, 8, remote_rpc_dma_addr, rh->rpc_rkey);
	if (!rw)
		return -ENOMEM;
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED;
	rw->rh = rh;

	DEBUG_LOG(PFX "nid: %d, offset: %d, vaddr: %llX op: %d\n", nid, offset, vaddr, op);

	if (op == RPC_OP_ALLOC)  {
		/* rm_alloc and free return true when they success */
		ret = rm_alloc(vaddr);
		if (cr_on && ret)
			rmm_alloc(0, vaddr);
	}
	else if (op == RPC_OP_FREE) {
		ret = rm_free(vaddr);
		if (cr_on && ret)
			rmm_free(0, vaddr);
	}

	*((int *) (rpc_buffer + 4)) = ret;
	DEBUG_LOG(PFX "ret in %s, %d\n", __func__, ret);

	/* -1 means this packet is ack */
	*((int *) rpc_buffer) = -1;
	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
	}

	return ret;
}

static int rpc_handle_evict_mem(struct rdma_handle *rh,  uint32_t offset)
{
	int i, num_page, ret;
	int done = 0;
	u64 dest;
	struct rdma_work *rw;
	uint8_t *evict_buffer, *page_pointer;
	const struct ib_send_wr *bad_wr = NULL;

	struct rpc_header *rhp;

	rhp = (struct rpc_header *) (rh->evict_buffer + offset);
	evict_buffer = rh->evict_buffer + (offset) + sizeof(struct rpc_header);
	num_page = (*(int32_t *) evict_buffer);
	page_pointer = evict_buffer + 4;

	if (!rh->backup) {
		for (i = 0; i < num_page; i++) {
			*((uint64_t *) (page_pointer)) = 
				(*((uint64_t *) (page_pointer))) + (rh->vaddr_start);
			page_pointer += (8 + PAGE_SIZE);
		}
	}

	if (cr_on) {
		DEBUG_LOG(PFX "replicate to backup server\n");
		rmm_evict_forward(0, rh->evict_buffer + offset, num_page * (8 + PAGE_SIZE) + 
				sizeof(struct rpc_header) + sizeof(int), &done);

		DEBUG_LOG(PFX "replicate done\n");
	}

	page_pointer = evict_buffer + 4;
	DEBUG_LOG(PFX "num_page %d, from %s\n", num_page, __func__);
	for (i = 0; i < num_page; i++) {
		dest =  *((uint64_t *) (page_pointer));
		memcpy((void *) dest, page_pointer + 8, PAGE_SIZE);
		page_pointer += (8 + PAGE_SIZE);

	}

	if (cr_on) {
		while(!(ret = done))
			cpu_relax();
	}


	/* set buffer to -1 to send ack to caller */
	*((int *) (evict_buffer + 4)) = -1;
	/* FIXME: In current implementation, rpc_dma_addr == evict_dma_addr */
	rw = __get_rdma_work(rh, rh->evict_dma_addr + offset + sizeof(struct rpc_header) + 4,
			4, rh->remote_rpc_dma_addr + offset + sizeof(struct rpc_header) + 4, rh->rpc_rkey);
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED;

	rw->rh = rh;

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
	}

	DEBUG_LOG(PFX "evict done, %s\n", __func__);

	return 0;
}


/*rpc handle functions for cpu server */
static int rpc_handle_fetch_cpu(struct rdma_handle *rh, uint32_t offset)
{
	int num_page, order;
	void *src, *dest;
	unsigned long *rpage_flags = NULL;

	struct rpc_header *rhp;
	struct fetch_args *fap;
	struct fetch_aux *aux;

	rhp = (struct rpc_header *) (rh->rpc_buffer + offset);
	fap = (struct fetch_args *) (rhp + 1);
	aux = (struct fetch_aux *) (fap + 1);


	if (rhp->async) {
		rpage_flags = (unsigned long *) aux->async.rpage_flags;
	}

	order = fap->order;
	src = (uint8_t *) rh->rpc_buffer + (offset + sizeof(struct rpc_header) +
			sizeof(struct fetch_args) + sizeof(struct fetch_aux));
	dest = (void *) aux->l_vaddr; 

	num_page = 1 << order;

	memcpy(dest, src, PAGE_SIZE * num_page);

	/*aux_asycn->rpage_flags is arleary copied to local var */
	aux->async.done = 1;
	if (rpage_flags) {
		DEBUG_LOG(PFX "async fetch is done %s \n", __func__);
		*rpage_flags |= RPAGE_FETCHED;
		ring_buffer_put(rh->rb, rh->rpc_buffer + offset);
	}

	return 0;
}



#ifdef CONFIG_RM 
void regist_handler(Rpc_handler rpc_table[])
{
	rpc_table[RPC_OP_FETCH] = rpc_handle_fetch_mem;
	rpc_table[RPC_OP_EVICT] = rpc_handle_evict_mem;
	rpc_table[RPC_OP_ALLOC] = rpc_handle_alloc_free_mem;
	rpc_table[RPC_OP_FREE] = rpc_handle_alloc_free_mem;
}
#else
void regist_handler(Rpc_handler rpc_table[])
{
	rpc_table[RPC_OP_FETCH] = rpc_handle_fetch_cpu;
	rpc_table[RPC_OP_EVICT] = rpc_handle_evict_done;
	rpc_table[RPC_OP_ALLOC] = rpc_handle_alloc_free_done;
	rpc_table[RPC_OP_FREE] = rpc_handle_alloc_free_done;
}

#endif
