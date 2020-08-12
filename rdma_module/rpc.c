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
#include <linux/io.h>

#include <rdma/rdma_cm.h>
#include <rdma/ib_verbs.h>

#include "rpc.h"
#include "main.h"
#include "ring_buffer.h"

extern int debug;
extern struct rdma_handle *rdma_handles[];

static struct rdma_work *__get_rdma_work(struct rdma_handle *rh, dma_addr_t dma_addr, size_t size, dma_addr_t rdma_addr, u32 rdma_key);
static struct rdma_work *__get_rdma_work_nonsleep(struct rdma_handle *rh, dma_addr_t dma_addr, size_t size, dma_addr_t rdma_addr, u32 rdma_key);
//static void __put_rdma_work(struct rdma_handle *rh, struct rdma_work *rw);
static void __put_rdma_work_nonsleep(struct rdma_handle *rh, struct rdma_work *rw);

/* SANGJIN START */
static int req_cnt = 0;
static int ack_cnt = 0;

extern spinlock_t cinfos_lock;
/* SANGJIN END */

static struct rdma_work *__get_rdma_work(struct rdma_handle *rh, dma_addr_t dma_addr, 
		size_t size, dma_addr_t rdma_addr, u32 rdma_key)
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

static struct rdma_work *__get_rdma_work_nonsleep(struct rdma_handle *rh, 
		dma_addr_t dma_addr, size_t size, dma_addr_t rdma_addr, u32 rdma_key)
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

/*
void *get_buffer(struct rdma_handle *rh, size_t size, dma_addr_t *dma_addr)
{
	void *addr;

	dma_addr_t = d_addr;
	addr =  ring_buffer_get_mapped(rh->rb, size + , &d_addr);
	*dma_addr = d_addr + sizeof(struct rpc_header);

	return addr + sizeof(struct rpc_header);
}
*/



int wait_for_ack_timeout(int *done, u64 ticks)
{
	int ret;
	u64 start = get_jiffies_64();
	u64 cur = get_jiffies_64();

	while(!(ret = *done)) {
		cur = get_jiffies_64();
		if (cur - start > ticks) {
			printk("timeout\n");
			return -ETIME;
		}
		cpu_relax();
	}

	return 0;
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

	rh = rdma_handles[index];

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

	ret = wait_for_ack_timeout(done, 10000);
	if (!ret)
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

	rh = rdma_handles[index];

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

	rh = rdma_handles[index];

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

	wait_for_ack_timeout(done, 10000);

	if (!ret)
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

	rh = rdma_handles[index];

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


	ret = wait_for_ack_timeout(&(aux->async.done), 30000);

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

	rh = rdma_handles[index];
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

	ret = wait_for_ack_timeout(done, 10000);

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
	rh = rdma_handles[index];

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

/* SANGJIN START */
int rmm_recovery(int nid)
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
        --------------------------
        |rpc_header| done(4bytes)|
        --------------------------
        */
        int buffer_size = sizeof(struct rpc_header) + 4;
        int payload_size = sizeof(struct rpc_header);
        int index = nid_to_rh(nid);

        rh = rdma_handles[index];

        dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size, &dma_addr);
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
        rhp->op = RPC_OP_RECOVERY;
        rhp->async = false;

        /* copy done buffer */
        args = dma_buffer + sizeof(struct rpc_header);
        done = (int *) args;
        *done = 0;

        ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
        __put_rdma_work_nonsleep(rh, rw);
        if (ret || bad_wr) {
                printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
                if (bad_wr)
                        ret = -EINVAL;
                goto put_buffer;
        }

        printk("recovery wait %p\n", rw);

        while(!(ret = *done))
                cpu_relax();

        ret = *((int *) (args + 4));

        //DEBUG_LOG(PFX "recovery done %d\n", ret);

        printk(KERN_ALERT PFX "recovery done\n");

put_buffer:
        memset(dma_buffer, 0, buffer_size);
        ring_buffer_put(rh->rb, dma_buffer);

        return ret;
}

int rmm_replicate(int src_nid, int dest_nid)
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
        ---------------------------------
        | rpc_header | dest_nid(4bytes) |
        ---------------------------------
        */
        int buffer_size = sizeof(struct rpc_header) + 4;
        int payload_size = sizeof(struct rpc_header) + 4;
        int index = nid_to_rh(src_nid);

        rh = rdma_handles[index];

        dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size, &dma_addr);
        if (!dma_buffer) 
                return -ENOMEM;
        
        offset = dma_buffer - (uint8_t *) rh->dma_buffer;
        remote_dma_addr = rh->remote_dma_addr + offset;

        /* We need to pass virtual address since using INLINE flag */
        rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, remote_dma_addr, rh->rpc_rkey);
        if (!rw) {
                ret = -ENOMEM;
                goto put_buffer;
        }
        rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
        rw->wr.wr.send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE;

        rw->rh = rh;

        rhp = (struct rpc_header *) dma_buffer;
        rhp->nid = src_nid;
        rhp->op = RPC_OP_REPLICATE;
        rhp->async = true;

        /* copy rpc args to buffer */
        args = dma_buffer + sizeof(struct rpc_header);
	*((uint32_t *) args) = dest_nid;

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
/* SANGJIN END */

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
	int i;
	int ret = 0;
	uint16_t nid = 0;
	uint16_t op = 0;
	uint64_t vaddr;
	dma_addr_t rpc_dma_addr, remote_rpc_dma_addr;
	struct rdma_work *rw;
	struct rpc_header *rhp;
	const struct ib_send_wr *bad_wr = NULL;
	char *rpc_buffer;
	struct node_info *infos;

	rhp = (struct rpc_header *) (rh->rpc_buffer + offset);

	rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));
	rpc_dma_addr = rh->rpc_dma_addr + (rpc_buffer - (char *) rh->rpc_buffer);
	remote_rpc_dma_addr = rh->remote_rpc_dma_addr + (rpc_buffer - (char *) rh->rpc_buffer);

	nid = rhp->nid;
	op = rhp->op; 
	/*Add offset */
	vaddr = (*(uint64_t *) (rpc_buffer)) + (rh->vaddr_start);

	rw = __get_rdma_work(rh, rpc_dma_addr, 8, remote_rpc_dma_addr, rh->rpc_rkey);
	if (!rw)
		return -ENOMEM;
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED;
	rw->rh = rh;

	DEBUG_LOG(PFX "nid: %d, offset: %d, vaddr: %llX op: %d\n", nid, offset, vaddr, op);

	infos = get_node_infos(MEM_GID, BACKUP_SYNC);
	if (op == RPC_OP_ALLOC)  {
		/* rm_alloc and free return true when they success */
		ret = rm_alloc(vaddr);
		if (ret) {
			for (i = 0; infos->size; i++) {
				rmm_alloc(infos->nids[i], vaddr);
			}
		}
	}
	else if (op == RPC_OP_FREE) {
		ret = rm_free(vaddr);
		if (ret) {
			for (i = 0; infos->size; i++) {
				rmm_free(infos->nids[i], vaddr);
			}
		}
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
	bool wait_for_replication = false;

	struct rpc_header *rhp;
	struct node_info *infos;

	rhp = (struct rpc_header *) (rh->evict_buffer + offset);
	evict_buffer = rh->evict_buffer + (offset) + sizeof(struct rpc_header);
	num_page = (*(int32_t *) evict_buffer);
	page_pointer = evict_buffer + 4;

	if ((rh->c_type == PRIMARY) || (rh->c_type == SECONDARY)) {
		for (i = 0; i < num_page; i++) {
			*((uint64_t *) (page_pointer)) = 
				(*((uint64_t *) (page_pointer))) + (rh->vaddr_start);
			page_pointer += (8 + PAGE_SIZE);
		}
	}

	infos = get_node_infos(MEM_GID, BACKUP_SYNC);
	for (i = 0; i < infos->size; i++) {
		wait_for_replication = true;
		DEBUG_LOG(PFX "replicate to backup server SYNC\n");
		rmm_evict_forward(infos->nids[i], rh->evict_buffer + offset, 
				num_page * (8 + PAGE_SIZE) + 
				sizeof(struct rpc_header) + sizeof(int), &done);
		DEBUG_LOG(PFX "replicate done\n");
	}

	infos = get_node_infos(MEM_GID, BACKUP_ASYNC);
	for (i = 0; i < infos->size; i++) {
		wait_for_replication = false;
		req_cnt++;
		DEBUG_LOG(PFX "replicate to backup server ASYNC\n");
		rmm_evict_forward(infos->nids[i], rh->evict_buffer + offset, 
				num_page * (8 + PAGE_SIZE) + 
				sizeof(struct rpc_header) + sizeof(int), &ack_cnt);
		DEBUG_LOG(PFX "replicate done\n");
	}

	page_pointer = evict_buffer + 4;
	DEBUG_LOG(PFX "num_page %d, from %s\n", num_page, __func__);
	for (i = 0; i < num_page; i++) {
		dest =  *((uint64_t *) (page_pointer));
		memcpy((void *) dest, page_pointer + 8, PAGE_SIZE);
		page_pointer += (8 + PAGE_SIZE);

	}

	if (wait_for_replication) {
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

/* SANGJIN START */
static int rpc_handle_recovery_cpu(struct rdma_handle *rh, uint32_t offset)
{
	struct rpc_header *rhp;
	uint8_t *buffer = rh->dma_buffer + offset;
	int *done;
	
	add_node_to_group(MEM_GID, 2, PRIMARY);
	remove_node_from_group(MEM_GID, 1, PRIMARY);
	add_node_to_group(MEM_GID, 1, SECONDARY);
	remove_node_from_group(MEM_GID, 2, SECONDARY);

	rhp = (struct rpc_header *)buffer;
	done = (int *)(buffer + sizeof(struct rpc_header));
	*done = 1;

	return 0;
}

/* rpc handle functions for backup server */
static int rpc_handle_recovery_backup(struct rdma_handle *rh, uint32_t offset)
{
        int ret = 0;
        uint16_t nid = 0;
        uint16_t op = 0;
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

        rw = __get_rdma_work(rh, rpc_dma_addr, 0, remote_rpc_dma_addr, rh->rpc_rkey);
        if (!rw)
                return -ENOMEM;
        rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
        rw->wr.wr.send_flags = IB_SEND_SIGNALED;
        rw->rh = rh;

        DEBUG_LOG(PFX "nid: %d, offset: %d, op: %d\n", nid, offset, op);

        // busy wait until full consistentcy with r1 and r2
        while (!(req_cnt == ack_cnt))
                cpu_relax();

        // initialize
        req_cnt = 0;
        ack_cnt = 0;

        ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
        __put_rdma_work_nonsleep(rh, rw);
        if (ret || bad_wr) {
                printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
                if (bad_wr)
                        ret = -EINVAL;
        }

	add_node_to_group(MEM_GID, 3, BACKUP_SYNC);
	remove_node_from_group(MEM_GID, 3, BACKUP_ASYNC);

        return ret;
}

static int rpc_handle_replicate_backup(struct rdma_handle *rh, uint32_t offset)
{
	int ret = 0;
        uint16_t nid = 0;
	uint16_t dest_nid;
        uint16_t op = 0;
        dma_addr_t rpc_dma_addr, remote_rpc_dma_addr;
        struct rdma_work *rw;
        struct rpc_header *rhp;
        const struct ib_send_wr *bad_wr = NULL;
        char *rpc_buffer;
	int i, j, nr_pages;
	struct evict_info *ei;
	struct list_head *pos, *n;
	struct timespec start_tv, end_tv;
	unsigned long elapsed;
	LIST_HEAD(addr_list);
	
	getnstimeofday(&start_tv);

        rhp = (struct rpc_header *) (rh->rpc_buffer + offset);

        rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));
        rpc_dma_addr = rh->rpc_dma_addr + (rpc_buffer - (char *) rh->rpc_buffer);
        remote_rpc_dma_addr = rh->remote_rpc_dma_addr + (rpc_buffer - (char *) rh->rpc_buffer);

        nid = rhp->nid;
        op = rhp->op;
	dest_nid = *(uint32_t *) (rpc_buffer);

        rw = __get_rdma_work(rh, rpc_dma_addr, 0, remote_rpc_dma_addr, rh->rpc_rkey);
        if (!rw)
                return -ENOMEM;
        rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
        rw->wr.wr.send_flags = IB_SEND_SIGNALED;
        rw->rh = rh;

        DEBUG_LOG(PFX "nid: %d, offset: %d, op: %d\n", nid, offset, op);
        DEBUG_LOG(PFX "dest nid: %d\n", dest_nid);

	nr_pages = 128;
	for (i = 0; i < (MCOS_BASIC_MEMORY_SIZE * RM_PAGE_SIZE / PAGE_SIZE) / nr_pages; i++) {
		INIT_LIST_HEAD(&addr_list);

		for (j = 0; j < nr_pages; j++) {
			ei = kmalloc(sizeof(struct evict_info), GFP_KERNEL);
			if (!ei) {
				printk("error in %s\n", __func__);
				return -ENOMEM;
			}

			ei->l_vaddr = RM_VADDR_START + (nr_pages * i + j) * PAGE_SIZE; 
			ei->r_vaddr = ei->l_vaddr;
			INIT_LIST_HEAD(&ei->next);
			list_add(&ei->next, &addr_list);
		}

retry:
		ret = rmm_evict(dest_nid, &addr_list, nr_pages);
		if (ret == -ETIME)
			goto retry;

		list_for_each_safe(pos, n, &addr_list) {
			ei = list_entry(pos, struct evict_info, next);
			kfree(ei);
		}
	}

	/* -1 means this packet is ack */
	*((int *) rpc_buffer) = -1;
        ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
        __put_rdma_work_nonsleep(rh, rw);
        if (ret || bad_wr) {
                printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
                if (bad_wr)
                        ret = -EINVAL;
        }

	getnstimeofday(&end_tv);
	elapsed = (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
		(end_tv.tv_nsec - start_tv.tv_nsec);

	printk(KERN_INFO PFX "total elapsed time %lu (ns)\n", elapsed);

        return ret;
}


/* SANGJIN END */
#ifdef CONFIG_RM 
void regist_handler(Rpc_handler rpc_table[])
{
	rpc_table[RPC_OP_FETCH] = rpc_handle_fetch_mem;
	rpc_table[RPC_OP_EVICT] = rpc_handle_evict_mem;
	rpc_table[RPC_OP_ALLOC] = rpc_handle_alloc_free_mem;
	rpc_table[RPC_OP_FREE] = rpc_handle_alloc_free_mem;
/* SANGJIN START */
	rpc_table[RPC_OP_RECOVERY] = rpc_handle_recovery_backup;
	rpc_table[RPC_OP_REPLICATE] = rpc_handle_replicate_backup;
/* SANGJIN END */
}
#else
void regist_handler(Rpc_handler rpc_table[])
{
	rpc_table[RPC_OP_FETCH] = rpc_handle_fetch_cpu;
/* SANGJIN START */
	rpc_table[RPC_OP_RECOVERY] = rpc_handle_recovery_cpu;
/* SANGJIN END */
}
#endif
