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

#ifdef CONFIG_RM
static int req_cnt = 0;
static int ack_cnt = 0;
#endif
extern spinlock_t cinfos_lock;
bool writeback_dirty_log = false;
int writeback_dirty_list_size = 0;
LIST_HEAD(writeback_dirty_list);

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
	rhp->req = true;

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
	rhp->req = true;

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
		//printk(KERN_ALERT PFX "buffer overun in %s\n", __func__);
		printk(KERN_ALERT "buffer overun in %s\n", __func__);
		return -ENOMEM;
	}
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		//printk(KERN_ALERT PFX "work pool overun in %s\n", __func__);
		printk(KERN_ALERT "work pool overun in %s\n", __func__);
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
	rhp->req = true;

	/*copy rpc args to buffer */
	args = dma_buffer + sizeof(struct rpc_header);
	*((uint64_t *) args) = vaddr;
	done = (int *) (args + 8);
	*done = 0;

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		//printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		printk(KERN_ERR "Cannot post send wr, %d %p\n", ret, bad_wr);
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
	rhp->req = true;

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
		//printk(KERN_ALERT PFX "buffer overun in %s\n", __func__);
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
	rhp->req = true;

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
	if (!dma_buffer) {
		//printk(KERN_ALERT "Cannot ring buffer get mapped, fetch\n");
		return -ENOMEM;
	}
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, (dma_addr_t) dma_buffer, payload_size, 
			remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		printk(KERN_ALERT "Cannot get rdma work fetch\n");
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
	rhp->req = true;

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
		printk(KERN_ALERT "Cannot post send wr fetch, %d %p\n", ret, bad_wr);
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

int rmm_prefetch_async(int nid, struct fetch_info *fi_array, int num_page)
{
	int offset, i, ret = 0;
	uint8_t *dma_buffer, *temp = NULL, *args, *l_vaddr_ptr, *rpage_flags_ptr;
	dma_addr_t dma_addr, remote_dma_addr;
	struct rdma_handle *rh;
	struct rpc_header *rhp;
	struct rdma_work *rw;
	const struct ib_send_wr *bad_wr = NULL;

	/*
   rhp	      args	        temp(r_vaddr_ptr)     l_vaddr_ptr	    rpage_flags_ptr	      page_ptr
   ---------------------------------------------------------------------------------------------------------------------------
   |rpc_header| num_page(4byte) | r_vaddr(8bytes) ... | l_vaddr(8bytes) ... | rpage_flags(8bytes) ... |reserved area for pages|
   ---------------------------------------------------------------------------------------------------------------------------
	 */

	int buffer_size = sizeof(struct rpc_header) + 4 +
		          ((24 + PAGE_SIZE) * num_page);
	int payload_size = sizeof(struct rpc_header) + 4 + (8 * num_page);
	int index = nid_to_rh(nid);

	rh = rdma_handles[index];

	dma_buffer = ring_buffer_get_mapped(rh->rb, buffer_size, &dma_addr);
	if (!dma_buffer) {
		//printk(KERN_ALERT "Cannot ring buffer get mapped\n");
		return -ENOMEM;
	}
	offset = dma_buffer - (uint8_t *) rh->dma_buffer;
	remote_dma_addr = rh->remote_dma_addr + offset;

	/*rw = __get_rdma_work_nonsleep(rh, (dma_addr_t)dma_buffer, payload_size, 
			remote_dma_addr, rh->rpc_rkey);*/
	rw = __get_rdma_work_nonsleep(rh, dma_addr, payload_size, 
			remote_dma_addr, rh->rpc_rkey);
	if (!rw) {
		printk(KERN_ALERT "Cannot get rdma work\n");
		ret = -ENOMEM;
		goto put_buffer;
	}

	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	//rw->wr.wr.send_flags = IB_SEND_SIGNALED | IB_SEND_INLINE;
	rw->wr.wr.send_flags = IB_SEND_SIGNALED;
	// If you don't use IB_SEND_INLINE, use dma_addr instead of dma_buffer in __get_rdma_work_nonsleep

	rhp = (struct rpc_header *) dma_buffer;
	rhp->nid = nid;
	rhp->op = RPC_OP_PREFETCH;
	rhp->async = true;

	DEBUG_LOG(PFX "copy args, %s, %p\n", __func__, dma_buffer);
	args = dma_buffer + sizeof(struct rpc_header);
	DEBUG_LOG(PFX "args: %p\n", args);

	*((int32_t *) args) = num_page;
	temp = args + 4;
	// temp indicates first r_vaddr

	l_vaddr_ptr = temp + 8 * num_page;
	rpage_flags_ptr = l_vaddr_ptr + 8 * num_page;
	DEBUG_LOG(PFX "temp: %p\n", temp);

	for (i = 0; i < num_page; i++) {
		*((uint64_t*) (temp)) = (uint64_t) fi_array[i].r_vaddr - FAKE_PA_START;
		temp += 8;
		*((uint64_t*) (l_vaddr_ptr)) = (uint64_t) fi_array[i].l_vaddr;
		l_vaddr_ptr += 8;
		*((uint64_t*) (rpage_flags_ptr)) = (uint64_t) fi_array[i].rpage_flags;
		rpage_flags_ptr += 8;
	}

	// Fill the other part of prefetch_aux now

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		printk(KERN_ALERT "Cannot post send wr, %d %p\n", ret, bad_wr);
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
		//printk(KERN_ALERT PFX "buffer overun in %s\n", __func__);
		//printk(KERN_ALERT "buffer overun in %s\n", __func__);
		return -ENOMEM;
	}
	offset = evict_buffer - (uint8_t *) rh->evict_buffer;
	remote_evict_dma_addr = rh->remote_rpc_dma_addr + offset;

	rw = __get_rdma_work_nonsleep(rh, evict_dma_addr, payload_size, remote_evict_dma_addr, rh->rpc_rkey);
	if (!rw) {
		//printk(KERN_ALERT PFX "work pool overun in %s\n", __func__);
		printk(KERN_ALERT "work pool overun in %s\n", __func__);
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
	rhp->req = true;

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
	//*done = 0; FIXME initialize before calling rmm_evict
	DEBUG_LOG(PFX "iterate done\n");

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	__put_rdma_work_nonsleep(rh, rw);
	if (ret || bad_wr) {
		//printk(KERN_ALERT PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		printk(KERN_ALERT "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put;
	}

	ret = wait_for_ack_timeout(done, 100000);

put:
	memset(evict_buffer, 0, buffer_size);
	ring_buffer_put(rh->rb, evict_buffer);

	return ret;
}

int rmm_evict_async(int nid, struct list_head *evict_list, int num_page, int *done)
{
	int offset;
	uint8_t *evict_buffer = NULL, *temp = NULL, *args;
	dma_addr_t evict_dma_addr, remote_evict_dma_addr;
	struct rdma_work *rw;
	struct rdma_handle *rh;	
	struct rpc_header *rhp;
	struct list_head *l;
	int ret = 0;

	/*
	   ------------------------------------------------------------------------
	   |rpc_header| num_page(4byte) | (r_vaddr, page)...| done_pointer(8bytes)|
	   ------------------------------------------------------------------------
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
	rhp->async = true;
	rhp->req = true;

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
	//*done = 0; FIXME initialize before calling evict_async
	*((int **) (temp)) = done;

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

int rmm_evict_forward(int nid, void *src_buffer, int payload_size, int *done)
{
	int offset;
	uint8_t *evict_buffer = NULL;
	dma_addr_t evict_dma_addr, remote_evict_dma_addr;
	struct rdma_work *rw;
	struct rdma_handle *rh;	
	struct rpc_header *rhp;
	int ret = 0;
	int buffer_size = payload_size + 8;
	const struct ib_send_wr *bad_wr = NULL;
	int index = nid_to_rh(nid) + 1;

	/*
	   ------------------------------------------------------------------------
	   |rpc_header| num_page(4byte) | (r_vaddr, page)...| done_pointer(8bytes)|
	   ------------------------------------------------------------------------
	*/

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
	rhp->req = true;
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

int rmm_synchronize(int src_nid, int dest_nid)
{
        int offset, ret = 0;
        uint8_t *dma_buffer, *args;
        struct rpc_header *rhp;
        dma_addr_t dma_addr, remote_dma_addr;
        struct rdma_handle *rh;
        struct rdma_work *rw;
        const struct ib_send_wr *bad_wr = NULL;
        int *done;
        int buffer_size = sizeof(struct rpc_header) + 8;
        int payload_size = sizeof(struct rpc_header) + 4;
	int index = nid_to_rh(src_nid);
        
	/*
        ------------------------------------------------
        | rpc_header | dest_nid(4bytes) | done(4bytes) |
        ------------------------------------------------
        */

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
        rhp->nid = src_nid;
        rhp->op = RPC_OP_SYNCHRONIZE;
        rhp->async = false;
	rhp->req = true;

        /* copy done buffer */
        args = dma_buffer + sizeof(struct rpc_header);
	*(uint32_t *)args = dest_nid;
	done = (uint32_t *)(args + 4);
	*done = 0;

        ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
        __put_rdma_work_nonsleep(rh, rw);
        if (ret || bad_wr) {
                printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
                if (bad_wr)
                        ret = -EINVAL;
                goto put_buffer;
        }

        while(!(ret = *done))
                cpu_relax();

        ret = *((int *) (args + 4));

        DEBUG_LOG(PFX "synchronize done %d\n", ret);

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
        int buffer_size = sizeof(struct rpc_header) + 4;
        int payload_size = sizeof(struct rpc_header) + 4;
        int index = nid_to_rh(src_nid);

        /*
        ---------------------------------
        | rpc_header | dest_nid(4bytes) |
        ---------------------------------
        */

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
        rhp->req = true;

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

int rmm_writeback_dirty(int src_nid, int dest_nid)
{
	int offset, ret = 0;
        uint8_t *dma_buffer, *args;
        struct rpc_header *rhp;
        dma_addr_t dma_addr, remote_dma_addr;
        struct rdma_handle *rh;
        struct rdma_work *rw;
        const struct ib_send_wr *bad_wr = NULL;
        int *done;
        int buffer_size = sizeof(struct rpc_header) + 8;
        int payload_size = sizeof(struct rpc_header) + 4;
	int index = nid_to_rh(src_nid);
        
	/*
        ------------------------------------------------
        | rpc_header | dest_nid(4bytes) | done(4bytes) |
        ------------------------------------------------
        */

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
        rhp->nid = src_nid;
        rhp->op = RPC_OP_WRITEBACK_DIRTY;
        rhp->async = false;
	rhp->req = true;

        /* copy done buffer */
        args = dma_buffer + sizeof(struct rpc_header);
	*(uint32_t *)args = dest_nid;
	done = (uint32_t *)(args + 4);
	*done = 0;

        ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
        __put_rdma_work_nonsleep(rh, rw);
        if (ret || bad_wr) {
                printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
                if (bad_wr)
                        ret = -EINVAL;
                goto put_buffer;
        }

        printk(PFX "waiting for evict dirty\n");

        while(!(ret = *done))
                cpu_relax();

        ret = *((int *) (args + 4));

        printk(PFX "evict dirty done %d\n", ret);

put_buffer:
        memset(dma_buffer, 0, buffer_size);
        ring_buffer_put(rh->rb, dma_buffer);

        return ret;
}

#ifdef CONFIG_RM
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

static int rpc_handle_prefetch_mem(struct rdma_handle *rh, uint32_t offset)
{
	int ret = 0;
	int payload_size;
	dma_addr_t dest_dma_addr, remote_dest_dma_addr;
	uint8_t *rpc_buffer, *r_vaddr_ptr, *rpage_flags_ptr, *page_ptr;

	struct rdma_work *rw;
	struct rpc_header *rhp;
	int num_page;
	int i;
	void *src, *dest;
	// copy r_vaddr to page

	const struct ib_send_wr *bad_wr = NULL;

	rhp = (struct rpc_header *) (rh->rpc_buffer + offset);
	
	rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));
	num_page = (*(int32_t *)rpc_buffer);
	r_vaddr_ptr = rpc_buffer + 4;
	rpage_flags_ptr = r_vaddr_ptr + 16 * num_page;
	page_ptr = rpage_flags_ptr + 8 * num_page;

	payload_size = num_page * PAGE_SIZE;

	dest_dma_addr = rh->rpc_dma_addr + offset + 
			sizeof(struct rpc_header) + 4 + 24 * num_page;
	remote_dest_dma_addr = rh->remote_rpc_dma_addr + offset +
			sizeof(struct rpc_header) + 4 + 24 * num_page;
	
	rw = __get_rdma_work_nonsleep(rh, dest_dma_addr, payload_size, remote_dest_dma_addr, rh->rpc_rkey);
	if (!rw) {
		return -ENOMEM;
	}
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->wr.wr.send_flags = IB_SEND_SIGNALED;
	rw->rh = rh;

	for (i = 0; i < num_page; i++) {
		src = (void *) (rh->vaddr_start + (*(uint64_t *)r_vaddr_ptr));
		//printk(KERN_ALERT "prefetch_mem: %lx\n", (*(uint64_t *)r_vaddr_ptr));
		dest = page_ptr;
		memcpy(dest, src, PAGE_SIZE);	// fault
		r_vaddr_ptr += 8;
		page_ptr += PAGE_SIZE;
	}

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
	struct rdma_work *rw;
	struct rpc_header *rhp;
	const struct ib_send_wr *bad_wr = NULL;
	char *rpc_buffer;
	struct node_info *infos;

	rhp = (struct rpc_header *) (rh->rpc_buffer + offset);

	rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));

	nid = rhp->nid;
	op = rhp->op; 
	/*Add offset */
	vaddr = (*(uint64_t *) (rpc_buffer)) + (rh->vaddr_start);

	rw = __get_rdma_work(rh, rh->rpc_dma_addr + offset, sizeof(struct rpc_header) + 4,
			rh->remote_rpc_dma_addr + offset, rh->rpc_rkey);
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

	DEBUG_LOG(PFX "ret in %s, %d\n", __func__, ret);
	*((int *) rpc_buffer) = ret;
	rhp->req = false;
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
	struct evict_info *ei;

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

	if (writeback_dirty_log) {
		page_pointer = evict_buffer + 4;

		for (i = 0; i < num_page; i++) {
			ei = kmalloc(sizeof(struct evict_info), GFP_KERNEL);
			if (!ei) {
				printk("error in %s\n", __func__);
				return -ENOMEM;
			}

			ei->l_vaddr = *((uint64_t *) (page_pointer));
			ei->r_vaddr = *((uint64_t *) (page_pointer));
			page_pointer += (8 + PAGE_SIZE);

			INIT_LIST_HEAD(&ei->next);
			list_add(&ei->next, &writeback_dirty_list);
		}
		writeback_dirty_list_size += num_page;
	}
	else {
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

	rhp->req = false;
	/* FIXME: In current implementation, rpc_dma_addr == evict_dma_addr */
	rw = __get_rdma_work(rh, rh->evict_dma_addr + offset,
			sizeof(struct rpc_header), rh->remote_rpc_dma_addr + offset, rh->rpc_rkey);
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

static int rpc_handle_synchronize_mem(struct rdma_handle *rh, uint32_t offset)
{
        int ret = 0;
        uint16_t nid = 0;
        uint16_t op = 0;
        dma_addr_t rpc_dma_addr, remote_rpc_dma_addr;
        struct rdma_work *rw;
        struct rpc_header *rhp;
        const struct ib_send_wr *bad_wr = NULL;
        char *rpc_buffer;
	uint32_t dest_nid;

        rhp = (struct rpc_header *) (rh->rpc_buffer + offset);
        rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));
        rpc_dma_addr = rh->rpc_dma_addr + (rpc_buffer - (char *) rh->rpc_buffer);
        remote_rpc_dma_addr = rh->remote_rpc_dma_addr + (rpc_buffer - (char *) rh->rpc_buffer);
	dest_nid = *(uint32_t *)(rpc_buffer);

        nid = rhp->nid;
        op = rhp->op;

        // busy wait until full consistentcy with r1 and r2
        while (!(req_cnt == ack_cnt))
                cpu_relax();

        // initialize
        req_cnt = 0;
        ack_cnt = 0;

        rw = __get_rdma_work(rh, rpc_dma_addr, 0, remote_rpc_dma_addr, rh->rpc_rkey);
        if (!rw)
                return -ENOMEM;

        rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
        rw->wr.wr.send_flags = IB_SEND_SIGNALED;
        rw->rh = rh;

        DEBUG_LOG(PFX "nid: %d, offset: %d, op: %d\n", nid, offset, op);

	rhp->req = false;
        ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
        __put_rdma_work_nonsleep(rh, rw);
        if (ret || bad_wr) {
                printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
                if (bad_wr)
                        ret = -EINVAL;
        }

	add_node_to_group(MEM_GID, dest_nid, BACKUP_SYNC);
	remove_node_from_group(MEM_GID, dest_nid, BACKUP_ASYNC);

        return ret;
}

static int __rpc_handle_replicate_mem(void *args)
{
	struct rdma_handle *rh = ((struct rpc_handle_args *)args)->rh;
	uint32_t offset = ((struct rpc_handle_args *)args)->offset;
	int ret = 0;
        uint16_t nid = 0;
	uint16_t dest_nid;
        uint16_t op = 0;
        struct rdma_work *rw;
        struct rpc_header *rhp;
        const struct ib_send_wr *bad_wr = NULL;
        char *rpc_buffer;
	int i, j, nr_pages, window_size;
	struct evict_info *ei;
	struct list_head *pos, *n;
	struct timespec start_tv, end_tv;
	unsigned long elapsed;
	LIST_HEAD(addr_list);
	int req_cnt = 0;
	int ack_cnt = 0;
	
	getnstimeofday(&start_tv);

        rhp = (struct rpc_header *) (rh->rpc_buffer + offset);

        rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));

        nid = rhp->nid;
        op = rhp->op;
	dest_nid = *(uint32_t *) (rpc_buffer);

	__connect_to_server(dest_nid, QP_FETCH, BACKUP_ASYNC);
	__connect_to_server(dest_nid, QP_EVICT, BACKUP_ASYNC);

	nr_pages = 256;
	window_size = 256;
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

		req_cnt++;
		ret = rmm_evict_async(dest_nid, &addr_list, nr_pages, &ack_cnt);

		if (i % window_size == 0)
			while (!(req_cnt == ack_cnt))
				cpu_relax();

		printk("req: %d, ack: %d\n", req_cnt, ack_cnt);

		list_for_each_safe(pos, n, &addr_list) {
			ei = list_entry(pos, struct evict_info, next);
			kfree(ei);
		}
	}

	while (!(req_cnt == ack_cnt))
		cpu_relax();
        
	rw = __get_rdma_work(rh, rh->rpc_dma_addr + offset, sizeof(struct rpc_header) + 4, rh->remote_dma_addr + offset, rh->rpc_rkey);
        if (!rw)
                return -ENOMEM;
        rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
        rw->wr.wr.send_flags = IB_SEND_SIGNALED;
        rw->rh = rh;

        DEBUG_LOG(PFX "nid: %d, offset: %d, op: %d\n", nid, offset, op);
	DEBUG_LOG(PFX "ret in %s, %d\n", __func__, ret);

	//*((int *) rpc_buffer) = ret; FIXME
	rhp->req = false;
        ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
        __put_rdma_work_nonsleep(rh, rw);
        if (ret || bad_wr) {
                printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
                if (bad_wr)
                        ret = -EINVAL;
        }

	getnstimeofday(&end_tv);
	elapsed = (end_tv.tv_sec - start_tv.tv_sec);
	printk(KERN_INFO PFX "replicate total elapsed time %lu (s)\n", elapsed);

	kfree(args);

        return ret;
}

static int rpc_handle_replicate_mem(struct rdma_handle *rh, uint32_t offset)
{
	struct rpc_handle_args *args = kmalloc(sizeof(struct rpc_handle_args), GFP_KERNEL);  
	args->rh = rh;
	args->offset = offset;
	writeback_dirty_log = true;
	writeback_dirty_list_size = 0;
	INIT_LIST_HEAD(&writeback_dirty_list);
	
	kthread_run(__rpc_handle_replicate_mem, args, "__rpc_handle_replicate_backup");	

	return 0;
}

static int rpc_handle_writeback_dirty_mem(struct rdma_handle *rh, uint32_t offset)
{
	int nr_pages, size, ret = 0;
        uint16_t nid = 0;
        uint16_t op = 0;
        dma_addr_t dma_addr, remote_dma_addr;
        struct rdma_work *rw;
        struct rpc_header *rhp;
        const struct ib_send_wr *bad_wr = NULL;
        char *rpc_buffer;
	uint32_t dest_nid;
	LIST_HEAD(addr_list);
	int i, j;
	struct evict_info *ei;
	struct list_head *pos, *n;

        rhp = (struct rpc_header *) (rh->rpc_buffer + offset);
        rpc_buffer = (rh->rpc_buffer + offset + sizeof(struct rpc_header));
        dma_addr = rh->rpc_dma_addr + offset;
        remote_dma_addr = rh->remote_rpc_dma_addr + offset;
	dest_nid = *(uint32_t *)(rpc_buffer);

        nid = rhp->nid;
        op = rhp->op;

	nr_pages = 256;
	for (i = 0; i < writeback_dirty_list_size; i += nr_pages) {
		INIT_LIST_HEAD(&addr_list);
		j = 0;
		ei = NULL;

		if (writeback_dirty_list_size - i < nr_pages) {
			ei = list_last_entry(&writeback_dirty_list, struct evict_info, next);
			size = writeback_dirty_list_size - i;
		} 
		else {
			list_for_each_safe(pos, n, &writeback_dirty_list) {
				j++;
				if (j == nr_pages) {
					ei = list_entry(pos, struct evict_info, next);
					break;
				}
			}
			size = nr_pages;
		}

		list_cut_position(&addr_list, &writeback_dirty_list, &ei->next); 
		rmm_evict(dest_nid, &addr_list, size);
	}

        rw = __get_rdma_work(rh, dma_addr, sizeof(struct rpc_header), remote_dma_addr, rh->rpc_rkey);
        if (!rw)
                return -ENOMEM;

        rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
        rw->wr.wr.send_flags = IB_SEND_SIGNALED;
        rw->rh = rh;

        DEBUG_LOG(PFX "nid: %d, offset: %d, op: %d\n", nid, offset, op);
        printk(PFX "nid: %d, offset: %d, op: %d\n", nid, offset, op);

	rhp->req = false;
        ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
        __put_rdma_work_nonsleep(rh, rw);
        if (ret || bad_wr) {
                printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
                if (bad_wr)
                        ret = -EINVAL;
        }

	writeback_dirty_log = false;
	add_node_to_group(MEM_GID, dest_nid, BACKUP_ASYNC);

        return ret;
}
#endif


#ifndef CONFIG_RM
/* rpc handle functions for cpu server */
static int rpc_handle_fetch_done(struct rdma_handle *rh, uint32_t offset)
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
		set_bit(RP_FETCHED, rpage_flags);
		ring_buffer_put(rh->rb, rh->rpc_buffer + offset);
	}

	return 0;
}

static int rpc_handle_prefetch_done(struct rdma_handle *rh, uint32_t offset)
{
	unsigned long *rpage_flags = NULL;
	struct rpc_header *rhp;
	uint8_t *temp, *args, *l_vaddr_ptr, *rpage_flags_ptr, *page_ptr;
	int num_page;
	int i;

	rhp = (struct rpc_header *) (rh->rpc_buffer + offset);
	args = (uint8_t *)rhp + sizeof(struct rpc_header);
	num_page = (*(int32_t *)args);
	temp = args + 4;
	l_vaddr_ptr = temp + 8 * num_page;
	rpage_flags_ptr = l_vaddr_ptr + 8 * num_page;
	page_ptr = rpage_flags_ptr + 8 * num_page;

	// memcpy and set RPAGE_FETCHED
	for(i = 0; i < num_page; i++) {
		// memcpy page to l_vaddr
		memcpy((void *)*((uint64_t*)l_vaddr_ptr), page_ptr, PAGE_SIZE);
		l_vaddr_ptr += 8;
		page_ptr += PAGE_SIZE;
		rpage_flags = (unsigned long *)*((uint64_t*)rpage_flags_ptr);
		set_bit(RP_FETCHED, rpage_flags);
		rpage_flags_ptr += 8;
	}
	ring_buffer_put(rh->rb, rh->rpc_buffer + offset);

	return 0;
}

static int rpc_handle_synchronize_done(struct rdma_handle *rh, uint32_t offset)
{
	uint8_t *buffer = rh->dma_buffer + offset;
	int *done;
	
	add_node_to_group(MEM_GID, 2, PRIMARY);
	remove_node_from_group(MEM_GID, 1, PRIMARY);
	//add_node_to_group(MEM_GID, 1, SECONDARY);
	remove_node_from_group(MEM_GID, 2, SECONDARY);

	done = (int *)(buffer + sizeof(struct rpc_header) + 4);
	*done = 1;

	return 0;
}

static int rpc_handle_replicate_done(struct rdma_handle *rh, uint32_t offset)
{
	uint8_t *buffer = rh->dma_buffer + offset;
	uint32_t dest_nid = *(uint32_t *)(buffer + sizeof(struct rpc_header));
	struct evict_info *ei;
	struct list_head *pos, *n;
	struct timespec start_tv, end_tv;
	unsigned long elapsed;

	getnstimeofday(&start_tv);

	rmm_writeback_dirty(rh->nid, dest_nid);

	getnstimeofday(&end_tv);
	elapsed = (end_tv.tv_sec - start_tv.tv_sec);
	printk(KERN_INFO PFX "evict dirty done total elapsed time %lu (s)\n", elapsed);

	list_for_each_safe(pos, n, &writeback_dirty_list) {
		ei = list_entry(pos, struct evict_info, next);
		kfree(ei);
	}
	
	writeback_dirty_list_size = 0;
	ring_buffer_put(rh->rb, buffer);

	return 0;
}

static int rpc_handle_writeback_dirty_done(struct rdma_handle *rh, uint32_t offset)
{
	uint8_t *buffer = rh->dma_buffer + offset;
	int *done;

	done = (int *)(buffer + sizeof(struct rpc_header) + 4);
	*done = 1;

	return 0;
}

#endif

#ifdef CONFIG_RM 
void regist_handler(Rpc_handler rpc_table[])
{
	rpc_table[RPC_OP_FETCH] = rpc_handle_fetch_mem;
	rpc_table[RPC_OP_EVICT] = rpc_handle_evict_mem;
	rpc_table[RPC_OP_ALLOC] = rpc_handle_alloc_free_mem;
	rpc_table[RPC_OP_FREE] = rpc_handle_alloc_free_mem;
	rpc_table[RPC_OP_PREFETCH] = rpc_handle_prefetch_mem;
	rpc_table[RPC_OP_SYNCHRONIZE] = rpc_handle_synchronize_mem;
	rpc_table[RPC_OP_REPLICATE] = rpc_handle_replicate_mem;
	rpc_table[RPC_OP_WRITEBACK_DIRTY] = rpc_handle_writeback_dirty_mem;
}
#else
void regist_handler(Rpc_handler rpc_table[])
{
	rpc_table[RPC_OP_FETCH] = rpc_handle_fetch_done;
	//rpc_table[RPC_OP_EVICT] = rpc_handle_evict_done;
	//rpc_table[RPC_OP_ALLOC] = rpc_handle_alloc_free_done;
	//rpc_table[RPC_OP_FREE] = rpc_handle_alloc_free_done;
	rpc_table[RPC_OP_PREFETCH] = rpc_handle_prefetch_done;
	rpc_table[RPC_OP_SYNCHRONIZE] = rpc_handle_synchronize_done;
	rpc_table[RPC_OP_REPLICATE] = rpc_handle_replicate_done;
}
#endif
