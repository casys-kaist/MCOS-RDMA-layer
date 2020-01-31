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
#include <asm/cacheflush.h>

#include <rdma/rdma_cm.h>

#include "common.h"
#include "ring_buffer.h"
#include "rmm.h"


static int debug = 0;
module_param(debug, int, 0);
MODULE_PARM_DESC(debug, "Debug level (0=none, 1=all)");
static int server = 0;

static struct completion done_worker[NR_WORKER_THREAD];
static struct worker_thread w_threads[NR_WORKER_THREAD];
static int HEAD = 0;

/* RDMA handle for each node */
static struct rdma_handle server_rh;
static struct rdma_handle *rdma_handles[MAX_NUM_NODES] = { NULL };
static struct rdma_handle *rdma_handles_evic[MAX_NUM_NODES] = { NULL };
static struct pool_info *rpc_pools[MAX_NUM_NODES] = { NULL };
static struct pool_info *rpc_pools_evic[MAX_NUM_NODES] = { NULL  };
static struct pool_info *sink_pools[MAX_NUM_NODES] = { NULL };
static struct pool_info *sink_pools_evic[MAX_NUM_NODES] = { NULL };

/* Global protection domain (pd) and memory region (mr) */
static struct ib_pd *rdma_pd = NULL;
/*Global CQ */
static struct ib_cq *rdma_cq = NULL;

static struct task_struct *accept_k = NULL;
static struct task_struct *polling_k = NULL;

/*proc file for control */
static struct proc_dir_entry *rmm_proc;

/*Variables for server */
static uint32_t my_ip;

/*buffer for test */
char *my_data[MAX_NUM_NODES] = { NULL };

int read_test(int nid)
{
	int ret = 0;
	struct rdma_handle *rh = rdma_handles[nid];
	struct ib_rdma_wr wr = {0};
	const struct ib_send_wr *bad_wr = NULL;
	struct ib_sge sg = {0};

	wr.remote_addr = rh->remote_rpc_dma_addr;
	wr.rkey = rh->rpc_rkey;

	sg.addr = rh->rpc_dma_addr;
	sg.length = sizeof(int);
	sg.lkey = rdma_pd->local_dma_lkey;

	wr.wr.next = NULL;
	wr.wr.num_sge = 1;
	wr.wr.wr_id = (u64) rh;
	wr.wr.sg_list = &sg;
	wr.wr.opcode = IB_WR_RDMA_READ;
	wr.wr.send_flags = IB_SEND_SIGNALED;
	
	printk(PFX "before read: %d from %llx\n", *((int *) rh->rpc_buffer), rh->remote_rpc_dma_addr);
	ret = ib_post_send(rh->qp, &wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk("Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		return ret;
	}
	wait_for_completion_interruptible(&rh->init_done);

	printk(PFX "after read: %d\n", *((int *) rh->rpc_buffer));

	return 0;
}

int rmm_alloc(int nid, u64 vaddr)
{
	int i, ret = 0;
	uint8_t *rpc_buffer;
	dma_addr_t rpc_dma_addr, remote_rpc_dma_addr;
	struct rdma_handle *rh = rdma_handles[nid];	
	struct rdma_work *rw;
	const struct ib_send_wr *bad_wr = NULL;
	bool done_copy;

	i = __get_rpc_buffer(rh);
	rpc_buffer = ((uint8_t *) rh->rpc_buffer) + (i * RPC_ARGS_SIZE);
	rpc_dma_addr = rh->rpc_dma_addr + (i * RPC_ARGS_SIZE);
	remote_rpc_dma_addr = rh->remote_rpc_dma_addr + (i * RPC_ARGS_SIZE);

	rw = __get_rdma_work(rh, rpc_dma_addr, RPC_ARGS_SIZE, remote_rpc_dma_addr, rh->rpc_rkey);
	rw->wr.wr.ex.imm_data = cpu_to_be32((i << 16) | rw->id | 0x8000);
	rw->work_type = WORK_TYPE_RPC_REQ;
	rw->addr = rpc_buffer;
	rw->dma_addr = rpc_dma_addr;
	rw->done = false;
	rw->rh = rh;

	/*copy rpc args to buffer */
	*((int *) rpc_buffer) = nid;
	*((uint64_t *) (rpc_buffer + sizeof(int))) = vaddr;

	DEBUG_LOG(PFX "nid: %d remote addr: %llx\n", nid, remote_rpc_dma_addr);

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put;
	}

	DEBUG_LOG(PFX "wait %p\n", rw);

	while(!(done_copy = rw->done))
		cpu_relax();

	DEBUG_LOG(PFX "alloc done\n");
put:
	__put_rpc_buffer(rh, i);
	__put_rdma_work(rh, rw);

	return ret;
}

int rmm_fetch(int nid, void *src, void * r_vaddr, unsigned int order)
{
	int i, ret = 0;
	uint8_t *rpc_buffer;
	dma_addr_t rpc_dma_addr, remote_rpc_dma_addr;
	struct rdma_handle *rh = rdma_handles[nid];	
	struct rdma_work *rw;
	const struct ib_send_wr *bad_wr = NULL;
	bool done_copy;

	i = __get_rpc_buffer(rh);
	rpc_buffer = ((uint8_t *) rh->rpc_buffer) + (i * RPC_ARGS_SIZE);
	rpc_dma_addr = rh->rpc_dma_addr + (i * RPC_ARGS_SIZE);
	remote_rpc_dma_addr = rh->remote_rpc_dma_addr + (i * RPC_ARGS_SIZE);

	rw = __get_rdma_work(rh, rpc_dma_addr, RPC_ARGS_SIZE, remote_rpc_dma_addr, rh->rpc_rkey);
	rw->wr.wr.ex.imm_data = cpu_to_be32((i << 16) | rw->id);
	rw->work_type = WORK_TYPE_RPC_REQ;
	rw->addr = rpc_buffer;
	rw->dma_addr = rpc_dma_addr;
	rw->done = false;
	rw->src = src;
	rw->rh = rh;

	DEBUG_LOG(PFX "i: %d, id: %d, imm_data: %X\n", i, rw->id, rw->wr.wr.ex.imm_data);

	/*copy rpc args to buffer */
	*((uint64_t *) (rpc_buffer)) = (uint64_t)  r_vaddr;
	*((uint64_t *) (rpc_buffer + 8)) = order;

	DEBUG_LOG(PFX " r_vaddr: %llX %llX\n", (uint64_t)  r_vaddr, *((uint64_t *) (rpc_buffer)));
	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put;
	}

	DEBUG_LOG(PFX "wait %p\n", rw);

	while(!(done_copy = rw->done))
		cpu_relax();

	//DEBUG_LOG(PFX "reclaim done %s\n", my_data);

put:
	__put_rpc_buffer(rh, i);
	__put_rdma_work(rh, rw);

	return ret;
}

int rmm_evict(int nid, struct evict_info *el, int num_page)
{
	int offset;
	uint8_t *evict_buffer, *temp;
	dma_addr_t evict_dma_addr, remote_evict_dma_addr;
	struct rdma_handle *rh = rdma_handles_evic[nid];	
	struct rdma_work *rw;
	struct list_head *l;
	bool done_evict;

	/* 8 is space for address */
	int ret = 0;
	int size = 6 + ((8 + PAGE_SIZE) * num_page);
	const struct ib_send_wr *bad_wr = NULL;

	evict_buffer = ring_buffer_get_mapped(rh->rb, size , &evict_dma_addr);
	offset = evict_buffer - (uint8_t *) rh->evict_buffer;
	remote_evict_dma_addr = rh->remote_rpc_dma_addr + offset;

	rw = __get_rdma_work(rh, evict_dma_addr, size, remote_evict_dma_addr, rh->rpc_rkey);
	rw->wr.wr.ex.imm_data = cpu_to_be32(offset);
	rw->work_type = WORK_TYPE_RPC_REQ;
	rw->addr = evict_buffer;
	rw->dma_addr = evict_dma_addr;
	rw->done = false;
	rw->rh = rh;

	/*copy rpc args to buffer */
	*((int32_t *) evict_buffer) = num_page;
	*((uint16_t *) (evict_buffer + 4)) = rw->id;
	temp = evict_buffer + 6;
	list_for_each(l, &el->next) {
		struct evict_info *e = list_entry(l, struct evict_info, next);
		*((uint64_t *) (temp)) = (uint64_t) e->r_vaddr;
		memcpy(temp + 8, (void *) e->l_vaddr, PAGE_SIZE);
		temp += (8 + PAGE_SIZE);
	}

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto put;
	}

	DEBUG_LOG(PFX "wait %p\n", rw);

	while(!(done_evict = rw->done))
		cpu_relax();


put:
	ring_buffer_put(rh->rb, evict_buffer);
	__put_rdma_work(rh, rw);

	return ret;
}

static inline int __get_rpc_buffer(struct rdma_handle *rh) 
{
	int i;

	do {
		spin_lock(&rh->rpc_slots_lock);
		i = find_first_zero_bit(rh->rpc_slots, NR_RPC_SLOTS);
		if (i < NR_RPC_SLOTS) break;
		spin_unlock(&rh->rpc_slots_lock);
		WARN_ON_ONCE("recv buffer is full");
	} while (i >= NR_RPC_SLOTS);
	set_bit(i, rh->rpc_slots);
	spin_unlock(&rh->rpc_slots_lock);

	/*
	   if (addr) {
	 *addr = rdma_sink_addr + RDMA_SLOT_SIZE * i;
	 }
	 if (dma_addr) {
	 *dma_addr = __rdma_sink_dma_addr + RDMA_SLOT_SIZE * i;
	 }
	 */
	return i;
}

static inline void __put_rpc_buffer(struct rdma_handle * rh, int slot) 
{
	spin_lock(&rh->rpc_slots_lock);
	/*
	   BUG_ON(!test_bit(slot, rh->rpc_slots));
	 */
	clear_bit(slot, rh->rpc_slots);
	spin_unlock(&rh->rpc_slots_lock);
}

static inline int __get_sink_buffer(struct rdma_handle *rh, unsigned int order) 
{
	int i;
	unsigned int num_pages = 1 << order;

	do {
		spin_lock(&rh->sink_slots_lock);
		i = bitmap_find_next_zero_area(rh->sink_slots, NR_SINK_SLOTS, 0, num_pages, 0);
		if (i < NR_SINK_SLOTS) break;
		spin_unlock(&rh->sink_slots_lock);
		WARN_ON_ONCE("recv buffer is full");
	} while (i >= NR_SINK_SLOTS);
	bitmap_set(rh->sink_slots, i, num_pages);
	spin_unlock(&rh->sink_slots_lock);

	/*
	   if (addr) {
	 *addr = rdma_sink_addr + RDMA_SLOT_SIZE * i;
	 }
	 if (dma_addr) {
	 *dma_addr = __rdma_sink_dma_addr + RDMA_SLOT_SIZE * i;
	 }
	 */
	return i;
}

static inline void __put_sink_buffer(struct rdma_handle * rh, int slot, unsigned int order) 
{
	unsigned int num_pages = 1 << order;

	spin_lock(&rh->sink_slots_lock);
	bitmap_clear(rh->sink_slots, slot, num_pages);
	spin_unlock(&rh->sink_slots_lock);
}

static inline struct worker_thread *get_worker_thread(void)
{
	struct worker_thread *wt;

	wt = &w_threads[HEAD % NR_WORKER_THREAD];
	HEAD++;

	return wt;
}

static void enqueue_work(struct rdma_handle *rh, uint32_t imm_data)
{
	struct worker_thread *wt;
	struct args_worker *aw;

	wt = get_worker_thread();
	spin_lock(&wt->lock_wt);

retry:
	aw = kmalloc(sizeof(struct args_worker), GFP_KERNEL);
	if (!aw) {
		printk(KERN_ERR PFX "fail to allocate memory in %s\n", __func__);
		goto retry;
	}

	aw->rh = rh;
	aw->imm_data = imm_data;

	aw->time_dequeue_ns = 0;
	aw->time_enqueue_ns = sched_clock();

	list_add_tail(&aw->next, &wt->work_head);
	wt->num_queued++;
	spin_unlock(&wt->lock_wt);
}

/* Must hold a lock before call this function */
static struct args_worker *dequeue_work(struct worker_thread *wt)
{
	struct args_worker *aw;

	aw  = list_first_entry(&wt->work_head, struct args_worker, next);
	aw->time_dequeue_ns = sched_clock();
	list_del(&aw->next);
	wt->num_queued--;

	return aw;
}


static int __handle_rpc(struct ib_wc *wc)
{
	int ret = 0;
	struct recv_work *rw;
	struct rdma_handle *rh;
	uint32_t imm_data;
	const struct ib_recv_wr *bad_wr = NULL;

	rw = (struct recv_work *) wc->wr_id;
	rh = rw->rh;
	DEBUG_LOG(PFX "%X\n", wc->ex.imm_data);
	imm_data = be32_to_cpu(wc->ex.imm_data);

	enqueue_work(rh, imm_data);

	ret = ib_post_recv(rh->qp, &rw->wr, &bad_wr);
	if (ret || bad_wr)  {
		if (bad_wr)
			ret = -EINVAL;
		printk(KERN_ERR PFX "fail to post recv in %s\n", __func__);
	}

	return 0;
}

/*rpc handle function for mem server */

#ifdef CONFIG_RM
static int rpc_handle_fetch_mem(struct rdma_handle *rh, uint16_t id, uint16_t offset)
{
	int i, ret = 0;
	int payload_size;
	void *dest;
	void *sink_addr;
	dma_addr_t sink_dma_addr, remote_sink_dma_addr;
	unsigned int order;
	struct rdma_work *rw;
	const struct ib_send_wr *bad_wr = NULL;
	uint8_t *rpc_buffer;

	rpc_buffer = rh->rpc_buffer + (offset * RPC_ARGS_SIZE);
	dest = (void *) *((uint64_t *) (rpc_buffer)) + rh->vaddr_start;
	order = *((uint64_t *) (rpc_buffer + 8));
	payload_size = (1 << order) * PAGE_SIZE;

	DEBUG_LOG(PFX "dest: %llx order: %u num_page: %d\n", (uint64_t) dest, order, 1 << order);

	i = __get_sink_buffer(rh, order);
	sink_addr = ((uint8_t *) rh->sink_buffer) + (i * PAGE_SIZE);
	sink_dma_addr = rh->sink_dma_addr + (i * PAGE_SIZE);
	remote_sink_dma_addr = rh->remote_sink_dma_addr + (i * PAGE_SIZE);

	rw = __get_rdma_work(rh, sink_dma_addr, payload_size, remote_sink_dma_addr, rh->sink_rkey);
	rw->wr.wr.ex.imm_data = cpu_to_be32((i << 16) | id);
	rw->work_type = WORK_TYPE_RPC_ACK;
	rw->addr = sink_addr;
	rw->dma_addr = sink_dma_addr;
	rw->done = false;

	rw->rh = rh;
	rw->order = order;
	rw->slot = i;

	DEBUG_LOG(PFX "i: %d, id: %d, imm_data: %X\n", i, id, rw->wr.wr.ex.imm_data);

	memcpy(sink_addr, dest, payload_size);

	DEBUG_LOG(PFX "rkey %x, remote_dma_addr %llx\n", rh->sink_rkey, remote_sink_dma_addr);
	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p %s\n", ret, bad_wr, __func__);
		if (bad_wr)
			ret = -EINVAL;
	}

	return ret;
}

static int rpc_handle_alloc_mem(struct rdma_handle *rh, uint16_t id, uint16_t offset)
{
	int ret = 0;
	int nid = -1;
	uint64_t vaddr;
	dma_addr_t rpc_dma_addr, remote_rpc_dma_addr;
	struct rdma_work *rw;
	const struct ib_send_wr *bad_wr = NULL;
	char *rpc_buffer;

	rpc_buffer = rh->rpc_buffer + (offset * RPC_ARGS_SIZE);
	rpc_dma_addr = rh->rpc_dma_addr + (offset * RPC_ARGS_SIZE);
	remote_rpc_dma_addr = rh->remote_rpc_dma_addr + (offset * RPC_ARGS_SIZE);

	nid = *(int *)rpc_buffer;
	/*Add offset */
	vaddr = *(uint64_t *) (rpc_buffer + sizeof(int)) + rh->vaddr_start;

	rw = __get_rdma_work(rh, rpc_dma_addr, RPC_ARGS_SIZE, remote_rpc_dma_addr, rh->rpc_rkey);
	rw->wr.wr.ex.imm_data = cpu_to_be32((offset << 16) | id | 0x8000);
	rw->work_type = WORK_TYPE_RPC_ACK;
	rw->addr = rpc_buffer;
	rw->dma_addr = rpc_dma_addr;
	rw->done = false;

	rw->rh = rh;
	rw->slot = -1;

	DEBUG_LOG(PFX "nid: %d, offset: %d, id: %d, imm_data: %X\n", nid, offset, id, rw->wr.wr.ex.imm_data);

	if (nid >= 0) {
		if (rm_alloc(vaddr))
			*((int *) rpc_buffer) = 1;
		else
			*((int *) rpc_buffer) = 0;
	}
	else
		printk(KERN_ERR PFX "nid was not initialized\n");

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
	}

	return ret;
}

static int rpc_handle_evict_mem(struct rdma_handle *rh,  int offset)
{
	int i, num_page, ret;
	void *dest;
	struct rdma_work *rw;
	uint8_t *evict_buffer, *temp;
	uint16_t id;
	const struct ib_send_wr *bad_wr = NULL;

	evict_buffer = rh->evict_buffer + (offset * (8 + PAGE_SIZE));
	id = (*(uint16_t *) evict_buffer);
	num_page = (*(int32_t *) (evict_buffer + 2));
	temp = evict_buffer + 6;

	for (i = 0; i < num_page; i++) {
		dest = (void *) *((uint64_t *) (temp));
		memcpy(dest, temp + 8, PAGE_SIZE);
		temp += (8 + PAGE_SIZE);
	}

	rw = __get_rdma_work(rh, 0, 0, 0, rh->rpc_rkey);
	rw->wr.wr.ex.imm_data = cpu_to_be32(id);
	rw->work_type = WORK_TYPE_RPC_ACK;
	rw->addr = NULL;
	rw->dma_addr = 0;
	rw->done = false;

	rw->rh = rh;
	rw->slot = -1;

	ret = ib_post_send(rh->qp, &rw->wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk(KERN_ERR PFX "Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
	}

	return 0;
}



#endif

/*rpc handle functions for cpu server */
#ifndef CONFIG_RM
static int rpc_handle_fetch_cpu(struct rdma_handle *rh, uint16_t id, uint16_t offset)
{
	int num_page;
	void *sink_addr;
	dma_addr_t sink_dma_addr;
	struct rdma_work *rw;

	rw = &rh->rdma_work_pool[id];
	sink_addr = (uint8_t *) rh->sink_buffer + (offset * PAGE_SIZE);
	sink_dma_addr = rh->sink_dma_addr + (offset * PAGE_SIZE);
	num_page = 1 << rw->order;

	memcpy(rw->src, sink_addr, PAGE_SIZE * num_page);

	rw->done = true;
	DEBUG_LOG(PFX "done %p %d\n", rw, rw->done);

	return 0;
}

static int rpc_handle_alloc_cpu(struct rdma_handle *rh, uint16_t id, uint16_t offset)
{
	void *rpc_addr;
	dma_addr_t rpc_dma_addr;
	struct rdma_work *rw;
	int nid;
	int ret = 0;

	nid = rh->nid;
	rw = &rh->rdma_work_pool[id];
	rpc_addr = (uint8_t *) rh->rpc_buffer + (offset * RPC_ARGS_SIZE);
	rpc_dma_addr = rh->rpc_dma_addr + (offset * RPC_ARGS_SIZE);

	ret =  *((int *) rpc_addr);
	rw->done = true;
	DEBUG_LOG(PFX "allocated %d\n", ret);
	DEBUG_LOG(PFX "done %p %d\n", rw, rw->done);

	return ret;
}

static int rpc_handle_evict_cpu(struct rdma_handle *rh,  int id)
{
	struct rdma_work *rw = &rh->rdma_work_pool[id];

	rw->done = true;

	return 0;
}

#endif

static int __handle_recv(struct ib_wc *wc)
{
	int ret = 0;
	struct pool_info *rp;
	struct recv_work *rw;
	struct rdma_handle *rh;

	rw = (struct recv_work *) wc->wr_id;
	rh = rw->rh;
	switch (rw->work_type) {
		case WORK_TYPE_RPC_ADDR:
			ib_dma_unmap_single(rh->device, rw->dma_addr, 
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			if (rh->connection_type == CONNECTION_FETCH)
				rp = rpc_pools[rh->nid];
			else
				rp = rpc_pools_evic[rh->nid];
			rh->remote_rpc_dma_addr = rp->addr;
			rh->remote_rpc_size = rp->size;
			rh->rpc_rkey = rp->rkey;
			kfree(rw);
			DEBUG_LOG(PFX "recv rpc addr %llx %x\n", rh->remote_rpc_dma_addr, rh->rpc_rkey);
			complete(&rh->init_done);
			break;

		case WORK_TYPE_SINK_ADDR:
			ib_dma_unmap_single(rh->device, rw->dma_addr, 
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			if (rh->connection_type == CONNECTION_FETCH)
				rp = sink_pools[rh->nid];
			else
				rp = sink_pools_evic[rh->nid];
			rh->remote_sink_dma_addr = rp->addr;
			rh->remote_sink_size = rp->size;
			rh->sink_rkey = rp->rkey;
			kfree(rw);
			DEBUG_LOG(PFX "recv sink addr %llx %x\n", rh->remote_sink_dma_addr, rh->sink_rkey);
			complete(&rh->init_done);
			break;

		default:
			DEBUG_LOG(PFX "error: unknown id\n");
			ret = -1;
			break;
	}

	return 0;
}

static int __handle_send(struct ib_wc *wc)
{
	int ret = 0;
	struct pool_info *rp;
	struct send_work *sw;
	struct rdma_handle *rh;
	dma_addr_t dma_addr;

	sw = (struct send_work *) wc->wr_id;
	dma_addr = sw->sgl.addr;
	rh = sw->rh;
	switch (sw->work_type) {
		case WORK_TYPE_SEND_ADDR:
			ib_dma_unmap_single(rh->device, dma_addr, 
					sizeof(struct pool_info), DMA_TO_DEVICE);
			rp = (struct pool_info *) sw->addr;

			kfree(sw);
			DEBUG_LOG(PFX "send addr %llx completed\n", rp->addr);
			break;

		default:
			DEBUG_LOG(PFX "error: unknown id\n");
			ret = -1;
			break;
	}

	return 0;
}

static int put_work(struct ib_wc *wc)
{
	struct rdma_work *rw;
	struct rdma_handle *rh;
	int slot, order;

	rw = (struct rdma_work *) wc->wr_id;
	rh = rw->rh;

	slot = rw->slot;
	order = rw->order;

	if (slot >= 0)
		__put_sink_buffer(rh, slot, order);
	__put_rdma_work(rh, rw);

	return 0;
}

static int polling_cq(void * args)
{
	struct ib_cq *cq = (struct ib_cq *) args;
	struct ib_wc wc = {0};
	struct rdma_work *rw;
	struct rdma_handle *rh;
	int ret = 0;

	DEBUG_LOG(PFX "Polling thread now running\n");

	while ((ret = ib_poll_cq(cq, 1, &wc)) >= 0) {
		if (kthread_should_stop())
			return 0;
		if (ret == 0)
			continue;

		if (wc.status) {
			if (wc.status == IB_WC_WR_FLUSH_ERR) {
				DEBUG_LOG("cq flushed\n");
				continue;
			} 
			else {
				printk(KERN_ERR PFX "cq completion failed with "
						"wr_id %Lx status %d opcode %d vendor_err %x\n",
						wc.wr_id, wc.status, wc.opcode, wc.vendor_err);
				goto error;
			}
		}

		switch (wc.opcode) {
			case IB_WC_SEND:
				DEBUG_LOG(PFX "send completion\n");
				__handle_send(&wc);
				break;

			case IB_WC_RDMA_WRITE:
				if (wc.wc_flags == IB_WC_WITH_IMM) {
					DEBUG_LOG(PFX "rdma write-imm completion\n");
					rw = (struct rdma_work *) wc.wr_id;

					if (rw->work_type == WORK_TYPE_RPC_ACK) {
						if (server)
							put_work(&wc);
					}
					else if (rw->work_type == WORK_TYPE_RPC_REQ) {
						/*
						if (rw->rh->connection_type == CONNECTION_EVICT)
							rw->done = true;
							*/
					}
				}
				else {
					DEBUG_LOG(PFX "rdma write completion\n");
				}
				break;

			case IB_WC_RDMA_READ:
				rh = (struct rdma_handle *) wc.wr_id; 
				complete(&rh->init_done);
				DEBUG_LOG(PFX "rdma read completion\n");
				break;

			case IB_WC_RECV:
				DEBUG_LOG(PFX "recv completion\n");
				__handle_recv(&wc);
				break;
			case IB_WC_RECV_RDMA_WITH_IMM:
				DEBUG_LOG(PFX "recv rpc\n");
				__handle_rpc(&wc);
				break;

			case IB_WC_REG_MR:
				rh = (struct rdma_handle *) wc.wr_id; 
				complete(&rh->init_done);
				DEBUG_LOG(PFX "mr registered\n");
				break;

			default:
				printk(KERN_ERR PFX
						"%s:%d Unexpected opcode %d\n",
						__func__, __LINE__, wc.opcode);
				goto error;
		}
	}

	return 0;
error:
	return -1;
}

/*
   static int rmm_wait_for_completion(struct rdma_handle *rh, enum ib_wc_opcode exit_condition)
   {
   struct ib_wc wc = {0};
//const struct ib_recv_wr *bad_wr;
int ret;

DEBUG_LOG(PFX "wait CQE: %d\n", exit_condition);
while ((ret = ib_poll_cq(rh->cq, 1, &wc)) >= 0) {
if (ret == 0) {
if (rh->state == RDMA_CLOSED)
goto  error;
continue;
}

if (wc.status) {
if (wc.status == IB_WC_WR_FLUSH_ERR) {
DEBUG_LOG("cq flushed\n");
continue;
} 
else {
printk(KERN_ERR PFX "cq completion failed with "
"wr_id %Lx status %d opcode %d vendor_err %x\n",
wc.wr_id, wc.status, wc.opcode, wc.vendor_err);
goto error;
}
}

switch (wc.opcode) {
case IB_WC_SEND:
DEBUG_LOG(PFX "send completion\n");
if (wc.wr_id == WORK_TYPE_RPC_ADDR)
DEBUG_LOG(PFX "send rpc addr %llx\n", rh->rpc_dma_addr);
else if (wc.wr_id == WORK_TYPE_SINK_ADDR)
DEBUG_LOG(PFX "send sink addr %llx\n", sink_dma_addr);
break;

case IB_WC_RDMA_WRITE:
DEBUG_LOG(PFX "rdma write completion\n");
break;

case IB_WC_RDMA_READ:
DEBUG_LOG(PFX "rdma read completion\n");
break;

case IB_WC_RECV:
DEBUG_LOG(PFX "recv completion\n");
__handle_recv(&wc);
break;

case IB_WC_REG_MR:
DEBUG_LOG(PFX "mr registered\n");
break;

default:
printk(KERN_ERR PFX
"%s:%d Unexpected opcode %d\n",
__func__, __LINE__, wc.opcode);
goto error;
}
if (wc.opcode == exit_condition)
break;
}

return 0;
error:
return -1;
}
 */

void cq_comp_handler(struct ib_cq *cq, void *context)
{
	int ret;
	struct ib_wc wc;

retry:
	while ((ret = ib_poll_cq(cq, 1, &wc)) > 0) {
		if (wc.opcode < 0 || wc.status) {
			//__process_faulty_work(&wc);
			continue;
		}
		switch(wc.opcode) {
			case IB_WC_SEND:
				//__process_sent(&wc);
				break;
			case IB_WC_RECV:
				//__process_recv(&wc);
				break;
			case IB_WC_RDMA_WRITE:
			case IB_WC_RDMA_READ:
				//__process_rdma_completion(&wc);
				break;
			case IB_WC_REG_MR:
				//__process_comp_wakeup(&wc, "mr registered\n");
				break;
			default:
				printk("Unknown completion op %d\n", wc.opcode);
				break;
		}
	}
	ret = ib_req_notify_cq(cq, IB_CQ_NEXT_COMP | IB_CQ_REPORT_MISSED_EVENTS);
	if (ret > 0) goto retry;
}


/****************************************************************************
 * Setup connections
 */

/*setup qp*/
static int __setup_pd_cq_qp(struct rdma_handle *rh)
{
	int ret = 0;
	int i;

	BUG_ON(rh->state != RDMA_ROUTE_RESOLVED && "for rh->device");

	/* Create global pd if it is not allocated yet */
	DEBUG_LOG(PFX "alloc pd\n");
	if (!rdma_pd) {
		rdma_pd = ib_alloc_pd(rh->device, 0);
		if (IS_ERR(rdma_pd)) {
			ret = PTR_ERR(rdma_pd);
			rdma_pd = NULL;
			goto out_err;
		}
	}

	/* Create global completion queue */
	DEBUG_LOG(PFX "alloc cq\n");
	if (!rdma_cq) {
		struct ib_cq_init_attr cq_attr = {
			.cqe = (MAX_SEND_DEPTH + MAX_RECV_DEPTH + NR_RPC_SLOTS) * MAX_NUM_NODES,
			.comp_vector = 0,
		};

		DEBUG_LOG(PFX "call create cq\n");
		rdma_cq = ib_create_cq(
				rh->device, cq_comp_handler, NULL, rh, &cq_attr);
		if (IS_ERR(rdma_cq)) {
			ret = PTR_ERR(rdma_cq);
			goto out_err;
		}

		/*
		   ret = ib_req_notify_cq(rh->cq, IB_CQ_NEXT_COMP);
		   if (ret < 0) goto out_err;
		 */

		DEBUG_LOG(PFX "create polling thread\n");
		polling_k = kthread_create(polling_cq, rdma_cq, "polling_cq");
		if (IS_ERR(polling_k))
			goto out_err;
		kthread_bind(polling_k, 14);
		wake_up_process(polling_k); 
		for (i = 0; i < NR_WORKER_THREAD; i++)
			complete(&done_worker[i]);
	}
	rh->cq = rdma_cq;

	/* create queue pair */
	DEBUG_LOG(PFX "create qp\n");
	{
		struct ib_qp_init_attr qp_attr = {
			.event_handler = NULL, // qp_event_handler,
			.qp_context = rh,
			.cap = {
				.max_send_wr = MAX_SEND_DEPTH,
				.max_recv_wr = MAX_RECV_DEPTH,
				.max_send_sge = 10,
				.max_recv_sge = 10,
				//	.max_inline_data = IMM_DATA_SIZE,
			},
			.sq_sig_type = IB_SIGNAL_REQ_WR,
			.qp_type = IB_QPT_RC,
			.send_cq = rdma_cq,
			.recv_cq = rdma_cq,
		};

		ret = rdma_create_qp(rh->cm_id, rdma_pd, &qp_attr);
		if (ret) 
			goto out_err;
		rh->qp = rh->cm_id->qp;
	}

	return 0;

out_err:
	return ret;
}

/* Alloc dma buffer */
static  int __setup_dma_buffer(struct rdma_handle *rh)
{
	int ret = 0;
	dma_addr_t dma_addr;
	size_t buffer_size = RPC_BUFFER_SIZE + SINK_BUFFER_SIZE;
	struct ib_reg_wr reg_wr = {
		.wr = {
			.opcode = IB_WR_REG_MR,
			.send_flags = IB_SEND_SIGNALED,
			.wr_id = (u64) rh,
		},
		.access = IB_ACCESS_REMOTE_WRITE | 
			IB_ACCESS_LOCAL_WRITE , 
	};
	const struct ib_send_wr *bad_wr = NULL;
	struct scatterlist sg = {};
	struct ib_mr *mr;
	char *step = NULL;


	step = "alloc dma buffer";
	rh->dma_buffer = ib_dma_alloc_coherent(rh->device, buffer_size, &dma_addr, GFP_KERNEL);
	if (!rh->dma_buffer) {
		return -ENOMEM;
	}

	step = "alloc mr";
	mr = ib_alloc_mr(rdma_pd, IB_MR_TYPE_MEM_REG, buffer_size / PAGE_SIZE);
	if (IS_ERR(mr)) {
		ret = PTR_ERR(mr);
		goto out_free;
	}

	sg_dma_address(&sg) = dma_addr;
	sg_dma_len(&sg) = buffer_size;

	step = "map mr";
	ret = ib_map_mr_sg(mr, &sg, 1, NULL, buffer_size);
	if (ret != 1) {
		printk(PFX "Cannot map scatterlist to mr, %d\n", ret);
		goto out_dereg;
	}
	reg_wr.mr = mr;
	reg_wr.key = mr->rkey;

	step = "post reg mr";
	ret = ib_post_send(rh->qp, &reg_wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk(PFX "Cannot register mr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto
			out_dereg;
	}

	ret = wait_for_completion_interruptible(&rh->init_done);
	if (ret) 
		goto out_dereg;

	rh->dma_addr = dma_addr;
	rh->mr = mr;

	rh->rpc_buffer = rh->dma_buffer;
	rh->rpc_dma_addr = rh->dma_addr;
	rh->rpc_buffer_size = RPC_BUFFER_SIZE;

	rh->sink_buffer = (void *) ((uint8_t *) rh->dma_buffer) + RPC_BUFFER_SIZE;
	rh->sink_dma_addr = rh->dma_addr + RPC_BUFFER_SIZE;

	rh->evict_buffer = rh->dma_buffer;
	rh->evict_dma_addr = rh->dma_addr;

	if (rh->connection_type == CONNECTION_EVICT)
		rh->rb = ring_buffer_create(dma_addr, rh->dma_buffer, "evict buffer"); 

	/*
	if (server) {
		memset(rh->dma_buffer, -1, buffer_size);
		printk(PFX "init value: %d\n", *((int *) rh->dma_buffer));
		//clflush_cache_range(rh->dma_buffer, buffer_size);
		//		flush_cache_vmap(rh->dma_buffer, rh->dma_buffer + buffer_size);
	}
	else {
		memset(rh->dma_buffer, 32, buffer_size);
		printk(PFX "init value: %d\n", *((int *) rh->dma_buffer));
	}
	//	flush_cache_vmap(rh->dma_buffer, rh->dma_buffer + buffer_size);
	//clflush_cache_range(rh->dma_buffer, buffer_size);
	*/

	return 0;
out_dereg:
	ib_dereg_mr(mr);

out_free:
	if (rh->dma_buffer)
		ib_dma_free_coherent(rh->device, buffer_size, rh->dma_buffer, rh->dma_addr);
	printk(KERN_ERR PFX "fail at %s\n", step);
	return ret;
}

static int __setup_recv_addr(struct rdma_handle *rh, enum wr_type work_type)
{
	int ret = 0;
	dma_addr_t dma_addr;
	struct ib_recv_wr *wr;
	const struct ib_recv_wr *bad_wr = NULL;
	struct ib_sge sgl = {0};
	struct pool_info *pp;
	struct recv_work *rw;

	if (work_type == WORK_TYPE_RPC_ADDR) {
		if (rh->connection_type == CONNECTION_FETCH)
			pp = rpc_pools[rh->nid];
		else
			pp = rpc_pools_evic[rh->nid];
	}
	else {
		if (rh->connection_type == CONNECTION_FETCH)
			pp = sink_pools[rh->nid];
		else
			pp = sink_pools_evic[rh->nid];
	}

	rw = kmalloc(sizeof(struct recv_work), GFP_KERNEL);
	if (!rw)
		return -ENOMEM;

	/*Post receive request for recieve request pool info*/
	dma_addr = ib_dma_map_single(rh->device, pp, sizeof(struct pool_info), DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret) 
		goto out_free;

	rw->work_type = work_type; 
	rw->dma_addr = dma_addr;
	rw->addr = pp;
	rw->rh = rh;

	sgl.lkey = rdma_pd->local_dma_lkey;
	sgl.addr = dma_addr;
	sgl.length = sizeof(struct pool_info);

	wr = &rw->wr;
	wr->sg_list = &sgl;
	wr->num_sge = 1;
	wr->next = NULL;
	wr->wr_id = (u64) rw; 

	ret = ib_post_recv(rh->qp, wr, &bad_wr);
	if (ret || bad_wr) 
		goto out_free;

	return ret;

out_free:
	ib_dma_unmap_single(rh->device, dma_addr, sizeof(struct pool_info), DMA_FROM_DEVICE);
	return ret;
}

/*pre alloc rdma work for rpc request */
static int __refill_rdma_work(struct rdma_handle *rh, int nr_works)
{
	int i;
	int nr_refilled = 0;
	struct rdma_work *work_list = NULL;
	struct rdma_work *last_work = NULL;

	rh->rdma_work_pool = kzalloc(sizeof(struct rdma_work) * nr_works, GFP_KERNEL);
	if (!rh->rdma_work_pool)
		return -ENOMEM;

	for (i = 0; i < nr_works; i++) {
		struct rdma_work *rw = &rh->rdma_work_pool[i];

		//rw->work_type = WORK_TYPE_RPC;
		rw->id = i;

		rw->sgl.addr = 0;
		rw->sgl.length = 0;
		rw->sgl.lkey = rdma_pd->local_dma_lkey;

		rw->wr.wr.next = NULL;
		rw->wr.wr.wr_id = (u64)rw;
		rw->wr.wr.sg_list = &rw->sgl;
		rw->wr.wr.num_sge = 1;
		rw->wr.wr.opcode = IB_WR_RDMA_WRITE_WITH_IMM; // IB_WR_RDMA_WRITE_WITH_IMM;
		rw->wr.wr.send_flags = IB_SEND_SIGNALED;
		rw->wr.remote_addr = 0;
		rw->wr.rkey = 0;

		if (!last_work) last_work = rw;
		rw->next = work_list;
		work_list = rw;
		nr_refilled++;
	}

	spin_lock(&rh->rdma_work_head_lock);
	if (work_list) {
		last_work->next = rh->rdma_work_head;
		rh->rdma_work_head = work_list;
	}
	spin_unlock(&rh->rdma_work_head_lock);
	BUG_ON(nr_refilled == 0);
	return nr_refilled;
}

static struct rdma_work *__get_rdma_work(struct rdma_handle *rh, dma_addr_t dma_addr, size_t size, dma_addr_t rdma_addr, u32 rdma_key)
{
	struct rdma_work *rw;
	//might_sleep();

	spin_lock(&rh->rdma_work_head_lock);
	rw = rh->rdma_work_head;
	rh->rdma_work_head = rh->rdma_work_head->next;
	spin_unlock(&rh->rdma_work_head_lock);

	if (!rh->rdma_work_head)
		return NULL;

	rw->sgl.addr = dma_addr;
	rw->sgl.length = size;

	rw->wr.remote_addr = rdma_addr;
	rw->wr.rkey = rdma_key;
	return rw;
}

static void __put_rdma_work(struct rdma_handle *rh, struct rdma_work *rw)
{
	might_sleep();
	spin_lock(&rh->rdma_work_head_lock);
	rw->next = rh->rdma_work_head;
	rh->rdma_work_head = rw;
	spin_unlock(&rh->rdma_work_head_lock);
}

static int __setup_work_request_pools(struct rdma_handle *rh)
{
	int ret;
	/* Initalize rdma work request pool */
	ret = __refill_rdma_work(rh, NR_RPC_SLOTS);
	return ret;

}

static int __setup_recv_works(struct rdma_handle *rh)
{
	int ret = 0, i;
	struct recv_work *rws = NULL;

	DEBUG_LOG(PFX "post recv for exchaging dma addr\n");
	ret = __setup_recv_addr(rh, WORK_TYPE_RPC_ADDR);
	if (ret)
		return ret;

	ret = __setup_recv_addr(rh, WORK_TYPE_SINK_ADDR);
	if (ret)
		return ret;

	rws = kmalloc(sizeof(*rws) * (PAGE_SIZE / IMM_DATA_SIZE), GFP_KERNEL);
	if (!rws) {
		return -ENOMEM;
	}

	/* pre-post receive work requests-imm */
	DEBUG_LOG(PFX "post recv for imm\n");
	for (i = 0; i < MAX_RECV_DEPTH; i++) {
		struct recv_work *rw = rws + i;
		struct ib_recv_wr *wr;
		const struct ib_recv_wr *bad_wr = NULL;

		rw->work_type = WORK_TYPE_IMM;
		rw->rh = rh;

		wr = &rw->wr;
		wr->sg_list = NULL;
		wr->num_sge = 0;
		wr->next = NULL;
		wr->wr_id = (u64) rw;

		ret = ib_post_recv(rh->qp, wr, &bad_wr);
		if (ret || bad_wr) 
			return ret;
	}
	rh->recv_works = rws;

	return ret;

}

/*send dma_addr and rkey */
static int __send_dma_addr(struct rdma_handle *rh, dma_addr_t addr, size_t size)
{
	int ret = 0;
	struct send_work *sw;
	struct ib_send_wr *wr;
	const struct ib_send_wr *bad_wr = NULL;
	struct ib_sge *sgl;
	struct pool_info *rp = NULL;
	dma_addr_t dma_addr;

	rp = kmalloc(sizeof(struct pool_info), GFP_KERNEL);
	if (!rp) 
		return -ENOMEM;
	sw = kmalloc(sizeof(struct send_work), GFP_KERNEL);
	if (!sw) 
		return -ENOMEM;
	rp->rkey = rh->mr->rkey;
	rp->addr = addr;
	rp->size = size;

	dma_addr = ib_dma_map_single(rh->device, rp,
			sizeof(struct pool_info), DMA_TO_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret)
		goto err;

	sw->next = NULL;
	sw->work_type = WORK_TYPE_SEND_ADDR;
	sw->addr = rp;
	sw->rh = rh;

	wr = &sw->wr;
	sgl = &sw->sgl;

	sgl->addr = dma_addr;
	sgl->length = sizeof(struct pool_info);
	sgl->lkey = rdma_pd->local_dma_lkey;

	wr->opcode = IB_WR_SEND;
	wr->send_flags = IB_SEND_SIGNALED;
	wr->sg_list = sgl;
	wr->num_sge = 1;
	wr->wr_id = (uint64_t) sw;
	wr->next = NULL;

	ret = ib_post_send(rh->qp, wr, &bad_wr);
	if (ret || bad_wr) {
		printk("Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto
			unmap;
	}
	/*
	   ret = rmm_wait_for_completion(rh, IB_WC_SEND);
	   if (ret)
	   goto unmap;
	 */
	return 0;

unmap:
	ib_dma_unmap_single(rh->device, dma_addr, sizeof(struct pool_info), DMA_TO_DEVICE); 
err:
	kfree(rp);
	return ret;
}

/****************************************************************************
 * Client-side connection handling
 */
#ifndef CONFIG_RM
int cm_client_event_handler(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	struct rdma_handle *rh = cm_id->context;

	switch (cm_event->event) {
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			rh->state = RDMA_ADDR_RESOLVED;
			complete(&rh->cm_done);
			break;
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
			rh->state = RDMA_ROUTE_RESOLVED;
			complete(&rh->cm_done);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			rh->state = RDMA_CONNECTED;
			complete(&rh->cm_done);
			break;
		case RDMA_CM_EVENT_DISCONNECTED:
			printk(PFX "Disconnected from %d\n", rh->nid);
			/* TODO deallocate associated resources */
			break;
		case RDMA_CM_EVENT_REJECTED:
		case RDMA_CM_EVENT_CONNECT_ERROR:
			complete(&rh->cm_done);
			break;
		case RDMA_CM_EVENT_ADDR_ERROR:
			printk(PFX "addr error\n");
			break;
		case RDMA_CM_EVENT_ROUTE_ERROR:
		case RDMA_CM_EVENT_UNREACHABLE:
		default:
			printk(PFX "Unhandled client event %d\n", cm_event->event);
			break;
	}
	return 0;
}

static int __connect_to_server(int nid, int connect_type)
{
	int ret = 0;
	static int c_type;
	const char *step;
	struct rdma_handle *rh;

	if (connect_type == CONNECTION_FETCH)
		rh = rdma_handles[nid];
	else
		rh = rdma_handles_evic[nid];

	step = "create rdma id";
	DEBUG_LOG(PFX "%s\n", step);
	rh->cm_id = rdma_create_id(&init_net,
			cm_client_event_handler, rh, RDMA_PS_IB, IB_QPT_RC);
	if (IS_ERR(rh->cm_id)) 
		goto out_err;

	step = "resolve server address";
	DEBUG_LOG(PFX "%s\n", step);
	{
		struct sockaddr_in addr = {
			.sin_family = AF_INET,
			.sin_port = htons(RDMA_PORT),
			.sin_addr.s_addr = ip_table[nid],
		};

		ret = rdma_resolve_addr(rh->cm_id, NULL,
				(struct sockaddr *)&addr, RDMA_ADDR_RESOLVE_TIMEOUT_MS);
		if (ret) 
			goto out_err;
		ret = wait_for_completion_interruptible(&rh->cm_done);
		if (ret || rh->state != RDMA_ADDR_RESOLVED) 
			goto out_err;
	}

	step = "resolve routing path";
	DEBUG_LOG(PFX "%s\n", step);
	ret = rdma_resolve_route(rh->cm_id, RDMA_ADDR_RESOLVE_TIMEOUT_MS);
	if (ret) 
		goto out_err;
	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (ret || rh->state != RDMA_ROUTE_RESOLVED) 
		goto out_err;

	/* cm_id->device is valid after the address and route are resolved */
	rh->device = rh->cm_id->device;

	step = "setup ib";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __setup_pd_cq_qp(rh);
	if (ret) 
		goto out_err;

	step = "post recv works";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __setup_recv_works(rh);
	if (ret) 
		goto out_err;

	step = "setup work reqeusts";
	ret = __setup_work_request_pools(rh);
	if (ret == 0)
		goto out_err;

	step = "connect";
	DEBUG_LOG(PFX "%s\n", step);
	c_type = connect_type;
	{
		struct rdma_conn_param conn_param = {
			.private_data = &c_type,
			.private_data_len = sizeof(connect_type),
			.responder_resources = 1,
			.initiator_depth = 1,
		};

		rh->state = RDMA_CONNECTING;
		ret = rdma_connect(rh->cm_id, &conn_param);
		if (ret) 
			goto out_err;
		ret = wait_for_completion_interruptible(&rh->cm_done);
		if (ret) 
			goto out_err;
		if (rh->state != RDMA_CONNECTED) {
			ret = -ETIMEDOUT;
			goto out_err;
		}
	}

	/*Because we need to register rpc regions to mr, 
	  those functions had to call after connection*/
	step = "setup dma buffers";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __setup_dma_buffer(rh);
	if (ret)
		goto out_err;

	step = "send pool addr";
	DEBUG_LOG(PFX "%s %llx %llx\n", step, rh->rpc_dma_addr, rh->sink_dma_addr);
	ret = __send_dma_addr(rh, rh->rpc_dma_addr, RPC_BUFFER_SIZE);
	if (ret)
		goto out_err;

	ret = __send_dma_addr(rh, rh->sink_dma_addr, SINK_BUFFER_SIZE);
	if (ret)
		goto out_err;

	wait_for_completion_interruptible(&rh->init_done);
	wait_for_completion_interruptible(&rh->init_done);

	printk(PFX "Connected to %d\n", nid);
	return 0;

out_err:
	printk(KERN_ERR PFX "Unable to %s, %pI4, %d\n", step, ip_table + nid, ret);
	return ret;
}
#endif

/****************************************************************************
 * Server-side connection handling
 */


/*Function for Mem server */
#ifdef CONFIG_RM
static int __accept_client(int nid, int connect_type)
{
	struct rdma_handle *rh;
	struct rdma_conn_param conn_param = {0};
	char *step = NULL;
	int ret;

	if (connect_type == CONNECTION_FETCH)
		rh = rdma_handles[nid];
	else
		rh = rdma_handles_evic[nid];

	DEBUG_LOG(PFX "accept client %d connect_type %d\n", nid, connect_type);
	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (rh->state != RDMA_ROUTE_RESOLVED) return -EINVAL;

	step = "setup pd cq qp";
	ret = __setup_pd_cq_qp(rh);
	if (ret) goto out_err;

	step = "post recv works";
	ret = __setup_recv_works(rh);
	if (ret)  goto out_err;

	step = "setup rdma works";
	ret = __setup_work_request_pools(rh);
	if (ret == 0)  goto out_err;

	step = "accept";
	rh->state = RDMA_CONNECTING;
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	ret = rdma_accept(rh->cm_id, &conn_param);
	if (ret)  goto out_err;

	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (ret)  goto out_err;

	if (connect_type == CONNECTION_FETCH) {
		step = "allocate memory for cpu server";
		rh->vaddr_start = rm_machine_init();
		if (rh->vaddr_start < 0) goto out_err;
	}

	step = "setup dma buffer";
	ret = __setup_dma_buffer(rh);
	if (ret) goto out_err;

	step = "post send";
	ret = __send_dma_addr(rh, rh->rpc_dma_addr, RPC_BUFFER_SIZE);
	if (ret)  goto out_err;

	ret = __send_dma_addr(rh, rh->sink_dma_addr, SINK_BUFFER_SIZE);
	if (ret)  goto out_err;

	wait_for_completion_interruptible(&rh->init_done);
	wait_for_completion_interruptible(&rh->init_done);

	return 0;

out_err:
	printk(KERN_ERR PFX "Failed at %s, %d\n", step, ret);
	return ret;
}

static int accept_client(void *args)
{
	int num_client;
	int ret = 0;

	for (num_client = 0; num_client < MAX_NUM_NODES; num_client++) {
		if (kthread_should_stop())
			return 0;
		if ((ret = __accept_client(num_client, CONNECTION_FETCH))) 
			return ret;
		if ((ret = __accept_client(num_client, CONNECTION_EVICT))) 
			return ret;
	}
	printk(KERN_INFO PFX "Cannot accepts more clients\n");

	return 0;
}



static int __on_client_connecting(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	int connect_type = *(int *)cm_event->param.conn.private_data;
	static atomic_t num_request = ATOMIC_INIT(-1);
	static atomic_t num_request_evic = ATOMIC_INIT(-1);
	int nid = 0;
	struct rdma_handle *rh;

	if (connect_type == CONNECTION_FETCH) {
		DEBUG_LOG(PFX "atomic inc\n");
		nid = atomic_inc_return(&num_request);
		DEBUG_LOG(PFX "nid: %d\n", nid);
		if (nid == MAX_NUM_NODES)
			return 0;
		rh = rdma_handles[nid];
		rh->nid = nid;
	}
	else {
		nid = atomic_inc_return(&num_request_evic);
		if (nid == MAX_NUM_NODES)
			return 0;
		rh = rdma_handles_evic[nid];
		rh->nid = nid;
	}
	cm_id->context = rh;
	rh->cm_id = cm_id;
	rh->device = cm_id->device;
	rh->state = RDMA_ROUTE_RESOLVED;

	DEBUG_LOG(PFX "connecting done\n");
	complete(&rh->cm_done);
	return 0;
}

static int __on_client_connected(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	struct rdma_handle *rh = cm_id->context;
	rh->state = RDMA_CONNECTED;

	printk(PFX "Connected to %d\n", rh->nid);

	complete(&rh->cm_done);

	return 0;
}

static int __on_client_disconnected(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	struct rdma_handle *rh = cm_id->context;
	rh->state = RDMA_CLOSED;

	printk(PFX "Disconnected from %d\n", rh->nid);
	return 0;
}

int cm_server_event_handler(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	int ret = 0;
	switch (cm_event->event) {
		case RDMA_CM_EVENT_CONNECT_REQUEST:
			ret = __on_client_connecting(cm_id, cm_event);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			ret = __on_client_connected(cm_id, cm_event);
			break;
		case RDMA_CM_EVENT_DISCONNECTED:
			ret = __on_client_disconnected(cm_id, cm_event);
			break;
		default:
			printk(PFX "Unhandled server event %d\n", cm_event->event);
			break;
	}
	return 0;
}

static int __listen_to_connection(void)
{
	int ret;
	struct sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(RDMA_PORT),
		.sin_addr.s_addr = my_ip,
	};

	struct rdma_cm_id *cm_id = rdma_create_id(&init_net,
			cm_server_event_handler, NULL, RDMA_PS_IB, IB_QPT_RC);
	if (IS_ERR(cm_id)) 
		return PTR_ERR(cm_id);
	server_rh.cm_id = cm_id;

	ret = rdma_bind_addr(cm_id, (struct sockaddr *)&addr);
	if (ret) {
		printk(KERN_ERR PFX "Cannot bind server address, %d\n", ret);
		return ret;
	}

	ret = rdma_listen(cm_id, MAX_NUM_NODES);
	if (ret) {
		printk(KERN_ERR PFX "Cannot listen to incoming requests, %d\n", ret);
		return ret;
	}

	return 0;
}
static int __establish_connections(void)
{
	int ret;

	DEBUG_LOG(PFX "establish connection\n");
	ret = __listen_to_connection();
	if (ret) 
		return ret;
	accept_k = kthread_create(accept_client, NULL, "accept thread");
	if (!accept_k)
		return -ENOMEM;
	kthread_bind(accept_k, ACC_CPU_ID);
	wake_up_process(accept_k); 

	return 0;
}

/*Function for CPU server*/
#else
static int __establish_connections(void)
{
	int i, ret;

	for (i = 0; i < MAX_NUM_NODES; i++) { 
		DEBUG_LOG(PFX "connect to server\n");
		if ((ret = __connect_to_server(i, CONNECTION_FETCH))) 
			return ret;
		if ((ret = __connect_to_server(i, CONNECTION_EVICT))) 
			return ret;
	}
	printk(PFX "Connections are established.\n");

	return 0;
}
#endif

void __exit exit_rmm_rdma(void)
{
	int i;

	if (polling_k)
		kthread_stop(polling_k);
	if (server && accept_k) {
		for (i = 0; i < MAX_NUM_NODES; i++) {
			complete(&rdma_handles[i]->cm_done);
			complete(&rdma_handles_evic[i]->cm_done);
		}
		kthread_stop(accept_k);
	}
	for (i = 0; i < NR_WORKER_THREAD; i++)
		kthread_stop(w_threads[i].task);

	/* Detach from upper layer to prevent race condition during exit */
	for (i = 0; i < MAX_NUM_NODES; i++) {
		if (rdma_handles[i]->cm_id)
			rdma_disconnect(rdma_handles[i]->cm_id);
		if (rdma_handles_evic[i]->cm_id)
			rdma_disconnect(rdma_handles[i]->cm_id);
	}

	remove_proc_entry("rmm", NULL);
	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh = rdma_handles[i];
		if (!rh) continue;

		if (rh->recv_buffer) {
			ib_dma_unmap_single(rh->device, rh->recv_buffer_dma_addr,
					PAGE_SIZE, DMA_FROM_DEVICE);
			kfree(rh->recv_buffer);
		}

		if (rh->dma_buffer) {
			ib_dma_free_coherent(rh->device, RPC_BUFFER_SIZE + SINK_BUFFER_SIZE, rh->dma_buffer, rh->dma_addr);
		}

		if (rh->qp && !IS_ERR(rh->qp)) rdma_destroy_qp(rh->cm_id);
		//if (rh->cq && !IS_ERR(rh->cq)) ib_destroy_cq(rh->cq);
		if (rh->cm_id && !IS_ERR(rh->cm_id)) rdma_destroy_id(rh->cm_id);
		if (rh->mr && !IS_ERR(rh->mr))
			ib_dereg_mr(rh->mr);

		kfree(rdma_handles[i]);
		kfree(rpc_pools[i]);
		kfree(sink_pools[i]);

		/*free evic */
		rh = rdma_handles_evic[i];
		if (!rh) continue;

		if (rh->recv_buffer) {
			ib_dma_unmap_single(rh->device, rh->recv_buffer_dma_addr,
					PAGE_SIZE, DMA_FROM_DEVICE);
			kfree(rh->recv_buffer);
		}

		if (rh->dma_buffer) {
			ib_dma_free_coherent(rh->device, RPC_BUFFER_SIZE + SINK_BUFFER_SIZE, rh->dma_buffer, rh->dma_addr);
		}

		if (rh->qp && !IS_ERR(rh->qp)) rdma_destroy_qp(rh->cm_id);
		//if (rh->cq && !IS_ERR(rh->cq)) ib_destroy_cq(rh->cq);
		if (rh->cm_id && !IS_ERR(rh->cm_id)) rdma_destroy_id(rh->cm_id);
		if (rh->mr && !IS_ERR(rh->mr))
			ib_dereg_mr(rh->mr);

		kfree(rdma_handles_evic[i]);
		kfree(rpc_pools_evic[i]);
		kfree(sink_pools_evic[i]);
	}
	if (rdma_cq && !IS_ERR(rdma_cq)) ib_destroy_cq(rdma_cq);

	/* MR is set correctly iff rdma buffer and pd are correctly allocated */
	if (rdma_pd) {
		ib_dealloc_pd(rdma_pd);
	}

	printk(KERN_INFO PFX "RDMA unloaded\n");
	return;
}

static void disconnect(void)
{
	int i = 0;

	if (polling_k)
		kthread_stop(polling_k);
	if (server && accept_k) {
		for (i = 0; i < MAX_NUM_NODES; i++) {
			complete(&rdma_handles[i]->cm_done);
			complete(&rdma_handles_evic[i]->cm_done);
		}
		kthread_stop(accept_k);
	}

	if (!server)  {

		for (i = 0; i < MAX_NUM_NODES; i++) {
			if (rdma_handles[i])
				rdma_disconnect(rdma_handles[i]->cm_id);
			if (rdma_handles_evic[i])
				rdma_disconnect(rdma_handles[i]->cm_id);
		}
	}


}

/*
   static int test_maplat(void)
   {
   int ret = 0;
   char *test, *test2;
   char *dummy;
   dma_addr_t dma_test;
   struct scatterlist sg = {};
   struct timespec start_tv, end_tv;
   struct rdma_handle *rh = rdma_handles[0];
   unsigned long elapsed, map_total = 0, cpy_total = 0;

   test = kmalloc(4096, GFP_KERNEL);
   test2 = kmalloc(4096, GFP_KERNEL);
   dummy = kmalloc(4096, GFP_KERNEL);

   getnstimeofday(&start_tv);

   dma_test = ib_dma_map_single(rh->device, test, 4096, DMA_FROM_DEVICE);
   sg_dma_address(&sg) = dma_test; 
   sg_dma_len(&sg) = 4096;
   ret = ib_map_mr_sg(rh->mr, &sg, 1, NULL, 4096);
   if (ret != 1) {
   printk("Cannot map scatterlist to mr, %d\n", ret);
   return -1;
   }
   getnstimeofday(&end_tv);
   elapsed = (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
   (end_tv.tv_nsec - start_tv.tv_nsec);
   map_total += elapsed;

   getnstimeofday(&start_tv);
   memcpy((void *) test2, dummy, 4096);
   getnstimeofday(&end_tv);
   elapsed = (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
   (end_tv.tv_nsec - start_tv.tv_nsec);
   cpy_total += elapsed;

   printk(KERN_INFO PFX "mean map lat: %lu\n", map_total);
   printk(KERN_INFO PFX "mean cpy lat: %lu\n", cpy_total);

   kfree(test);
   kfree(test2);
   kfree(dummy);

   ib_dma_unmap_single(rh->device, dma_test, 4096, DMA_FROM_DEVICE);

   return 0;
   }
 */

static atomic_t num_done = ATOMIC_INIT(0);

struct worker_info {
	int nid;
	int test_size;
	int order;
	int num_op;
	uint16_t *random_index1;
	uint16_t *random_index2;
	struct completion *done;
};

static int tt_worker(void *args)
{
	int i;
	struct worker_info *wi;
	int nid;
	int num_op;
	uint16_t *random_index1, *random_index2;
	int num_page;

	wi = (struct worker_info *) args;
	nid = wi->nid;
	num_op = wi->num_op;
	random_index1 = wi->random_index1;
	random_index2 = wi->random_index2;
	num_page = 1 << wi->order;

	for (i = 0; i < num_op; i++) {
		//rmm_fetch(nid, my_data[nid] + (random_index1[i] * (PAGE_SIZE * num_page)), (void *) (random_index2[i] * (PAGE_SIZE * num_page)), wi->order);
		rmm_fetch(nid % 4, my_data[nid] + (random_index1[i] * (PAGE_SIZE * num_page)), 
				(void *) (random_index2[i] * (PAGE_SIZE * num_page)), wi->order);
	}

	if (atomic_inc_return(&num_done) == wi->test_size) {
		complete(wi->done);
	}

	return 0;
}

static uint64_t elapsed_th[MAX_NUM_NODES];

static void test_throughput(int test_size, int order)
{
	int i, j;
	int num_op = 1000000;
	struct completion job_done;
	struct task_struct *t_arr[16];
	uint16_t index;
	uint16_t *random_index1[16], *random_index2[16];
	struct worker_info wi[16];
	struct timespec start_tv, end_tv;
	uint64_t elapsed;
	int num_page;

	printk(KERN_INFO PFX "tt start %d\n", test_size);

	init_completion(&job_done);
	atomic_set(&num_done, 0);
	for (i = 0; i < test_size; i++) {
		random_index1[i] = kmalloc(num_op * sizeof(uint16_t), GFP_KERNEL);
		if (!random_index1[i])
			goto out_free;

		random_index2[i] = kmalloc(num_op * sizeof(uint16_t), GFP_KERNEL);
		if (!random_index2[i])
			goto out_free;
	}

	num_page = 1 << order;
	if (order >= 9)
		num_op = 5000;

	for (i = 0; i < test_size; i++)
		for (j = 0; j < num_op; j++) {
			get_random_bytes(&index, sizeof(index));
			random_index1[i][j] = index % (1024 / num_page);
			random_index2[i][j] = index % ((1 << 18) / num_page);
		}

	for (i = 0; i < test_size; i++) {
		wi[i].nid = i;
		wi[i].random_index1 = random_index1[i];
		wi[i].random_index2 = random_index2[i];
		wi[i].order = order;
		wi[i].num_op = num_op;
		wi[i].done = &job_done;
		wi[i].test_size = test_size;
	}

	getnstimeofday(&start_tv);
	for (i = 0; i < test_size; i++) {
		t_arr[i] = kthread_run(tt_worker, &wi[i], "woker: %d", i);
	}

	wait_for_completion_interruptible(&job_done);

	getnstimeofday(&end_tv);
	elapsed = (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
		(end_tv.tv_nsec - start_tv.tv_nsec);
	elapsed_th[test_size-1] = elapsed;

	//	printk(KERN_INFO PFX "num op: %d, test size: %d, elapsed(ns) %llu\n", (num_op * test_size), test_size, elapsed);
	if (test_size == MAX_NUM_NODES)
		for (i = 0; i < MAX_NUM_NODES; i++)
			printk(KERN_INFO PFX "test size: %d, elapsed(ns) %llu\n", i + 1, elapsed_th[i]);


out_free:
	for (i = 0; i < test_size; i++) {
		if (random_index1[i])
			kfree(random_index1[i]);
		if (random_index2[i])
			kfree(random_index2[i]);
	}
}

static void test_fetch(int order)
{
	int i;
	int num_page, size;
	struct timespec start_tv, end_tv;
	unsigned long elapsed, total;
	uint16_t index;
	uint16_t *arr1, *arr2;

	arr1 = kmalloc(512 * sizeof(uint16_t), GFP_KERNEL);
	if (!arr1)
		return;

	arr2 = kmalloc(512 * sizeof(uint16_t), GFP_KERNEL);
	if (!arr2) {
		kfree(arr1);
		return;
	}

	num_page = (1 << order);
	size = PAGE_SIZE * num_page;
	for (i = 0; i < 512; i++) {
		get_random_bytes(&index, sizeof(index));
		index %= (1024 / num_page);
		arr1[i] = index;
	}

	for (i = 0; i < 512; i++) {
		get_random_bytes(&index, sizeof(index));
		index %= ((1 << 18) / num_page);
		arr2[i] = index;
	}

	if (server)
		return;

	elapsed = 0;
	total = 0;
	DEBUG_LOG(PFX "fetch start\n");
	getnstimeofday(&start_tv);
	for (i = 0; i < 512; i++) {
		rmm_fetch(0, my_data[0] + (arr1[i] * size), (void *) 0 + (i * size), order);
	}
	getnstimeofday(&end_tv);
	elapsed = (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
		(end_tv.tv_nsec - start_tv.tv_nsec);

	printk(KERN_INFO PFX "average elapsed time %lu (ns)\n", elapsed / 512);

	kfree(arr1);
	kfree(arr2);
}

static void test_evict(void)
{
	int i;
	struct timespec start_tv, end_tv;
	unsigned long elapsed, total;

	if (server)
		return;

	elapsed = 0;
	total = 0;
	DEBUG_LOG(PFX "fetch start\n");
	for (i = 0; i < 500; i++) {
		sprintf(my_data[0] + (i * PAGE_SIZE), "This is %d", i);

	}
	getnstimeofday(&start_tv);
	for (i = 0; i < 500; i++) {
		rmm_evict(0, (void *) (i * PAGE_SIZE), my_data[0] + (i * PAGE_SIZE));
	}
	getnstimeofday(&end_tv);
	elapsed = (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
		(end_tv.tv_nsec - start_tv.tv_nsec);

	printk(KERN_INFO PFX "average elapsed time %lu (ns)\n", total / 500);
}

static ssize_t rmm_write_proc(struct file *file, const char __user *buffer,
		size_t count, loff_t *ppos)
{
	static int num = 1;
	char *cmd, *val;
	int i = 0;
	static int order = 0;

	cmd = kmalloc(count, GFP_KERNEL);
	if (cmd == NULL) {
		printk(KERN_ERR PFX "kmalloc failure\n");
		return -ENOMEM;
	}
	if (copy_from_user(cmd, buffer, count)) {
		kfree(cmd);
		return -EFAULT;
	}
	cmd[count-1] = 0;
	DEBUG_LOG(KERN_INFO PFX "proc write: %s\n", cmd);

	if ((val = strchr(cmd, '='))) {
		val++;
		kstrtoint(val, 10, &i);
		printk(KERN_INFO PFX "order is %d\n", i);
		order = i;
	}

	if (strcmp("diss", cmd) == 0)
		disconnect();
	else if (strcmp("tf", cmd) == 0)
		test_fetch(order);
	else if (strcmp("te", cmd) == 0)
		test_evict();
	else if (strcmp("tt", cmd) == 0)
		if (num <= MAX_NUM_NODES)
			test_throughput(num++, order);

	return count;
}

static struct file_operations rmm_ops = {
	.owner = THIS_MODULE,
	.write = rmm_write_proc,
};

static struct completion done_thread;

static int handle_message(void *args)
{
	struct worker_thread *wt = (struct worker_thread *) args;
	struct args_worker *aw;
	struct rdma_handle *rh;
	enum rpc_opcode op;
	uint16_t id;
	uint16_t offset;
	uint32_t imm_data;

	wt->cpu = smp_processor_id();
	printk(PFX "woker thread created cpu id: %d\n", wt->cpu);
	complete(&done_thread);
	init_completion(&done_worker[wt->cpu-15]);
	wait_for_completion_interruptible(&done_worker[wt->cpu-15]);

	//local_irq_disable();
	preempt_disable();
	while (1) {
		while (!wt->num_queued) {
			if (kthread_should_stop())
				return 0;
			cpu_relax();
		}

		spin_lock(&wt->lock_wt);
		while (!list_empty(&wt->work_head)) {
			aw = dequeue_work(wt);
			wt->num_handled++;
			spin_unlock(&wt->lock_wt);
			imm_data = aw->imm_data;

			rh = aw->rh;
			offset = imm_data >> 16;
			id = imm_data & 0x00007FFF;
			op = (imm_data & 0x00008000) >> 15;

#ifdef CONFIG_RM
			if (rh->connection_type == CONNECTION_FETCH) {
				if (op == 0)
					rpc_handle_fetch_mem(rh, id, offset);
				else 
					rpc_handle_alloc_mem(rh, id, offset);
			}    
			else {
				rpc_handle_evict_mem(rh, imm_data);
			}    
#else
			if (rh->connection_type == CONNECTION_FETCH) {
				if (op == 0)
					rpc_handle_fetch_cpu(rh, id, offset);
				else
					rpc_handle_alloc_cpu(rh, id, offset);
			}
			else {
				rpc_handle_evict_cpu(rh, imm_data);
			}
#endif
			kfree(aw);
			spin_lock(&wt->lock_wt);
		}
		spin_unlock(&wt->lock_wt);
	}
	//local_irq_enable();
	preempt_enable();

	return 0;
}

int create_worker_thread(void)
{
	int i;

	init_completion(&done_thread);

	for (i = 0; i < NR_WORKER_THREAD; i++) {
		struct worker_thread *wt = &w_threads[i];

		wt->num_queued = 0;
		wt->delay = 0;
		wt->num_handled = 0;
		INIT_LIST_HEAD(&wt->work_head);
		spin_lock_init(&wt->lock_wt);
		wt->task = kthread_create(handle_message, wt, "worker thread %d", i);
		if (IS_ERR(wt->task)) {
			printk(KERN_ERR PFX "Cannot create worker thread\n");
			return -EINVAL;
		}
		kthread_bind(wt->task, 15 + i);
		wake_up_process(wt->task); 
		wait_for_completion_interruptible(&done_thread);
	}

	return 0;
}

int __init init_rmm_rdma(void)
{
	int i;

#ifdef CONFIG_RM
	server = 1;
#endif

	printk(PFX "init rmm rdma\n");
	rmm_proc = proc_create("rmm", 0666, NULL, &rmm_ops);
	if (rmm_proc == NULL) {
		printk(KERN_ERR PFX "cannot create /proc/rmm\n");
		return -ENOMEM;
	}

	if (!identify_myself(&my_ip)) 
		return -EINVAL;

	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh;
		rh = rdma_handles[i] = kzalloc(sizeof(struct rdma_handle), GFP_KERNEL);
		if (!rh) 
			goto out_free;
		if (!(rpc_pools[i] = kzalloc(sizeof(struct pool_info), GFP_KERNEL)))
			goto out_free;
		if (!(sink_pools[i] = kzalloc(sizeof(struct pool_info), GFP_KERNEL)))
			goto out_free;

		rh->nid = i;
		rh->state = RDMA_INIT;
		rh->connection_type = CONNECTION_FETCH;

		spin_lock_init(&rh->rdma_work_head_lock);
		spin_lock_init(&rh->rpc_slots_lock);
		spin_lock_init(&rh->sink_slots_lock);

		init_completion(&rh->cm_done);
		init_completion(&rh->init_done);
	}
	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh;
		rh = rdma_handles_evic[i] = kzalloc(sizeof(struct rdma_handle), GFP_KERNEL);
		if (!rh) 
			goto out_free;
		if (!(rpc_pools_evic[i] = kzalloc(sizeof(struct pool_info), GFP_KERNEL)))
			goto out_free;
		if (!(sink_pools_evic[i] = kzalloc(sizeof(struct pool_info), GFP_KERNEL)))
			goto out_free;

		rh->nid = i;
		rh->state = RDMA_INIT;
		rh->connection_type = CONNECTION_EVICT;

		spin_lock_init(&rh->rdma_work_head_lock);
		spin_lock_init(&rh->rpc_slots_lock);
		spin_lock_init(&rh->sink_slots_lock);
		//spin_lock_init(&rh->evict_slots_lock);

		init_completion(&rh->cm_done);
		init_completion(&rh->init_done);
	}
	server_rh.state = RDMA_INIT;
	init_completion(&server_rh.cm_done);

	for (i = 0; i < MAX_NUM_NODES; i++) {
		my_data[i] = kmalloc(PAGE_SIZE * 1024, GFP_KERNEL);
		if (!my_data[i])
			goto out_free;
	}

	create_worker_thread();

	if (__establish_connections())
		goto out_free;

	if (!server) {
		printk(PFX "remote memory alloc\n");
		for (i = 0; i < MAX_NUM_NODES; i++) {
			rmm_alloc(i, 0);
		}
	}

	printk(PFX "Ready on InfiniBand RDMA\n");
	return 0;

out_free:
	exit_rmm_rdma();
	return -EINVAL;
}

module_init(init_rmm_rdma);
module_exit(exit_rmm_rdma);
MODULE_AUTHOR("Yoonjae Jeon, Sanghoon Kim");
MODULE_DESCRIPTION("RDMA Interface for Pagefault handler");
MODULE_LICENSE("GPL");
