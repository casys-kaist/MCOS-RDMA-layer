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

#include "common.h"
#include "ring_buffer.h"
#include "main.h"
#include "mcos.h"
#include "rpc.h"

#define NUM_QPS (MAX_NUM_NODES * 2)

int debug = 0;
int my_nid = 0;
module_param(debug, int, 0);
MODULE_PARM_DESC(debug, "Debug level (0=none, 1=all)");

static int server = 0;

const struct connection_config c_config[ARRAY_SIZE(ip_addresses)] = {
	{4, MEM_GID, BACKUP_SYNC},
	{-1, -1, -1}, /* end */
};

struct connection_info c_infos[MAX_NUM_GROUPS];
static int num_groups = 0;

static struct completion done_worker[NR_WORKER_THREAD];
static struct worker_thread w_threads[NR_WORKER_THREAD];
static atomic_t HEAD = ATOMIC_INIT(0); 

/* RDMA handle for each node */
static struct rdma_handle server_rh;
struct rdma_handle *rdma_handles[NUM_QPS] = { NULL };
static struct pool_info *rpc_pools[NUM_QPS] = { NULL };
static struct pool_info *sink_pools[NUM_QPS] = { NULL };

static u64 vaddr_start_arr[MAX_NUM_NODES] = { 0 };

/* Global protection domain (pd) and memory region (mr) */
static struct ib_pd *rdma_pd = NULL;
/*Global CQ */
static struct ib_cq *rdma_cq = NULL;

static struct task_struct *polling_k = NULL;

/*proc file for control */
static struct proc_dir_entry *rmm_proc;

/*Variables for server */
static uint32_t my_ip;

/* RPC hadnler table */
Rpc_handler rpc_table[NUM_RPC];

/*buffer for test */
char *my_data[MAX_NUM_NODES] = { NULL };

unsigned long delta[512], delta_delay[512];
unsigned long accum = 0, accum_delay = 0;
//int head = 0, head_delay = 0;
//
static int __send_dma_addr(struct rdma_handle *rh, dma_addr_t addr, size_t size);
static int __setup_recv_works(struct rdma_handle *rh);
static int start_connection(void);



static inline unsigned long long get_ns(void)
{
	//return (cpu_clock(0) * (1000000L / NSEC_PER_SEC));
	//return cpu_clock(0);
	return sched_clock();
}

static inline void rmm_yield_cpu(void)
{
	cond_resched();
}

static inline struct worker_thread *get_worker_thread(void)
{
	struct worker_thread *wt;
	int h;

	h = atomic_fetch_add(1, &HEAD);
	wt = &w_threads[h % NR_WORKER_THREAD];

	return wt;
}

static void enqueue_work(struct rdma_handle *rh, uint32_t imm_data)
{
	struct worker_thread *wt;
	struct args_worker *aw;

	wt = get_worker_thread();
	spin_lock(&wt->lock_wt);

retry:
	aw = kmalloc(sizeof(struct args_worker), GFP_ATOMIC);
	if (!aw) {
		printk(KERN_ERR PFX "fail to allocate memory in %s\n", __func__);
		goto retry;
	}

	aw->rh = rh;
	aw->imm_data = imm_data;

	aw->time_dequeue_ns = 0;
	aw->time_enqueue_ns = get_ns();

	INIT_LIST_HEAD(&aw->next);
	list_add_tail(&aw->next, &wt->work_head);
	wt->num_queued++;
	wt->total_queued++;
	spin_unlock(&wt->lock_wt);
}

/* Must hold a lock before call this function */
static struct args_worker *dequeue_work(struct worker_thread *wt)
{
	struct args_worker *aw;

	aw  = list_first_entry(&wt->work_head, struct args_worker, next);
	aw->time_dequeue_ns = get_ns();
	list_del(&aw->next);
	wt->num_queued--;

	return aw;
}


/*rpc handle function for mem server */


static int rpc_handle_alloc_free_done(struct rdma_handle *rh, uint32_t offset)
{
	void *rpc_addr;
	struct rpc_header *rhp;
	int nid, ret, op;
	int *done;
	unsigned long *rpage_flags;

	nid = rh->nid;
	rhp = rh->rpc_buffer + offset;
	rpc_addr = (uint8_t *) rh->rpc_buffer + (offset + sizeof(struct rpc_header));

	if (rhp->async) {
		ret = *((int *) (rpc_addr + 4));
		rpage_flags = *((unsigned long **) (rpc_addr + 8));
		op = rhp->op;

		if (ret) {
			if (op == RPC_OP_ALLOC) {
				DEBUG_LOG(PFX "asycn alloc is done %s \n", __func__);
				*rpage_flags |= RPAGE_ALLOCED;
			}
			else {
				DEBUG_LOG(PFX "asycn free is done %s \n", __func__);
				*rpage_flags |= RPAGE_FREED;
			}
		}
		else {
			if (op == RPC_OP_ALLOC) {
				DEBUG_LOG(PFX "asycn alloc is failed %s \n", __func__);
				*rpage_flags |= RPAGE_ALLOC_FAILED;
			}
			else {
				DEBUG_LOG(PFX "asycn free is failed %s \n", __func__);
				*rpage_flags |= RPAGE_FREE_FAILED;
			}
		}
		ring_buffer_put(rh->rb, rh->rpc_buffer + offset);
	}
	else {
		done = (int *) (rpc_addr + 8); 
		*done = 1;
	}

	return 0;
}

static int rpc_handle_evict_done(struct rdma_handle *rh, uint32_t offset)
{
	struct rpc_header *rhp;
	uint8_t *buffer = rh->dma_buffer + offset;
	unsigned nr_pages = *(int *) (buffer + sizeof(struct rpc_header));
	int *done;
	int **done_p;

	rhp = (struct rpc_header *) buffer;
	if (!rhp->async) {
		done = (int *) (buffer + sizeof(struct rpc_header) + 4 + (nr_pages * (PAGE_SIZE + 8)));
	}
	else {
		done_p = (int **) (buffer + sizeof(struct rpc_header) + 4 + (nr_pages * (PAGE_SIZE + 8)));
		done = *done_p;
		ring_buffer_put(rh->rb, buffer);
	}
	*done = 1;

	return 0;
}

static int __handle_rpc(struct ib_wc *wc)
{
	int ret = 0;
	struct recv_work *rw;
	struct rdma_handle *rh;
	struct rpc_header *rhp;
	uint32_t imm_data;
	int op;
	const struct ib_recv_wr *bad_wr = NULL;
	int header;
	int processed = 0;

	rw = (struct recv_work *) wc->wr_id;
	rh = rw->rh;
	imm_data = be32_to_cpu(wc->ex.imm_data);
	rhp = rh->rpc_buffer + imm_data;

	//	DEBUG_LOG(PFX "%08X\n", imm_data);

	if (rh->qp_type == QP_EVICT) {
		header = *(int *) (rh->evict_buffer + (imm_data + sizeof(struct rpc_header) + 4));
		DEBUG_LOG(PFX "header %d in %s\n", header, __func__);
		if (header == -1) {
			rpc_handle_evict_done(rh, imm_data);
			processed = 1;
		}
	}

	if (rh->qp_type == QP_FETCH) {
		op = rhp->op;
		header =  *(int *) (rh->rpc_buffer + (imm_data + sizeof(struct rpc_header)));
		if ((op == RPC_OP_ALLOC || op == RPC_OP_FREE) && header == -1) {
			rpc_handle_alloc_free_done(rh, imm_data);
			processed = 1;
		}
	}

	if (!processed) {
		DEBUG_LOG(PFX "enqueue work in %s\n", __func__);
		enqueue_work(rh, imm_data);
	}

	ret = ib_post_recv(rh->qp, &rw->wr, &bad_wr);
	if (ret || bad_wr)  {
		if (bad_wr)
			ret = -EINVAL;
		printk(KERN_ERR PFX "fail to post recv in %s\n", __func__);
	}

	return 0;
}

static int __handle_recv(struct ib_wc *wc)
{
	int ret = 0;
	struct pool_info *rp;
	struct recv_work_addr *rw;
	struct rdma_handle *rh;

	rw = (struct recv_work_addr *) wc->wr_id;
	rh = rw->rh;
	switch (rw->work_type) {
		case WORK_TYPE_RPC_ADDR:
			ib_dma_unmap_single(rh->device, rw->dma_addr, 
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			if (rh->qp_type == QP_FETCH)
				rp = rpc_pools[rh->nid*2];
			else 
				rp = rpc_pools[(rh->nid*2)+1];
			rh->remote_rpc_dma_addr = rp->addr;
			rh->remote_dma_addr = rp->addr;
			rh->remote_rpc_size = rp->size;
			rh->rpc_rkey = rp->rkey;
			kfree(rw);
			DEBUG_LOG(PFX "recv rpc addr %llx %x\n", rh->remote_rpc_dma_addr, rh->rpc_rkey);
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

static void handle_error(struct ib_wc *wc)
{
	printk(KERN_ALERT PFX "cq completion failed with "
			"wr_id %Lx status %d opcode %d vendor_err %x\n",
			wc->wr_id, wc->status, wc->opcode, wc->vendor_err);
}

static int polling_cq(void * args)
{
	struct ib_cq *cq = (struct ib_cq *) args;
	struct ib_wc *wc;
	//	struct rdma_work *rw;
	struct rdma_handle *rh;
	int i;

	int ret = 0;
	int num_poll = 1;
	//	u64 start = get_jiffies_64();

	DEBUG_LOG(PFX "Polling thread now running\n");
	wc = kmalloc(sizeof(struct ib_wc) * num_poll, GFP_KERNEL);
	if (!wc) {
		printk(KERN_ERR "failed to allocate memory in %s\n", __func__);
		return -ENOMEM;
	}

	while ((ret = ib_poll_cq(cq, num_poll, wc)) >= 0) {
		if (kthread_should_stop())
			goto free;

		for (i = 0; i < ret; i++) {
			if (wc[i].status) {
				if (wc[i].status == IB_WC_WR_FLUSH_ERR) {
					DEBUG_LOG("cq flushed\n");
					continue;
				} 
				else {
					handle_error(&wc[i]);
					goto error;
				}
			}

			switch (wc[i].opcode) {
				case IB_WC_SEND:
					DEBUG_LOG(PFX "send completion\n");
					__handle_send(&wc[i]);
					break;

				case IB_WC_RDMA_WRITE:
					if (wc[i].wc_flags == IB_WC_WITH_IMM) {
						DEBUG_LOG(PFX "rdma write-imm completion\n");
						/*
						   rw = (struct rdma_work *) wc[i].wr_id;
						   put_work(&wc[i]);
						   */
					}
					else {
						DEBUG_LOG(PFX "rdma write completion\n");
					}
					break;

				case IB_WC_RDMA_READ:
					rh = (struct rdma_handle *) wc[i].wr_id; 
					complete(&rh->init_done);
					DEBUG_LOG(PFX "rdma read completion\n");
					break;

				case IB_WC_RECV:
					DEBUG_LOG(PFX "recv completion\n");
					__handle_recv(&wc[i]);
					break;
				case IB_WC_RECV_RDMA_WITH_IMM:
					DEBUG_LOG(PFX "recv rpc\n");
					__handle_rpc(&wc[i]);
					break;

				case IB_WC_REG_MR:
					rh = (struct rdma_handle *) wc[i].wr_id; 
					complete(&rh->init_done);
					DEBUG_LOG(PFX "mr registered\n");
					break;

				default:
					printk(KERN_ERR PFX
							"%s:%d Unexpected opcode %d\n",
							__func__, __LINE__, wc[i].opcode);
					goto error;
			}
		}

		/*if (get_jiffies_64() - start >= MAX_TICKS) {
		  rmm_yield_cpu();
		  start = get_jiffies_64();
		  }*/
		rmm_yield_cpu();

	}

free:
	kfree(wc);
	return 0;
error:
	kfree(wc);
	return -1;
}

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
			.cqe = (MAX_SEND_DEPTH + MAX_RECV_DEPTH + NR_RDMA_SLOTS) * MAX_NUM_NODES,
			.comp_vector = 0,
		};

		if (cq_attr.cqe > rh->device->attrs.max_cqe) {
			printk(PFX "cqe exceeds max_cqe\n");
			cq_attr.cqe = rh->device->attrs.max_cqe - 1;
		}

		printk(PFX "call create cq, cqe %d\n", cq_attr.cqe);
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
		kthread_bind(polling_k, POLL_CPU_ID);
		wake_up_process(polling_k); 
		for (i = 0; i < NR_WORKER_THREAD; i++)
			complete(&done_worker[i]);
	}
	rh->cq = rdma_cq;


	/* create queue pair */
	if (!rh->qp) {
		/*FIXME: max(max_~~_wr, dev_cap.max) */
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

		if (qp_attr.cap.max_send_wr > rh->device->attrs.max_qp_wr) {
			printk(PFX "max_send_wr exceeds max_qp_wr\n");
			qp_attr.cap.max_send_wr = rh->device->attrs.max_qp_wr - 1;
		}

		if (qp_attr.cap.max_recv_wr > rh->device->attrs.max_qp_wr) {
			printk(PFX "max_send_wr exceeds max_qp_wr\n");
			qp_attr.cap.max_recv_wr = rh->device->attrs.max_qp_wr - 1;
		}

		ret = rdma_create_qp(rh->cm_id, rdma_pd, &qp_attr);
		if (ret) 
			goto out_err;
		rh->qp = rh->cm_id->qp;
		printk(PFX "qp created\n");
	}


	return 0;

out_err:
	return ret;
}

/* Alloc dma buffer */
static  int __setup_dma_buffer(struct rdma_handle *rh)
{
	int ret = 0;
	dma_addr_t dma_addr = 0;
	size_t buffer_size = DMA_BUFFER_SIZE;
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
	struct ib_mr *mr = NULL;
	char *step = NULL;
	int rh_id = 0;
	resource_size_t paddr;

	step = "alloc dma buffer";
	rh_id = nid_to_rh(rh->nid);
	if (rh->qp_type == QP_EVICT)
		rh_id++;
	DEBUG_LOG(PFX "%s with rh id: %d\n", step, rh_id);
	if (!rh->dma_buffer) {
		paddr = (resource_size_t) (((uint8_t *) DMA_BUFFER_START) + (rh_id * buffer_size));

		if (!(rh->dma_buffer = memremap(paddr, buffer_size, MEMREMAP_WB))) {
			printk(KERN_ERR PFX "memremap error in %s\n", __func__);
			return -ENOMEM;
		}
		//flush_tlb_all();
		memset(rh->dma_buffer, 0, buffer_size);
		dma_addr = paddr;
	}

	step = "alloc mr";
	DEBUG_LOG(PFX "%s\n", step);
	if (!rh->mr) {
		mr = ib_alloc_mr(rdma_pd, IB_MR_TYPE_MEM_REG, 2);
		if (IS_ERR(mr)) {
			ret = PTR_ERR(mr);
			goto out_free;
		}
	}

	sg_dma_address(&sg) = dma_addr;
	sg_dma_len(&sg) = buffer_size;

	step = "map mr";
	DEBUG_LOG(PFX "%s\n", step);
	ret = ib_map_mr_sg(mr, &sg, 1, NULL, buffer_size);
	if (ret != 1) {
		printk(PFX "Cannot map scatterlist to mr, %d\n", ret);
		goto out_dereg;
	}
	reg_wr.mr = mr;
	reg_wr.key = mr->rkey;

	step = "post reg mr";
	DEBUG_LOG(PFX "%s\n", step);
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

	rh->evict_buffer = rh->dma_buffer;
	rh->evict_dma_addr = rh->dma_addr;

	rh->rb = ring_buffer_create(rh->rpc_buffer, rh->rpc_dma_addr, DMA_BUFFER_SIZE,"dma buffer"); 

	return 0;
out_dereg:
	ib_dereg_mr(mr);

out_free:
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
	struct recv_work_addr *rw;

	int index = (rh->nid) * 2;

	if (work_type == WORK_TYPE_RPC_ADDR) {
		if (rh->qp_type == QP_FETCH)
			pp = rpc_pools[index];
		else 
			pp = rpc_pools[index+1];
	}
	else {
		if (rh->qp_type == QP_FETCH)
			pp = sink_pools[index];
		else 
			pp = sink_pools[index+1];
	}

	rw = kmalloc(sizeof(struct recv_work_addr), GFP_KERNEL);
	if (!rw)
		return -ENOMEM;

	/*Post receive request for recieve request pool info*/
	dma_addr = ib_dma_map_single(rh->device, pp, sizeof(struct pool_info), DMA_TO_DEVICE);
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


	/*FIXME */
	if (!rh->rdma_work_pool) {
		rh->rdma_work_pool = kzalloc(sizeof(struct rdma_work) * nr_works, GFP_KERNEL);
		if (!rh->rdma_work_pool)
			return -ENOMEM;
	}

	for (i = 0; i < nr_works; i++) {
		struct rdma_work *rw = &rh->rdma_work_pool[i];

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


static int __setup_work_request_pools(struct rdma_handle *rh)
{
	int ret;
	/* Initalize rdma work request pool */
	ret = __refill_rdma_work(rh, NR_RDMA_SLOTS);
	return ret;

}

static int __setup_recv_works(struct rdma_handle *rh)
{
	int ret = 0, i;
	struct recv_work *rws = NULL;

	/* prevent to duplicated allocation */
	if (!rh->recv_works) {
		rws = kmalloc(sizeof(*rws) * (MAX_RECV_DEPTH), GFP_KERNEL);
		if (!rws) {
			return -ENOMEM;
		}
	}
	else {
	}

	DEBUG_LOG(PFX "post recv for exchanging dma addr\n");
	ret = __setup_recv_addr(rh, WORK_TYPE_RPC_ADDR);
	if (ret)
		return ret;

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
int cm_client_event_handler(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	struct rdma_handle *rh = cm_id->context;

	switch (cm_event->event) {
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			printk(PFX "addr resolved\n");
			rh->state = RDMA_ADDR_RESOLVED;
			complete(&rh->cm_done);
			break;
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
			printk(PFX "route resolved\n");
			rh->state = RDMA_ROUTE_RESOLVED;
			complete(&rh->cm_done);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			printk(PFX "connection established\n");
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

static int __connect_to_server(int nid, int qp_type, enum connection_type c_type)
{
	struct conn_private_data private_data;
	const char *step;
	struct rdma_handle *rh;

	int ret = 0;
	int index;

	index = nid_to_rh(nid);
	if (qp_type == QP_EVICT)
		index++;

	rh = rdma_handles[index];

	rh->nid = nid;
	rh->qp_type = qp_type;
	rh->c_type = c_type;

	DEBUG_LOG(PFX "nid: %d\n", nid);

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
	private_data.qp_type = qp_type;
	private_data.c_type = c_type;
	private_data.nid = my_nid;
	DEBUG_LOG(PFX "%s\n", step);
	{
		struct rdma_conn_param conn_param = {
			.private_data = &private_data,
			.private_data_len = sizeof(private_data),
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
	ret = __send_dma_addr(rh, rh->rpc_dma_addr, DMA_BUFFER_SIZE);
	if (ret)
		goto out_err;

	wait_for_completion_interruptible(&rh->init_done);

	printk(PFX "Connected to %d\n", nid);
	return 0;

out_err:
	printk(KERN_ERR PFX "Unable to %s, %pI4, %d\n", step, ip_table + nid, ret);
	return ret;
}

int connect_to_servers(void)
{
	int i, j, k, ret, nid;
	enum connection_type ctype;
	struct node_info infos;

	for (i = 0; i < num_groups; i++) { 
		for (j = 0; j < NUM_CTYPE; j++) {
			infos = c_infos[i].infos[j];
			ctype = j;
			for (k = 0; k < infos.size; k++) {
				nid = infos.nids[k];
				DEBUG_LOG(PFX "connect to server\n");
				if ((ret = __connect_to_server(nid, QP_FETCH, ctype))) 
					return ret;
				if ((ret = __connect_to_server(nid, QP_EVICT, ctype))) 
					return ret;
			}
		}
	}

	return 0;
}

/****************************************************************************
 * Server-side connection handling
 */


/*Function for Mem server */
#ifdef CONFIG_RM
static int __accept_client(struct rdma_handle *rh)
{
	struct rdma_conn_param conn_param = {0};
	char *step = NULL;
	int ret;

	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (rh->state != RDMA_ROUTE_RESOLVED) return -EINVAL;

	step = "setup pd cq qp";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __setup_pd_cq_qp(rh);
	if (ret) goto out_err;

	step = "post recv works";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __setup_recv_works(rh);
	if (ret)  goto out_err;

	step = "setup rdma works";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __setup_work_request_pools(rh);
	if (ret == 0)  goto out_err;

	step = "accept";
	DEBUG_LOG(PFX "%s\n", step);
	rh->state = RDMA_CONNECTING;
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	ret = rdma_accept(rh->cm_id, &conn_param);
	if (ret)  goto out_err;

	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (ret)  goto out_err;

	step = "allocate memory for cpu server";
	DEBUG_LOG(PFX "%s\n", step);
	if (rh->c_type == PRIMARY)
		rh->vaddr_start = vaddr_start_arr[rh->nid];
	if (rh->vaddr_start < 0) goto out_err;

	step = "setup dma buffer";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __setup_dma_buffer(rh);
	if (ret) goto out_err;

	step = "post send";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __send_dma_addr(rh, rh->rpc_dma_addr, DMA_BUFFER_SIZE);
	if (ret)  goto out_err;

	wait_for_completion_interruptible(&rh->init_done);

	return 0;

out_err:
	printk(KERN_ERR PFX "Failed at %s, %d\n", step, ret);
	return ret;
}

static int accept_client(void *args)
{
	int ret = 0;

	printk(KERN_INFO PFX "accept thread running\n");
	if ((ret = __accept_client(args))) 
		return ret;

	return 0;
}



static int __on_client_connecting(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	int index;
	struct rdma_handle *rh;
	struct task_struct *accept_k = NULL;
	struct conn_private_data private = 
		*(struct conn_private_data *) cm_event->param.conn.private_data;

	unsigned nid = private.nid;
	unsigned qp_type = private.qp_type;
	unsigned c_type = private.c_type;

	DEBUG_LOG(PFX "nid: %d, connection type %d, %s\n", nid, qp_type, __func__);
	if (nid == MAX_NUM_NODES)
		return 0;

	index = nid_to_rh(nid);
	if (qp_type == QP_EVICT)
		index++;

	rh = rdma_handles[index];
	rh->nid = nid;
	cm_id->context = rh;
	rh->cm_id = cm_id;
	rh->device = cm_id->device;
	rh->qp_type = qp_type;
	rh->c_type = c_type;
	rh->state = RDMA_ROUTE_RESOLVED;

	complete(&rh->cm_done);

	if (qp_type == QP_FETCH)
		basic_memory_init(nid);

	accept_k = kthread_create(accept_client, rh, "accept thread");
	if (!accept_k)
		return -ENOMEM;
	wake_up_process(accept_k); 

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

	ret = rdma_listen(cm_id, NUM_QPS);
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
	connect_to_servers();
	ret = __listen_to_connection();
	if (ret) 
		return ret;

	return 0;
}

/*Function for CPU server*/
#else
static int __establish_connections(void)
{
	int i, ret;

	connect_to_servers();
	printk(PFX "Connections are established.\n");


	return 0;
}
#endif

void __exit exit_rmm_rdma(void)
{
	int i;

	printk(PFX "statistics\n avg: %lu avg_delay: %lu\n", 
			accum, accum_delay);

	if (polling_k)
		kthread_stop(polling_k);

	for (i = 0; i < NR_WORKER_THREAD; i++)
		kthread_stop(w_threads[i].task);

	/* Detach from upper layer to prevent race condition during exit */
	for (i = 0; i < NUM_QPS; i++) {
		if (rdma_handles[i]->cm_id)
			rdma_disconnect(rdma_handles[i]->cm_id);
	}
	remove_proc_entry("rmm", NULL);

	for (i = 0; i < NUM_QPS; i++) {
		struct rdma_handle *rh = rdma_handles[i];
		if (!rh) continue;

		if (rh->recv_buffer) {
			ib_dma_unmap_single(rh->device, rh->recv_buffer_dma_addr,
					PAGE_SIZE, DMA_FROM_DEVICE);
			kfree(rh->recv_buffer);
		}

		/*
		   if (rh->dma_buffer) {
		   ib_dma_unmap_single(rh->device, rh->dma_addr, 
		   DMA_BUFFER_SIZE, DMA_BIDIRECTIONAL);
		   }
		   */

		if (rh->qp && !IS_ERR(rh->qp)) rdma_destroy_qp(rh->cm_id);
		if (rh->cm_id && !IS_ERR(rh->cm_id)) rdma_destroy_id(rh->cm_id);
		if (rh->mr && !IS_ERR(rh->mr))
			ib_dereg_mr(rh->mr);

		kfree(rdma_handles[i]);
		kfree(rpc_pools[i]);
		kfree(sink_pools[i]);
	}

	if (rdma_cq && !IS_ERR(rdma_cq)) ib_destroy_cq(rdma_cq);

	/* MR is set correctly iff rdma buffer and pd are correctly allocated */
	if (rdma_pd) {
		ib_dealloc_pd(rdma_pd);
	}

	printk(KERN_INFO PFX "RDMA unloaded\n");
	return;
}


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

static int dummy_thread(void *args)
{
	int i;
	int nid = (int) args;


	i = 0;
	while (1)  {
		if (kthread_should_stop())
			return 0;
		rmm_fetch(nid, my_data[nid] + (i * (PAGE_SIZE)), (void *) (i * (PAGE_SIZE)), 0);
		i = (i + 1) % 1024;
		rmm_yield_cpu();
	}

	return 0;
}

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
	unsigned long elapsed;
	uint16_t index;
	uint16_t *arr1, *arr2;
	//unsigned long flags;

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
	DEBUG_LOG(PFX "fetch start\n");
	getnstimeofday(&start_tv);
	for (i = 0; i < 512; i++) {
		rmm_fetch(0, my_data[0] + (i * size), (void *) (i * size), order);
		DEBUG_LOG(PFX "fetched data %s\n", my_data[0] + (i * size));
	}
	getnstimeofday(&end_tv);
	elapsed = (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
		(end_tv.tv_nsec - start_tv.tv_nsec);

	printk(KERN_INFO PFX "total elapsed time %lu (ns)\n", elapsed);
	printk(KERN_INFO PFX "average elapsed time %lu (ns)\n", elapsed / 512);
	//	printk(KERN_INFO PFX "average elapsed time for preparing fetch %lu (ns)\n", 
	//			elapsed_fetch / 512);

	kfree(arr1);
	kfree(arr2);
}

static int test_evict(void)
{
	int i, j, num;
	uint16_t index;
	struct timespec start_tv, end_tv;
	unsigned long elapsed, total;
	struct evict_info *ei;
	struct list_head *pos, *n;
	LIST_HEAD(addr_list);

	if (server)
		return 0;

	elapsed = 0;
	total = 0;
	DEBUG_LOG(PFX "evict start\n");
	for (i = 0; i < 1024; i++) {
		sprintf(my_data[0] + (i * PAGE_SIZE), "This is %d", i);

	}

	num = 100;
	for (i = 0; i < num; i++) {
		INIT_LIST_HEAD(&addr_list);
		printk(PFX "evict %d\n", i);

		for (j = 0; j < 64; j++) {
			ei = kmalloc(sizeof(struct evict_info), GFP_KERNEL);
			if (!ei) {
				printk("error in %s\n", __func__);
				return -ENOMEM;
			}

			get_random_bytes(&index, sizeof(index));
			index %= 1024;

			ei->l_vaddr = (uint64_t) (my_data[0] + index * (PAGE_SIZE));
			ei->r_vaddr = (uint64_t) (index * PAGE_SIZE);
			INIT_LIST_HEAD(&ei->next);
			list_add(&ei->next, &addr_list);
		}

		getnstimeofday(&start_tv);
		rmm_evict(0, &addr_list, 64);
		getnstimeofday(&end_tv);
		elapsed += (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
			(end_tv.tv_nsec - start_tv.tv_nsec);

		list_for_each_safe(pos, n, &addr_list) {
			ei = list_entry(pos, struct evict_info, next);
			kfree(ei);
		}
	}
	printk(KERN_INFO PFX "average elapsed time(evict) %lu (ns)\n", elapsed / num);


	return 0;
}

static ssize_t rmm_write_proc(struct file *file, const char __user *buffer,
		size_t count, loff_t *ppos)
{
	static int num = 1;
	char *cmd, *val;
	int i = 0;
	static int head = 0;
	static int order = 0;
	static struct task_struct *t_arr[20];

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

	if (strcmp("pd", cmd) == 0)
		;
	else if (strcmp("bd", cmd) == 0)
		;
	else if (strcmp("tf", cmd) == 0)
		test_fetch(order);
	else if (strcmp("te", cmd) == 0)
		test_evict();
	else if (strcmp("tt", cmd) == 0) {
		if (num <= MAX_NUM_NODES)
			test_throughput(num++, order);
	}
	else if (strcmp("start", cmd) == 0)
		start_connection();
	else if (strcmp("make dummy", cmd) == 0)
		t_arr[head++] = kthread_run(dummy_thread, 0, "dummy thread %d", head);
	else if (strcmp("stop dummy", cmd) == 0) {
		for (i = 0; i < head; i++)  {
			kthread_stop(t_arr[i]);
		}
		head = 0;
	}

	return count;
}

static struct file_operations rmm_ops = {
	.owner = THIS_MODULE,
	.write = rmm_write_proc,
};

static struct completion done_thread;

static int handle_message(void *args)
{
	enum rpc_opcode op;
	uint32_t offset;
	uint32_t imm_data;

	struct args_worker *aw;
	struct rdma_handle *rh;
	struct rpc_header *rhp;
	struct worker_thread *wt = (struct worker_thread *) args;

	//	u64 start = get_jiffies_64();

	wt->cpu = smp_processor_id();
	printk(PFX "woker thread created cpu id: %d\n", wt->cpu);
	complete(&done_thread);
	init_completion(&done_worker[wt->cpu-WORKER_CPU_ID]);
	wait_for_completion_interruptible(&done_worker[wt->cpu-WORKER_CPU_ID]);
	printk(PFX "woker thread run at cpu %d\n", wt->cpu);

	while (1) {
		while (!wt->num_queued) {
			if (kthread_should_stop())
				return 0;
			/*if (get_jiffies_64() - start >= MAX_TICKS) {
			  rmm_yield_cpu();
			  start = get_jiffies_64();
			  }*/
			rmm_yield_cpu();
			cpu_relax();
		}

		spin_lock(&wt->lock_wt);
		while (!list_empty(&wt->work_head)) {
			aw = dequeue_work(wt);
			wt->num_handled++;
			spin_unlock(&wt->lock_wt);
			imm_data = aw->imm_data;

			rh = aw->rh;
			rhp = rh->rpc_buffer + imm_data;
			offset = imm_data;
			op = rhp->op;

			(*rpc_table[op])(rh, offset);

			kfree(aw);
			spin_lock(&wt->lock_wt);
		}
		spin_unlock(&wt->lock_wt);
	}

	return 0;
}

static int create_worker_thread(void)
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
		kthread_bind(wt->task, WORKER_CPU_ID + i);
		wake_up_process(wt->task); 
		wait_for_completion_interruptible(&done_thread);
	}

	return 0;
}

#ifdef RMM_TEST
void init_for_test(void)
{
	int i;

	for (i = 0; i < MAX_NUM_NODES; i++) {
		my_data[i] = kmalloc(PAGE_SIZE * 1024, GFP_KERNEL);
		if (!my_data[i])
			return -1;
	}

	/*
	   if (!server) {
	   printk(PFX "remote memory alloc\n");
	   for (i = 0; i < 1; i++) {
	   mcos_rmm_alloc(0, FAKE_PA_START + i * 4096);
	   }
	   }
	   else {
	   for (i = 0; i < 1024; i++) {
	   sprintf(my_data[0] + (i * PAGE_SIZE), "Data in server: %d", i);
	   printk(PFX "%s\n", my_data[0] + (i * PAGE_SIZE));
	   }
	   }
	   */
}
#else
void init_for_test(void)
{
}
#endif

static int start_connection(void)
{

	return 0;
}

static void init_connection_infos(void)
{
	int i;
	struct node_info *infos;

	for (i = 0; c_config[i].nid >= 0; i++) {
		infos = get_node_infos(c_config[i].gid, c_config[i].ctype);
		infos->nids[infos->size] = c_config[i].nid;
		infos->size++;
	}
}


int __init init_rmm_rdma(void)
{
	int i;
	//	unsigned long flags;

#ifdef CONFIG_RM
	server = 1;
	/* allocate memory for cpu servers */
	for (i = 0; i < MAX_NUM_NODES; i++)
		vaddr_start_arr[i] = rm_machine_init();
#endif /*end for CONFIG_RM */


	rmm_proc = proc_create("rmm", 0666, NULL, &rmm_ops);
	if (rmm_proc == NULL) {
		printk(KERN_ERR PFX "cannot create /proc/rmm\n");
		return -ENOMEM;
	}

	printk(PFX "init rmm rdma\n");

	if (!identify_myself(&my_ip, &my_nid)) 
		return -EINVAL;

	regist_handler(rpc_table);

	printk("rmm: init data\n");
	for (i = 0; i < NUM_QPS; i++) {
		struct rdma_handle *rh;
		rh = rdma_handles[i] = kzalloc(sizeof(struct rdma_handle), GFP_KERNEL);
		if (!rh) 
			goto out_free;
		if (!(rpc_pools[i] = kzalloc(sizeof(struct pool_info), GFP_KERNEL)))
			goto out_free;

		rh->state = RDMA_INIT;
		rh->vaddr_start = 0;

		spin_lock_init(&rh->rdma_work_head_lock);

		init_completion(&rh->cm_done);
		init_completion(&rh->init_done);
	}

	server_rh.state = RDMA_INIT;

	init_completion(&server_rh.cm_done);
	init_connection_infos();
	init_mcos();

	create_worker_thread();

	if (__establish_connections())
		goto out_free;

	init_for_test();


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
