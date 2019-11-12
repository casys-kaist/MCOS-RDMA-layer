#include <linux/module.h>
#include <linux/bitmap.h>
#include <linux/seq_file.h>
#include <linux/atomic.h>
#include <linux/proc_fs.h>
#include <linux/kthread.h>

#include <rdma/rdma_cm.h>

#include "common.h"
#include "ring_buffer.h"

#define RDMA_PORT 11453
#define RDMA_ADDR_RESOLVE_TIMEOUT_MS 5000

#define IMM_DATA_SIZE 32
#define RPC_ARGS_SIZE 32

#define SINK_BUFFER_SIZE	(PAGE_SIZE * 100)
#define RPC_BUFFER_SIZE		PAGE_SIZE
#define RDMA_BUFFER_SIZE	PAGE_SIZE

#define MAX_RECV_DEPTH	((PAGE_SIZE / IMM_DATA_SIZE) + 10)
#define MAX_SEND_DEPTH	(8)
#define RDMA_SLOT_SIZE	(PAGE_SIZE * 2)
#define NR_RDMA_SLOTS	(RPC_BUFFER_SIZE / RPC_ARGS_SIZE)
#define NR_RPC_SLOTS	(RPC_BUFFER_SIZE / RPC_ARGS_SIZE)

#define CONNECT_FETCH	0
#define CONNECT_EVICT	1

#define PFX "rmm: "
#define DEBUG_LOG if (debug) printk

static int debug = 0;
module_param(debug, int, 0);
MODULE_PARM_DESC(debug, "Debug level (0=none, 1=all)");
static int server = 0;
module_param(server, int, 0);
MODULE_PARM_DESC(server, "0=client, 1=server");

enum rpc_opcode {
	RPC_OP_FECTCH,
	RPC_OP_EVICT,
};

enum wr_type {
	WORK_TYPE_REG,
	WORK_TYPE_RPC_ADDR,
	WORK_TYPE_SINK_ADDR,
	WORK_TYPE_RPC_RECV,
	WORK_TYPE_SEND_ADDR,
	WORK_TYPE_RPC,
};

struct recv_work {
	enum wr_type work_type;
	struct rdma_handle *rh;
	struct ib_sge sgl;
	struct ib_recv_wr wr;
	dma_addr_t dma_addr;
	void *addr;
};

struct send_work {
	enum wr_type work_type;
	struct rdma_handle *rh;
	struct send_work *next;
	struct ib_sge sgl;
	struct ib_send_wr wr;
	void *addr;
	unsigned long flags;
};

struct rdma_work {
	enum wr_type work_type;
	struct rdma_work *next;
	struct ib_sge sgl;
	struct ib_rdma_wr wr;
	void *addr;
	dma_addr_t dma_addr;
};

struct pool_info {
	u32 rkey;
	dma_addr_t addr;
	size_t size;
};

struct rdma_handle {
	int nid;
	enum {
		RDMA_INIT,
		RDMA_ADDR_RESOLVED,
		RDMA_ROUTE_RESOLVED,
		RDMA_CONNECTING,
		RDMA_CONNECTED,
		RDMA_CLOSING,
		RDMA_CLOSED,
	} state;
	struct completion cm_done;
	struct recv_work *recv_works;

	int connection_type;

	/* local */
	void *recv_buffer;
	void *rpc_buffer;
	void *sink_buffer;

	struct rdma_work *rdma_work_pool;
	spinlock_t rdma_work_pool_lock;

	dma_addr_t recv_buffer_dma_addr;
	dma_addr_t rpc_dma_addr;
	dma_addr_t sink_dma_addr;

	size_t recv_buffer_size;
	size_t rpc_buffer_size;

	/* remote */
	dma_addr_t remote_rpc_dma_addr;
	dma_addr_t remote_sink_dma_addr;
	size_t remote_rpc_size;
	size_t remote_sink_size;
	u32 rpc_rkey;
	u32 sink_rkey;

	DECLARE_BITMAP(rpc_slots, NR_RDMA_SLOTS);
	spinlock_t rpc_slots_lock;

	struct rdma_cm_id *cm_id;
	struct ib_device *device;
	struct ib_cq *cq;
	struct ib_qp *qp;
	struct ib_mr *mr;
};

static int __send_dma_addr(struct rdma_handle *rh, dma_addr_t addr, size_t size);
static int __setup_sink_buffer(struct rdma_handle *rh);
static int __setup_recv_works(struct rdma_handle *rh);
static int __accept_client(int nid, int connect_type);
static int accept_client(void * args);

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
static struct ib_mr *rdma_mr = NULL;
/*Global CQ */
static struct ib_cq *rdma_cq = NULL;

static struct task_struct *accept_k = NULL;
static struct task_struct *polling_k = NULL;

/*proc file for control */
static struct proc_dir_entry *rmm_proc;

/*Variables for server */
static uint32_t my_ip;

static inline int __get_rpc_buffer(struct rdma_handle *rh) 
{
	int i;

	do {
		spin_lock(&rh->rpc_slots_lock);
		i = find_first_zero_bit(rh->rpc_slots, NR_RDMA_SLOTS);
		if (i < NR_RDMA_SLOTS) break;
		spin_unlock(&rh->rpc_slots_lock);
		WARN_ON_ONCE("recv buffer is full");
	} while (i >= NR_RDMA_SLOTS);
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

static int __handle_recv(struct ib_wc *wc)
{
	int ret = 0;
	struct pool_info *rp;
	struct recv_work *rw;
	struct rdma_handle *rh;
	const struct ib_recv_wr *bad_wr;

	rw = (struct recv_work *) wc->wr_id;
	rh = rw->rh;
	switch (rw->work_type) {
		case WORK_TYPE_RPC_ADDR:
			ib_dma_unmap_single(rh->device, rw->dma_addr, 
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			if (rh->connection_type == CONNECT_FETCH)
				rp = rpc_pools[rh->nid];
			else
				rp = rpc_pools_evic[rh->nid];
			rh->remote_rpc_dma_addr = rp->addr;
			rh->remote_rpc_size = rp->size;
			rh->rpc_rkey = rp->rkey;
			kfree(rw);
			DEBUG_LOG(PFX "recv rpc addr %llx %x\n", rh->remote_rpc_dma_addr, rh->rpc_rkey);
			break;

		case WORK_TYPE_SINK_ADDR:
			ib_dma_unmap_single(rh->device, rw->dma_addr, 
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			if (rh->connection_type == CONNECT_FETCH)
				rp = sink_pools[rh->nid];
			else
				rp = sink_pools_evic[rh->nid];
			rh->remote_sink_dma_addr = rp->addr;
			rh->remote_sink_size = rp->size;
			rh->sink_rkey = rp->rkey;
			kfree(rw);
			DEBUG_LOG(PFX "recv sink addr %llx %x\n", rh->remote_sink_dma_addr, rh->sink_rkey);
			break;

		case WORK_TYPE_RPC_RECV:
			ret = ib_post_recv(rh->qp, &rw->wr, &bad_wr);
			if (ret || bad_wr)
				return -1;
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

static int polling_cq(void * args)
{
	struct ib_cq *cq = (struct ib_cq *) args;
	struct ib_wc wc = {0};
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
		polling_k = kthread_run(polling_cq, rdma_cq, "polling_cq");
		if (!polling_k)
			goto out_err;
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
				.max_inline_data = IMM_DATA_SIZE,
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

/*Alloc rpc buffer for RPC */
static  int __setup_rpc_buffer(struct rdma_handle *rh)
{
	int ret = 0;
	dma_addr_t dma_addr;
	size_t buffer_size = RPC_BUFFER_SIZE;
	struct ib_reg_wr reg_wr = {
		.wr = {
			.opcode = IB_WR_REG_MR,
			.send_flags = IB_SEND_SIGNALED,
			.wr_id = WORK_TYPE_REG,
		},
		.access = IB_ACCESS_REMOTE_WRITE,
		/*
		   IB_ACCESS_LOCAL_WRITE |
		   IB_ACCESS_REMOTE_READ |
		 */
	};
	const struct ib_send_wr *bad_wr = NULL;
	struct scatterlist sg = {};
	struct ib_mr *mr;
	char *step = NULL;


	step = "alloc rpc buffer";
	/*rpc buffer */
	rh->rpc_buffer = kmalloc(buffer_size, GFP_KERNEL);
	if (!rh->rpc_buffer) {
		return -ENOMEM;
	}
	dma_addr = ib_dma_map_single(rh->device, rh->rpc_buffer,
			buffer_size, DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret)
		goto out_free;

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

	/*
	ret = rmm_wait_for_completion(rh, IB_WC_REG_MR);
	if (ret < 0) {
		printk(KERN_ERR PFX "failed to register rpc mr\n");
		goto out_dereg;
	}
	*/
	rh->rpc_dma_addr = dma_addr;

	rh->mr = mr;
	rh->rpc_buffer_size = RPC_BUFFER_SIZE;

	return 0;
out_dereg:
	ib_dereg_mr(mr);

out_free:
	if (rh->rpc_buffer)
		kfree(rh->rpc_buffer);
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
		if (rh->connection_type == CONNECT_FETCH)
			pp = rpc_pools[rh->nid];
		else
			pp = rpc_pools_evic[rh->nid];
	}
	else {
		if (rh->connection_type == CONNECT_FETCH)
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

	for (i = 0; i < nr_works; i++) {
		struct rdma_work *rw;

		rw = kzalloc(sizeof(*rw), GFP_KERNEL);
		if (!rw) goto out;

		rw->work_type = WORK_TYPE_RPC;

		rw->sgl.addr = 0;
		rw->sgl.length = 0;
		rw->sgl.lkey = rdma_pd->local_dma_lkey;

		rw->wr.wr.next = NULL;
		rw->wr.wr.wr_id = (u64)rw;
		rw->wr.wr.sg_list = &rw->sgl;
		rw->wr.wr.num_sge = 1;
		rw->wr.wr.opcode = IB_WR_RDMA_WRITE; // IB_WR_RDMA_WRITE_WITH_IMM;
		rw->wr.wr.send_flags = IB_SEND_SIGNALED;
		rw->wr.remote_addr = 0;
		rw->wr.rkey = 0;

		if (!last_work) last_work = rw;
		rw->next = work_list;
		work_list = rw;
		nr_refilled++;
	}

out:
	spin_lock(&rh->rdma_work_pool_lock);
	if (work_list) {
		last_work->next = rh->rdma_work_pool;
		rh->rdma_work_pool = work_list;
	}
	spin_unlock(&rh->rdma_work_pool_lock);
	BUG_ON(nr_refilled == 0);
	return nr_refilled;
}

static struct rdma_work *__get_rdma_work(struct rdma_handle *rh, dma_addr_t dma_addr, size_t size, dma_addr_t rdma_addr, u32 rdma_key)
{
	struct rdma_work *rw;
	might_sleep();

	spin_lock(&rh->rdma_work_pool_lock);
	rw = rh->rdma_work_pool;
	rh->rdma_work_pool = rh->rdma_work_pool->next;
	spin_unlock(&rh->rdma_work_pool_lock);

	if (!rh->rdma_work_pool) {
	//	__refill_rdma_work(NR_RDMA_SLOTS);
		return NULL;
	}

	rw->sgl.addr = dma_addr;
	rw->sgl.length = size;

	rw->wr.remote_addr = rdma_addr;
	rw->wr.rkey = rdma_key;
	return rw;
}

static void __put_rdma_work(struct rdma_handle *rh, struct rdma_work *rw)
{
	might_sleep();
	spin_lock(&rh->rdma_work_pool_lock);
	rw->next = rh->rdma_work_pool;
	rh->rdma_work_pool = rw;
	spin_unlock(&rh->rdma_work_pool_lock);
}

static int __setup_work_request_pools(struct rdma_handle *rh)
{
	/* Initalize rdma work request pool */
	__refill_rdma_work(rh, NR_RDMA_SLOTS);
	return 0;

}

static int __setup_recv_works(struct rdma_handle *rh)
{
	int ret = 0, i;
	dma_addr_t dma_addr;
	char *recv_buffer = NULL;
	struct recv_work *rws = NULL;
	const size_t buffer_size = PAGE_SIZE;

	ret = __setup_recv_addr(rh, WORK_TYPE_RPC_ADDR);
	if (ret)
		return ret;

	ret = __setup_recv_addr(rh, WORK_TYPE_SINK_ADDR);
	if (ret)
		return ret;

	/* Initalize receive buffers */
	recv_buffer = kmalloc(buffer_size, GFP_KERNEL);
	if (!recv_buffer) {
		return -ENOMEM;
	}
	rws = kmalloc(sizeof(*rws) * (buffer_size / IMM_DATA_SIZE), GFP_KERNEL);
	if (!rws) {
		ret = -ENOMEM;
		goto out_free;
	}

	/* Populate receive buffer and work requests */
	dma_addr = ib_dma_map_single(
			rh->device, recv_buffer, buffer_size, DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret) goto out_free;

	/* receive work request pre-post */
	for (i = 0; i < MAX_RECV_DEPTH; i++) {
		struct recv_work *rw = rws + i;
		struct ib_recv_wr *wr;
		const struct ib_recv_wr *bad_wr = NULL;
		struct ib_sge *sgl;

		rw->work_type = WORK_TYPE_RPC_RECV;
		rw->dma_addr = dma_addr + IMM_DATA_SIZE * i;
		rw->addr = recv_buffer + IMM_DATA_SIZE * i;
		rw->rh = rh;

		sgl = &rw->sgl;
		sgl->lkey = rdma_pd->local_dma_lkey;
		sgl->addr = rw->dma_addr;
		sgl->length = IMM_DATA_SIZE;

		wr = &rw->wr;
		wr->sg_list = sgl;
		wr->num_sge = 1;
		wr->next = NULL;
		wr->wr_id = (u64) rw;

		ret = ib_post_recv(rh->qp, wr, &bad_wr);
		if (ret || bad_wr) goto out_free;
	}
	rh->recv_works = rws;
	rh->recv_buffer = recv_buffer;
	rh->recv_buffer_dma_addr = dma_addr;

	return ret;

out_free:
	if (recv_buffer) kfree(recv_buffer);
	if (rws) kfree(rws);
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

	if (connect_type == CONNECT_FETCH)
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
	ret = __setup_recv_works(rh);
	if (ret) 
		goto out_err;

	step = "setup work reqeusts";
	ret = __setup_work_request_pools(rh);
	if (ret)
		goto out_err;

	step = "connect";
	DEBUG_LOG(PFX "%s\n", step);
	c_type = connect_type;
	{
		struct rdma_conn_param conn_param = {
			.private_data = &c_type,
			.private_data_len = sizeof(connect_type),
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
	step = "setup buffers and pools";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __setup_rpc_buffer(rh);
	if (ret)
		goto out_err;

	step = "setup sink buffer";
	DEBUG_LOG(PFX "%s\n", step);
	__setup_sink_buffer(rh);

	step = "send pool addr";
	DEBUG_LOG(PFX "%s %llx %llx\n", step, rh->rpc_dma_addr, rh->sink_dma_addr);
	ret = __send_dma_addr(rh, rh->rpc_dma_addr, RPC_BUFFER_SIZE);
	if (ret)
		goto out_err;

	ret = __send_dma_addr(rh, rh->sink_dma_addr, SINK_BUFFER_SIZE);
	if (ret)
		goto out_err;

	printk(PFX "Connected to %d\n", nid);
	return 0;

out_err:
	printk(KERN_ERR PFX "Unable to %s, %pI4, %d\n", step, ip_table + nid, ret);
	return ret;
}

static int __setup_sink_buffer(struct rdma_handle *rh)
{
	int ret;
	DECLARE_COMPLETION_ONSTACK(done);
	struct ib_mr *mr = NULL;
	const struct ib_send_wr *bad_wr = NULL;
	struct ib_reg_wr reg_wr = {
		.wr = {
			.opcode = IB_WR_REG_MR,
			.send_flags = IB_SEND_SIGNALED,
			.wr_id = (u64)&done,
		},
		.access = IB_ACCESS_REMOTE_WRITE,
		/*
		   IB_ACCESS_LOCAL_WRITE |
		   IB_ACCESS_REMOTE_READ |
		 */
	};
	struct scatterlist sg = {};
	void *sink_buffer;
	dma_addr_t sink_dma_addr;

	sink_buffer = (void *) kmalloc(SINK_BUFFER_SIZE, GFP_KERNEL);
	if (!sink_buffer) return -ENOMEM;

	sink_dma_addr = ib_dma_map_single(
			rdma_pd->device, sink_buffer, SINK_BUFFER_SIZE,
			DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rdma_pd->device, sink_dma_addr);
	if (ret) goto out_free;

	mr = ib_alloc_mr(rdma_pd, IB_MR_TYPE_MEM_REG, SINK_BUFFER_SIZE / PAGE_SIZE);
	if (IS_ERR(mr)) {
		ret = PTR_ERR(mr);
		goto out_free;
	}

	sg_dma_address(&sg) = sink_dma_addr;
	sg_dma_len(&sg) = SINK_BUFFER_SIZE;

	ret = ib_map_mr_sg(mr, &sg, 1, NULL, SINK_BUFFER_SIZE);
	if (ret != 1) {
		printk("Cannot map scatterlist to mr, %d\n", ret);
		goto out_dereg;
	}
	reg_wr.mr = mr;
	reg_wr.key = mr->rkey;

	ret = ib_post_send(rdma_handles[0]->qp, &reg_wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk("Cannot register mr, %d %p\n", ret, bad_wr);
		if (bad_wr) ret = -EINVAL;
		goto out_dereg;
	}

	rdma_mr = mr;
	rh->sink_dma_addr = sink_dma_addr;
	rh->sink_buffer = sink_buffer;

	return 0;

out_dereg:
	ib_dereg_mr(mr);
	return ret;

out_free:
	kfree(sink_buffer);
	return ret;
}


/****************************************************************************
 * Server-side connection handling
 */

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

static int accept_client(void *args)
{
	int num_client;
	int ret = 0;

	for (num_client = 0; num_client < MAX_NUM_NODES; num_client++) {
		if (kthread_should_stop())
			return 0;
		if ((ret = __accept_client(num_client, CONNECT_FETCH))) 
			return ret;
		if ((ret = __accept_client(num_client, CONNECT_EVICT))) 
			return ret;
	}
	printk(KERN_INFO PFX "Cannot accepts more clients\n");

	return 0;
}


static int __accept_client(int nid, int connect_type)
{
	struct rdma_handle *rh;
	struct rdma_conn_param conn_param = {};
	char *step = NULL;
	int ret;

	if (connect_type == CONNECT_FETCH)
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
	if (ret)  goto out_err;

	step = "accept";
	rh->state = RDMA_CONNECTING;
	ret = rdma_accept(rh->cm_id, &conn_param);
	if (ret)  goto out_err;

	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (ret)  goto out_err;

	step = "setup sink buffer";
	ret = __setup_sink_buffer(rh);
	if (ret) goto out_err;

	step = "setup rpc buffer";
	ret = __setup_rpc_buffer(rh);
	if (ret)  goto out_err;


	step = "post send";
	ret = __send_dma_addr(rh, rh->rpc_dma_addr, RPC_BUFFER_SIZE);
	if (ret)  goto out_err;

	ret = __send_dma_addr(rh, rh->sink_dma_addr, SINK_BUFFER_SIZE);
	if (ret)  goto out_err;

	return 0;

out_err:
	printk(KERN_ERR PFX "Failed at %s, %d\n", step, ret);
	return ret;
}

static int __on_client_connecting(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	int connect_type = *(int *)cm_event->param.conn.private_data;
	static atomic_t num_request = ATOMIC_INIT(-1);
	static atomic_t num_request_evic = ATOMIC_INIT(-1);
	int nid = 0;
	struct rdma_handle *rh;

	if (connect_type == CONNECT_FETCH) {
		DEBUG_LOG(PFX "atomic inc\n");
		nid = atomic_inc_return(&num_request);
		DEBUG_LOG(PFX "nid: %d\n", nid);
		if (nid == MAX_NUM_NODES)
			return 0;
		rh = rdma_handles[nid];
		//rh->nid = nid;
	}
	else {
		nid = atomic_inc_return(&num_request_evic);
		if (nid == MAX_NUM_NODES)
			return 0;
		rh = rdma_handles_evic[nid];
		//rh->nid = nid;
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
	int i, ret;

	DEBUG_LOG(PFX "establish connection\n");
	if (server) {
		ret = __listen_to_connection();
		if (ret) 
			return ret;
		accept_k = kthread_run(accept_client, NULL, "accept thread");
		if (!accept_k)
			return -ENOMEM;
	}
	else {
		for (i = 0; i < MAX_NUM_NODES; i++) { 
			DEBUG_LOG(PFX "connect to server\n");
			if ((ret = __connect_to_server(i, CONNECT_FETCH))) 
				return ret;
			if ((ret = __connect_to_server(i, CONNECT_EVICT))) 
				return ret;
		}
		printk(PFX "Connections are established.\n");
	}

	return 0;
}

void __exit exit_rmm_rdma(void)
{
	int i;

	/* Detach from upper layer to prevent race condition during exit */
	if (polling_k)
		kthread_stop(polling_k);
	if (server && accept_k) {
		for (i = 0; i < MAX_NUM_NODES; i++) {
			complete(&rdma_handles[i]->cm_done);
			complete(&rdma_handles_evic[i]->cm_done);
		}
		kthread_stop(accept_k);
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

		if (rh->rpc_buffer) {
			ib_dma_unmap_single(rh->device, rh->rpc_dma_addr,
					RPC_BUFFER_SIZE, DMA_FROM_DEVICE);
			kfree(rh->rpc_buffer);
		}

		if (rh->sink_buffer) {
			ib_dma_unmap_single(rh->device, rh->sink_dma_addr,
					SINK_BUFFER_SIZE, DMA_FROM_DEVICE);
			kfree(rh->sink_buffer);
		}

		if (rh->qp && !IS_ERR(rh->qp)) rdma_destroy_qp(rh->cm_id);
		if (rh->cq && !IS_ERR(rh->cq)) ib_destroy_cq(rh->cq);
		if (rh->cm_id && !IS_ERR(rh->cm_id)) rdma_destroy_id(rh->cm_id);

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

		if (rh->rpc_buffer) {
			ib_dma_unmap_single(rh->device, rh->rpc_dma_addr,
					PAGE_SIZE, DMA_FROM_DEVICE);
			kfree(rh->rpc_buffer);
		}

		if (rh->sink_buffer) {
			ib_dma_unmap_single(rh->device, rh->sink_dma_addr,
					SINK_BUFFER_SIZE, DMA_FROM_DEVICE);
			kfree(rh->sink_buffer);
		}

		if (rh->qp && !IS_ERR(rh->qp)) rdma_destroy_qp(rh->cm_id);
		if (rh->cq && !IS_ERR(rh->cq)) ib_destroy_cq(rh->cq);
		if (rh->cm_id && !IS_ERR(rh->cm_id)) rdma_destroy_id(rh->cm_id);

		kfree(rdma_handles[i]);
		kfree(rpc_pools_evic[i]);
		kfree(sink_pools_evic[i]);
	}

	/* MR is set correctly iff rdma buffer and pd are correctly allocated */
	if (rdma_mr && !IS_ERR(rdma_mr)) {
		ib_dereg_mr(rdma_mr);
		ib_dealloc_pd(rdma_pd);
	}

	/*
	   for (i = 0; i < send_buffer.nr_chunks; i++) {
	   if (send_buffer.dma_addr_base[i]) {
	   ib_dma_unmap_single(rdma_pd->device,
	   send_buffer.dma_addr_base[i], RB_CHUNK_SIZE, DMA_TO_DEVICE);
	   }
	   }
	   while (send_work_pool) {
	   struct send_work *sw = send_work_pool;
	   send_work_pool = sw->next;
	   kfree(sw);
	   }
	   ring_buffer_destroy(&send_buffer);

	   while (rdma_work_pool) {
	   struct rdma_work *rw = rdma_work_pool;
	   rdma_work_pool = rw->next;
	   kfree(rw);

	 */

	printk(KERN_INFO PFX "RDMA unloaded\n");
	return;
}

static void disconnect(void)
{
	int i = 0;

	if (server)
		return;

	for (i = 0; i < MAX_NUM_NODES; i++) {
		if (rdma_handles[i])
			rdma_disconnect(rdma_handles[i]->cm_id);
		if (rdma_handles_evic[i])
			rdma_disconnect(rdma_handles[i]->cm_id);
	}
}

static ssize_t rmm_write_proc(struct file *file, const char __user *buffer,
		size_t count, loff_t *ppos)
{
	char *cmd;

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

	if (strcmp("exit", cmd) == 0)
		exit_rmm_rdma();
	else if (strcmp("diss", cmd) == 0)
		disconnect();

	return 0;
}

static struct file_operations rmm_ops = {
	.owner = THIS_MODULE,
	.write = rmm_write_proc,
};

int __init init_rmm_rdma(void)
{
	int i;

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
		rh->connection_type = CONNECT_FETCH;
		spin_lock_init(&rh->rdma_work_pool_lock);
		spin_lock_init(&rh->rpc_slots_lock);
		init_completion(&rh->cm_done);
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
		rh->connection_type = CONNECT_EVICT;
		spin_lock_init(&rh->rdma_work_pool_lock);
		spin_lock_init(&rh->rpc_slots_lock);
		init_completion(&rh->cm_done);
	}
	server_rh.state = RDMA_INIT;
	init_completion(&server_rh.cm_done);

	if (__establish_connections())
		goto out_free;

	printk("Ready on InfiniBand RDMA\n");
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
