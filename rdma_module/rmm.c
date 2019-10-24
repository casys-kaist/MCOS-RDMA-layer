#include <linux/module.h>
#include <linux/bitmap.h>
#include <linux/seq_file.h>
#include <linux/atomic.h>
#include <linux/proc_fs.h>

#include <rdma/rdma_cm.h>

#include "common.h"
#include "ring_buffer.h"

#define RDMA_PORT 11453
#define RDMA_ADDR_RESOLVE_TIMEOUT_MS 5000

#define SINK_BUFFER_SIZE	(PAGE_SIZE * 12)
#define RPC_BUFFER_SIZE		PAGE_SIZE

#define MAX_RECV_DEPTH	(8)
#define MAX_SEND_DEPTH	(8)
#define RDMA_SLOT_SIZE	(PAGE_SIZE * 2)
#define NR_RDMA_SLOTS	((PAGE_SIZE << (MAX_ORDER - 1)) / RDMA_SLOT_SIZE)

#define CONNECT_FETCH	0
#define CONNECT_EVICT	1

#define RPC_OP_FECTCH	0
#define RPC_OP_EVICT	1

#define PFX "rmm: "
#define DEBUG_LOG if (debug) printk

static int debug = 0;
module_param(debug, int, 0);
MODULE_PARM_DESC(debug, "Debug level (0=none, 1=all)");
static int server = 0;
module_param(server, int, 0);
MODULE_PARM_DESC(server, "0=client, 1=server");

enum wr_id_type {
	WR_ID_REG,
	WR_ID_RPC_ADDR,
	WR_ID_SINK_ADDR,
	WR_ID_RPC,
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
	dma_addr_t recv_dma_addr;
	dma_addr_t rpc_dma_addr;
	size_t recv_buffer_size;
	size_t rpc_buffer_size;

	/*temp dma addr, only used init */
	dma_addr_t pool1;

	/* remote */
	dma_addr_t remote_rpc_dma_addr;
	size_t remote_rpc_size;
	u32 rpc_rkey;

	struct rdma_cm_id *cm_id;
	struct ib_device *device;
	struct ib_cq *cq;
	struct ib_qp *qp;
	struct ib_mr *mr;
};

static int __send_dma_addr(struct rdma_handle *rh, dma_addr_t addr, size_t size);
static int __setup_sink_buffer(void);

/* RDMA handle for each node */
static struct rdma_handle server_rh;
static struct rdma_handle *rdma_handles[MAX_NUM_NODES] = { NULL };
static struct rdma_handle *rdma_handles_evic[MAX_NUM_NODES] = { NULL };
static struct pool_info rpc_pools[MAX_NUM_NODES] = { 0 };
static struct pool_info rpc_pools_evic[MAX_NUM_NODES] = { 0 };

/* Global protection domain (pd) and memory region (mr) */
static struct ib_pd *rdma_pd = NULL;
static struct ib_mr *rdma_mr = NULL;
/*Global CQ */
static struct ib_cq *rdma_cq = NULL;

/* Global RDMA sink */
static DEFINE_SPINLOCK(__rdma_slots_lock);
static DECLARE_BITMAP(__rdma_slots, NR_RDMA_SLOTS) = {0};
static struct pool_info sink_pool;
static dma_addr_t sp_dma_addr;
static char *sink_addr;
static dma_addr_t sink_dma_addr;
static dma_addr_t remote_sink_dma_addr;
static size_t remote_sink_size;
static u32 sink_rkey;

/*proc file for control */
static struct proc_dir_entry *rmm_proc;

/*Variables for server */
static uint32_t my_ip;

static int __handle_recv(struct rdma_handle *rh, struct ib_wc *wc)
{
	int ret = 0;
	struct pool_info *rp;

	switch (wc->wr_id) {
		case WR_ID_RPC_ADDR:
			ib_dma_sync_single_for_cpu(rh->device, rh->pool1,
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			if (rh->connection_type == CONNECT_FETCH)
				rp = &rpc_pools[rh->nid];
			else
				rp = &rpc_pools_evic[rh->nid];
			rh->remote_rpc_dma_addr = rp->addr;
			rh->remote_rpc_size = rp->size;
			rh->rpc_rkey = rp->rkey;
			ib_dma_unmap_single(rh->device, rh->rpc_dma_addr, 
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			DEBUG_LOG(PFX "recv rpc addr %llx\n", rp->addr);
			break;

		case WR_ID_SINK_ADDR:
			ib_dma_sync_single_for_cpu(rh->device, sp_dma_addr,
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			rp = &sink_pool;
			remote_sink_dma_addr = rp->addr;
			remote_sink_size = rp->size;
			sink_rkey = rp->rkey;
			ib_dma_unmap_single(rh->device, sp_dma_addr, 
					sizeof(struct pool_info), DMA_FROM_DEVICE);
			DEBUG_LOG(PFX "recv sink addr %llx\n", rp->addr);
			break;

		default:
			DEBUG_LOG(PFX "error: unknown id\n");
			ret = -1;
			break;
	}

	return 0;
}

static int rmm_wait_for_completion(struct rdma_handle *rh, enum ib_wc_opcode exit_condition)
{
	struct ib_wc wc;
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
						"wr_id %Lx status %d opcode %d vender_err %x\n",
						wc.wr_id, wc.status, wc.opcode, wc.vendor_err);
				goto error;
			}
		}

		switch (wc.opcode) {
			case IB_WC_SEND:
				DEBUG_LOG(PFX "send completion\n");
				if (wc.wr_id == WR_ID_RPC_ADDR)
					DEBUG_LOG(PFX "send rpc addr %llx\n", rh->rpc_dma_addr);
				else if (wc.wr_id == WR_ID_SINK_ADDR)
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
				__handle_recv(rh, &wc);
				break;

			case IB_WC_REG_MR:
				DEBUG_LOG(PFX "mr registerd\n");
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
	int ret;

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
			.cqe = (MAX_SEND_DEPTH + MAX_RECV_DEPTH + NR_RDMA_SLOTS) * 3,
			.comp_vector = 0,
		};

		rdma_cq = ib_create_cq(
				rh->device, cq_comp_handler, NULL, rh, &cq_attr);
		if (IS_ERR(rdma_cq)) {
			ret = PTR_ERR(rdma_cq);
			goto out_err;
		}
		rh->cq = rdma_cq;

		/*
		   ret = ib_req_notify_cq(rh->cq, IB_CQ_NEXT_COMP);
		   if (ret < 0) goto out_err;
		 */
	}

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
				.max_inline_data = 32,
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
			.wr_id = WR_ID_REG,
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
	rmm_wait_for_completion(rh, IB_WC_REG_MR);
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


static int __setup_recv_addr(struct rdma_handle *rh, enum wr_id_type wr_id)
{
	int ret = 0;
	dma_addr_t dma_addr;
	struct ib_recv_wr wr;
	const struct ib_recv_wr *bad_wr = NULL;
	struct ib_sge sgl = {0};
	struct pool_info *pp;

	if (wr_id == WR_ID_RPC_ADDR) {
		if (rh->connection_type == CONNECT_FETCH)
			pp = &rpc_pools[rh->nid];
		else
			pp = &rpc_pools_evic[rh->nid];
	}
	else {
		pp = &sink_pool;
	}

	/*Post receive request for recieve request pool info*/
	dma_addr = ib_dma_map_single(rh->device, pp, sizeof(struct pool_info), DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret) 
		goto out_free;

	sgl.lkey = rdma_pd->local_dma_lkey;
	sgl.addr = dma_addr;
	sgl.length = sizeof(struct pool_info) - 1;

	wr.sg_list = &sgl;
	wr.num_sge = 1;
	wr.next = NULL;
	wr.wr_id = (u64) wr_id; 

	if (wr_id == WR_ID_RPC_ADDR)
		rh->pool1 = dma_addr;
	else
		sp_dma_addr = dma_addr;

	ret = ib_post_recv(rh->qp, &wr, &bad_wr);
	if (ret || bad_wr) 
		goto out_free;

	return ret;

out_free:
	ib_dma_unmap_single(rh->device, dma_addr, sizeof(struct pool_info), DMA_FROM_DEVICE);
	return ret;
}

#ifdef QWERT
static int __setup_work_request_pools(void)
{
	int ret;
	int i;

	/* Initialize send buffer */
	ret = ring_buffer_init(&send_buffer, "rdma_send");
	if (ret) return ret;

	for (i = 0; i < send_buffer.nr_chunks; i++) {
		dma_addr_t dma_addr = ib_dma_map_single(rdma_pd->device,
				send_buffer.chunk_start[i], RB_CHUNK_SIZE, DMA_TO_DEVICE);
		ret = ib_dma_mapping_error(rdma_pd->device, dma_addr);
		if (ret) goto out_unmap;
		send_buffer.dma_addr_base[i] = dma_addr;
	}

	/* Initialize send work request pool */
	for (i = 0; i < MAX_SEND_DEPTH; i++) {
		struct send_work *sw;

		sw = kzalloc(sizeof(*sw), GFP_KERNEL);
		if (!sw) {
			ret = -ENOMEM;
			goto out_unmap;
		}
		sw->header.type = WORK_TYPE_SEND;

		sw->sgl.addr = 0;
		sw->sgl.length = 0;
		sw->sgl.lkey = rdma_pd->local_dma_lkey;

		sw->wr.next = NULL;
		sw->wr.wr_id = (u64)sw;
		sw->wr.sg_list = &sw->sgl;
		sw->wr.num_sge = 1;
		sw->wr.opcode = IB_WR_SEND;
		sw->wr.send_flags = IB_SEND_SIGNALED;

		sw->next = send_work_pool;
		send_work_pool = sw;
	}

	/* Initalize rdma work request pool */
	__refill_rdma_work(NR_RDMA_SLOTS);
	return 0;

out_unmap:
	while (rdma_work_pool) {
		struct rdma_work *rw = rdma_work_pool;
		rdma_work_pool = rw->next;
		kfree(rw);
	}
	while (send_work_pool) {
		struct send_work *sw = send_work_pool;
		send_work_pool = sw->next;
		kfree(sw);
	}
	for (i = 0; i < send_buffer.nr_chunks; i++) {
		if (send_buffer.dma_addr_base[i]) {
			ib_dma_unmap_single(rdma_pd->device,
					send_buffer.dma_addr_base[i], RB_CHUNK_SIZE, DMA_TO_DEVICE);
			send_buffer.dma_addr_base[i] = 0;
		}
	}
	return ret;
}
#endif


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

	step = "post recv";
	ret = __setup_recv_addr(rh, WR_ID_RPC_ADDR);
	if (ret) 
		goto out_err;
	ret = __setup_recv_addr(rh, WR_ID_SINK_ADDR);
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

	step = "setup global sink buffer";
	DEBUG_LOG(PFX "%s\n", step);
	if (rh->nid == 0 && connect_type == CONNECT_FETCH)
		__setup_sink_buffer();

	step = "send pool addr";
	DEBUG_LOG(PFX "%s\n", step);
	ret = __send_dma_addr(rh, rh->rpc_dma_addr, RPC_BUFFER_SIZE);
	if (ret)
		goto out_err;
	ret = __send_dma_addr(rh, sink_dma_addr, SINK_BUFFER_SIZE);
	if (ret)
		goto out_err;

	printk(PFX "Connected to %d\n", nid);
	return 0;

out_err:
	printk(KERN_ERR PFX "Unable to %s, %pI4, %d\n", step, ip_table + nid, ret);
	return ret;
}

static int __setup_sink_buffer(void)
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

	sink_addr = (void *) kmalloc(SINK_BUFFER_SIZE, GFP_KERNEL);
	if (!sink_addr) return -EINVAL;

	sink_dma_addr = ib_dma_map_single(
			rdma_pd->device, sink_addr, SINK_BUFFER_SIZE,
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
	ret = wait_for_completion_io_timeout(&done, 5 * HZ);
	if (!ret) {
		printk("Timed-out to register mr\n");
		ret = -ETIMEDOUT;
		goto out_dereg;
	}

	rdma_mr = mr;
	//printk("lkey: %x, rkey: %x, length: %x\n", mr->lkey, mr->rkey, mr->length);
	return 0;

out_dereg:
	ib_dereg_mr(mr);
	return ret;

out_free:
	kfree(sink_addr);
	sink_addr = NULL;
	return ret;
}


/****************************************************************************
 * Server-side connection handling
 */

/*send dma_addr and rkey */
static int __send_dma_addr(struct rdma_handle *rh, dma_addr_t addr, size_t size)
{
	int ret;
	struct ib_send_wr wr;
	const struct ib_send_wr *bad_wr = NULL;
	struct ib_sge sgl;
	struct pool_info rp = {
		.rkey = rh->mr->rkey,
		.addr = addr,
		.size = size,
	};
	dma_addr_t dma_addr;

	dma_addr = ib_dma_map_single(rh->device, &rp,
			sizeof(struct pool_info), DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret)
		goto err;

	sgl.addr = dma_addr;
	sgl.length = sizeof(struct pool_info);
	sgl.lkey = rdma_pd->local_dma_lkey;

	wr.opcode = IB_WR_SEND;
	wr.send_flags = IB_SEND_SIGNALED;
	wr.sg_list = &sgl;
	wr.num_sge = 1;
	wr.next = NULL;

	ret = ib_post_send(rh->qp, &wr, &bad_wr);
	if (ret || bad_wr) {
		printk("Cannot post send wr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto
			unmap;
	}
	ret = rmm_wait_for_completion(rh, IB_WC_SEND);
	if (ret)
		goto unmap;

	return 0;

unmap:
	ib_dma_unmap_single(rh->device, dma_addr, sizeof(struct pool_info), DMA_FROM_DEVICE);
err:
	return ret;
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

	step = "post recv";
	ret = __setup_recv_addr(rh, WR_ID_RPC_ADDR);
	if (ret)  goto out_err;

	if (connect_type == CONNECT_FETCH) {
		ret = __setup_recv_addr(rh, WR_ID_SINK_ADDR);
		if (ret)  goto out_err;
	}

	step = "accept";
	rh->state = RDMA_CONNECTING;
	ret = rdma_accept(rh->cm_id, &conn_param);
	if (ret)  goto out_err;

	step = "setup rpc buffer";
	ret = __setup_rpc_buffer(rh);
	if (ret)  goto out_err;

	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (ret)  goto out_err;

	step = "post send";
	ret = __send_dma_addr(rh, rh->rpc_dma_addr, RPC_BUFFER_SIZE);
	if (ret)  goto out_err;

	if (connect_type == CONNECT_FETCH) {
		ret = __send_dma_addr(rh, sink_dma_addr, SINK_BUFFER_SIZE);
		if (ret)  goto out_err;
	}

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

	if (__setup_sink_buffer < 0)
		printk(PFX "Fail to allocate sink buffer\n");
	printk(PFX "Sink buffer allocated\n");

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
	int num_client;

	DEBUG_LOG(PFX "establish connection\n");
	if (server) {
		ret = __listen_to_connection();
		if (ret) 
			return ret;

		for (num_client = 0; num_client < MAX_NUM_NODES; num_client++) {
			if ((ret = __accept_client(num_client, CONNECT_FETCH))) 
				return ret;
			if ((ret = __accept_client(num_client, CONNECT_EVICT))) 
				return ret;
			/*wait for recv rpc pool addr and sink buffer */
			rmm_wait_for_completion(rdma_handles[num_client], IB_WC_RECV);
			rmm_wait_for_completion(rdma_handles[num_client], IB_WC_RECV);
			rmm_wait_for_completion(rdma_handles_evic[num_client], IB_WC_RECV);
		}
		printk(KERN_INFO PFX "Cannot accepts more clients\n");
	}
	else {
		for (i = 0; i < MAX_NUM_NODES; i++) { 
			DEBUG_LOG(PFX "connect to server\n");
			if ((ret = __connect_to_server(i, CONNECT_FETCH))) 
				return ret;
			if ((ret = __connect_to_server(i, CONNECT_EVICT))) 
				return ret;
			/*wait for recv request pool addr */
			rmm_wait_for_completion(rdma_handles[i], IB_WC_RECV);
			rmm_wait_for_completion(rdma_handles_evic[i], IB_WC_RECV);
		}
	}

	printk(PFX "Connections are established.\n");
	return 0;
}

void __exit exit_rmm_rdma(void)
{
	int i;

	/* Detach from upper layer to prevent race condition during exit */

	//remove_proc_entry("rmm", NULL);
	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh = rdma_handles[i];
		if (!rh) continue;

		if (rh->recv_buffer) {
			ib_dma_unmap_single(rh->device, rh->recv_dma_addr,
					PAGE_SIZE, DMA_FROM_DEVICE);
			kfree(rh->recv_buffer);
		}

		if (rh->rpc_buffer) {
			ib_dma_unmap_single(rh->device, rh->rpc_dma_addr,
					PAGE_SIZE, DMA_FROM_DEVICE);
			kfree(rh->rpc_buffer);
		}

		if (rh->qp && !IS_ERR(rh->qp)) rdma_destroy_qp(rh->cm_id);
		if (rh->cq && !IS_ERR(rh->cq)) ib_destroy_cq(rh->cq);
		if (rh->cm_id && !IS_ERR(rh->cm_id)) rdma_destroy_id(rh->cm_id);

		kfree(rdma_handles[i]);

		/*free evic */
		rh = rdma_handles_evic[i];
		if (!rh) continue;

		if (rh->recv_buffer) {
			ib_dma_unmap_single(rh->device, rh->recv_dma_addr,
					PAGE_SIZE * 16, DMA_FROM_DEVICE);
			kfree(rh->recv_buffer);
		}

		if (rh->rpc_buffer) {
			ib_dma_unmap_single(rh->device, rh->rpc_dma_addr,
					PAGE_SIZE, DMA_FROM_DEVICE);
			kfree(rh->rpc_buffer);
		}

		if (rh->qp && !IS_ERR(rh->qp)) rdma_destroy_qp(rh->cm_id);
		if (rh->cq && !IS_ERR(rh->cq)) ib_destroy_cq(rh->cq);
		if (rh->cm_id && !IS_ERR(rh->cm_id)) rdma_destroy_id(rh->cm_id);

		kfree(rdma_handles[i]);
	}

	/* MR is set correctly iff rdma buffer and pd are correctly allocated */
	if (rdma_mr && !IS_ERR(rdma_mr)) {
		ib_dereg_mr(rdma_mr);
		ib_dma_unmap_single(rdma_pd->device, sink_dma_addr,
				SINK_BUFFER_SIZE, DMA_FROM_DEVICE);
		kfree(sink_addr);
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
	/*
	   rmm_proc = proc_create("rmm", 0666, NULL, &rmm_ops);
	   if (rmm_proc == NULL) {
	   printk(KERN_ERR PFX "cannot create /proc/rmm\n");
	   return -ENOMEM;
	   }
	 */

	if (!identify_myself(&my_ip)) 
		return -EINVAL;

	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh;
		rh = rdma_handles[i] = kzalloc(sizeof(struct rdma_handle), GFP_KERNEL);
		if (!rh) 
			goto out_free;

		rh->nid = i;
		rh->state = RDMA_INIT;
		rh->connection_type = CONNECT_FETCH;
		init_completion(&rh->cm_done);
	}
	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh;
		rh = rdma_handles_evic[i] = kzalloc(sizeof(struct rdma_handle), GFP_KERNEL);
		if (!rh) 
			goto out_free;

		rh->nid = i;
		rh->state = RDMA_INIT;
		rh->connection_type = CONNECT_EVICT;
		init_completion(&rh->cm_done);
	}
	server_rh.state = RDMA_INIT;
	init_completion(&server_rh.cm_done);

	if (__establish_connections())
		goto out_free;

	/*server init sink buffer at cm handler */
	/*Init sink buffer and send it */
	if (!server ) {
		if (__setup_sink_buffer())
			goto out_free;

		for (i = 0; i < MAX_NUM_NODES; i++)
			__send_dma_addr(rdma_handles[i], sink_dma_addr, SINK_BUFFER_SIZE);
	}

	/*
	   if (__setup_work_request_pools())
	   goto out_free;
	 */

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
