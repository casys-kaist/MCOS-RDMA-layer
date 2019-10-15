#include <linux/module.h>
#include <linux/bitmap.h>
#include <linux/seq_file.h>
#include <linux/atomic.h>

#include <rdma/rdma_cm.h>

#include "common.h"
#include "ring_buffer.h"

#define RDMA_PORT 11453
#define RDMA_ADDR_RESOLVE_TIMEOUT_MS 5000

#define MAX_RECV_DEPTH	(8)
#define MAX_SEND_DEPTH	(8)
#define RDMA_SLOT_SIZE	(PAGE_SIZE * 2)
#define NR_RDMA_SLOTS	((PAGE_SIZE << (MAX_ORDER - 1)) / RDMA_SLOT_SIZE)

#define PFX "RMM: "
#define DEBUG_LOG if (debug) printk

static int debug = 0;
module_param(debug, int, 0);
MODULE_PARM_DESC(debug, "Debug level (0=none, 1=all)");
static int server = 0;
module_param(server, int, 0);
MODULE_PARM_DESC(server, "0=client, 1=server");

struct request_pool {
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
	void *recv_buffer;
	void *request_buffer;
	dma_addr_t recv_buffer_dma_addr;
	dma_addr_t request_buffer_dma_addr;
	size_t buffer_size;

	struct rdma_cm_id *cm_id;
	struct ib_device *device;
	struct ib_cq *cq;
	struct ib_qp *qp;
	struct ib_mr *mr;
};

/* RDMA handle for each node */
static struct rdma_handle *rdma_handles[MAX_NUM_NODES] = { NULL };
static struct rdma_handle *rdma_handles_evic[MAX_NUM_NODES] = { NULL };
static struct request_pool request_pools[MAX_NUM_NODES] = { 0 };

/* Global protection domain (pd) and memory region (mr) */
static struct ib_pd *rdma_pd = NULL;
static struct ib_mr *rdma_mr = NULL;

/* Global RDMA sink */
static DEFINE_SPINLOCK(__rdma_slots_lock);
static DECLARE_BITMAP(__rdma_slots, NR_RDMA_SLOTS) = {0};
static char *__rdma_sink_addr;
static dma_addr_t __rdma_sink_dma_addr;

/*Variables for server */
static int num_client = 0;

static int rmm_wait_for_completion(struct rdma_handle *rh, enum ib_wc_opcode exit_condition)
{
	struct ib_wc wc;
	const struct ib_recv_wr *bad_wr;
	int ret;

	while ((ret = ib_poll_cq(rh->cq, 1, &wc)) >= 0) {
		if (ret == 0) {
			if (rh->state == RDMA_CLOSED)
				return -1;
			continue;
		}

		if (wc.status) {
			if (wc.status == IB_WC_WR_FLUSH_ERR) {
				DEBUG_LOG("cq flushed\n");
				continue;
			} else {
				printk(KERN_ERR PFX "cq completion failed with "
						"wr_id %Lx status %d opcode %d vender_err %x\n",
						wc.wr_id, wc.status, wc.opcode, wc.vendor_err);
				goto error;
			}
		}

		switch (wc.opcode) {
			case IB_WC_SEND:
				DEBUG_LOG(PFX "send completion\n");
				break;

			case IB_WC_RDMA_WRITE:
				DEBUG_LOG(PFX "rdma write completion\n");
				break;

			case IB_WC_RDMA_READ:
				DEBUG_LOG(PFX "rdma read completion\n");
				break;

			case IB_WC_RECV:
				DEBUG_LOG(PFX "recv completion\n");
				break;

			default:
				printk(KERN_ERR PFX
						"%s:%d Unexpected opcode %d, Shutting down\n",
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
static __init int __setup_pd_cq_qp(struct rdma_handle *rh)
{
	int ret;

	BUG_ON(rh->state != RDMA_ROUTE_RESOLVED && "for rh->device");

	/* Create global pd if it is not allocated yet */
	if (!rdma_pd) {
		rdma_pd = ib_alloc_pd(rh->device);
		if (IS_ERR(rdma_pd)) {
			ret = PTR_ERR(rdma_pd);
			rdma_pd = NULL;
			goto out_err;
		}
	}

	/* create completion queue */
	if (!rh->cq) {
		struct ib_cq_init_attr cq_attr = {
			.cqe = MAX_SEND_DEPTH + MAX_RECV_DEPTH + NR_RDMA_SLOTS,
			.comp_vector = 0,
		};

		rh->cq = ib_create_cq(
				rh->device, cq_comp_handler, NULL, rh, &cq_attr);
		if (IS_ERR(rh->cq)) {
			ret = PTR_ERR(rh->cq);
			goto out_err;
		}

		/*
		ret = ib_req_notify_cq(rh->cq, IB_CQ_NEXT_COMP);
		if (ret < 0) goto out_err;
		*/
	}
	if (!rh_evic->cq) {
		struct ib_cq_init_attr cq_attr = {
			.cqe = MAX_SEND_DEPTH + MAX_RECV_DEPTH + NR_RDMA_SLOTS,
			.comp_vector = 0,
		};

		rh_evic->cq = ib_create_cq(
				rh_evic->device, cq_comp_handler, NULL, rh_evic, &cq_attr);
		if (IS_ERR(rh_evic->cq)) {
			ret = PTR_ERR(rh_evic->cq);
			goto out_err;
		}

		/*
		ret = ib_req_notify_cq(rh_evic->cq, IB_CQ_NEXT_COMP);
		if (ret < 0) goto out_err;
		*/
	}

	/* create queue pair */
	{
		struct ib_qp_init_attr qp_attr = {
			.event_handler = NULL, // qp_event_handler,
			.qp_context = rh,
			.cap = {
				.max_send_wr = MAX_SEND_DEPTH,
				.max_recv_wr = MAX_RECV_DEPTH + NR_RDMA_SLOTS,
				.max_send_sge = PCN_KMSG_MAX_SIZE >> PAGE_SHIFT,
				.max_recv_sge = PCN_KMSG_MAX_SIZE >> PAGE_SHIFT,
			},
			.sq_sig_type = IB_SIGNAL_REQ_WR,
			.qp_type = IB_QPT_RC,
			.send_cq = rh->cq,
			.recv_cq = rh->cq,
		};

		ret = rdma_create_qp(rh->cm_id, rdma_pd, &qp_attr);
		if (ret) 
			goto out_err;
		rh->qp = rh->cm_id->qp;
	}
	{
		struct ib_qp_init_attr qp_attr = {
			.event_handler = NULL, // qp_event_handler,
			.qp_context = rh_evic,
			.cap = {
				.max_send_wr = MAX_SEND_DEPTH * 8,
				.max_recv_wr = MAX_RECV_DEPTH + NR_RDMA_SLOTS,
				.max_send_sge = PCN_KMSG_MAX_SIZE >> PAGE_SHIFT,
				.max_recv_sge = PCN_KMSG_MAX_SIZE >> PAGE_SHIFT,
			},
			.sq_sig_type = IB_SIGNAL_REQ_WR,
			.qp_type = IB_QPT_RC,
			.send_cq = rh_evic->cq,
			.recv_cq = rh_evic->cq,
		};

		ret = rdma_create_qp(rh_evic->cm_id, rdma_pd, &qp_attr);
		if (ret) 
			goto out_err;
		rh_evic->qp = rh_evic->cm_id->qp;
	}

	return 0;

out_err:
	return ret;
}
static __init int __setup_buffers_and_pools(struct rdma_handler *rh)
{
	int ret = 0;
	dma_addr_t dma_addr;
	const size_t buffer_size = PAGE_SIZE * 2;
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

	rh->request_buffer = kmalloc(buffer_size, GFP_KERNEL);
	if (!rh->request_buffer) {
		return -ENOMEM;
	}
	dma_addr = ib_dma_map_single(rh->device, rh->request_buffer,
			buffer_size, DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret)
		goto out_free;

	mr = ib_alloc_mr(rdma_pd, IB_MR_TYPE_MEM_REG, buffer_size / PAGE_SIZE);
	if (IS_ERR(mr)) {
		ret = PTR_ERR(mr);
		goto out_free;
	}

	sg_dma_address(&sg) = dma_addr;
	sg_dma_len(&sg) = buffer_size;

	ret = ib_map_mr_sg(mr, &sg, 1, buffer_size);
	if (ret != 1) {
		printk("Cannot map scatterlist to mr, %d\n", ret);
		goto out_dereg;
	}
	reg_wr.mr = mr;
	reg_wr.key = mr->rkey;

	ret = ib_post_send(rh->qp, &reg_wr.wr, &bad_wr);
	if (ret || bad_wr) {
		printk("Cannot register mr, %d %p\n", ret, bad_wr);
		if (bad_wr)
			ret = -EINVAL;
		goto
			out_dereg;
	}
	rmm_wait_for_completion(rh, IB_WC_REG_MR);

	rh->mr = mr;
	rh->buffer_size = buffer_size;
out_dreg:
	ib_dereg_mr(mr);
	return ret;

out_free:
	if (rh->request_buffer)
		kfree(request_buffer);
}

static __init int __setup_recv_request(struct rdma_handle *rh)
{
	int ret = 0, i;
	dma_addr_t dma_addr;
	struct ib_recv_wr wr, *bad_wr = NULL;
	struct ib_sge *sgl;


	/*Post receive request for recieve request pool info*/
	dma_addr = ib_dma_map_single(
			rh->device, &request_pools[rh->nid], sizeof(struct request_pool), DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret) 
		goto out_free;

	sgl = &request_pools[rh->nid];
	sgl->lkey = rdma_pd->local_dma_lkey;
	sgl->addr = dma_addr;
	sgl->length = sizeof(struct request_pool);

	wr = &rw->wr;
	wr.sg_list = sgl;
	wr.num_sge = 1;
	wr.next = NULL;
	wr.wr_id = (u64) rh->nid;

	ret = ib_post_recv(rh->qp, &wr, &bad_wr);
	if (ret || bad_wr) 
		goto out_free;

	return ret;

out_free:
	return ret;
}


static int __init __setup_work_request_pools(void)
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
			MSGPRINTK("Disconnected from %d\n", rh->nid);
			/* TODO deallocate associated resources */
			break;
		case RDMA_CM_EVENT_REJECTED:
		case RDMA_CM_EVENT_CONNECT_ERROR:
			complete(&rh->cm_done);
			break;
		case RDMA_CM_EVENT_ADDR_ERROR:
		case RDMA_CM_EVENT_ROUTE_ERROR:
		case RDMA_CM_EVENT_UNREACHABLE:
		default:
			printk("Unhandled client event %d\n", cm_event->event);
			break;
	}
	return 0;
}

static int __connect_to_server(int nid)
{
	int ret;
	const char *step;
	struct rdma_handle *rh = rdma_handles[nid];
	struct rdma_handle *rh_evic = rdma_handles_evic[nid];

	step = "create rdma id";
	rh->cm_id = rdma_create_id(&init_net,
			cm_client_event_handler, rh, RDMA_PS_IB, IB_QPT_RC);
	if (IS_ERR(rh->cm_id)) 
		goto out_err;

	rh_evic->cm_id = rdma_create_id(&init_net,
			cm_client_event_handler, rh_evic, RDMA_PS_IB, IB_QPT_RC);
	if (IS_ERR(rh_evic->cm_id)) 
		goto out_err;

	step = "resolve server address";
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

		ret = rdma_resolve_addr(rh_evic->cm_id, NULL,
				(struct sockaddr *)&addr, RDMA_ADDR_RESOLVE_TIMEOUT_MS);
		if (ret) 
			goto out_err;
		ret = wait_for_completion_interruptible(&rh_evic->cm_done);
		if (ret || rh_evic->state != RDMA_ADDR_RESOLVED) 
			goto out_err;
	}

	step = "resolve routing path";
	ret = rdma_resolve_route(rh->cm_id, RDMA_ADDR_RESOLVE_TIMEOUT_MS);
	if (ret) 
		goto out_err;
	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (ret || rh->state != RDMA_ROUTE_RESOLVED) 
		goto out_err;

	ret = rdma_resolve_route(rh_evic->cm_id, RDMA_ADDR_RESOLVE_TIMEOUT_MS);
	if (ret) 
		goto out_err;
	ret = wait_for_completion_interruptible(&rh_evic->cm_done);
	if (ret || rh_evic->state != RDMA_ROUTE_RESOLVED) 
		goto out_err;

	/* cm_id->device is valid after the address and route are resolved */
	rh->device = rh->cm_id->device;
	rh_evic->device = rh_evic->cm_id->device;

	step = "setup ib";
	ret = __setup_pd_cq_qp(rh);
	if (ret) 
		goto out_err;

	step = "setup buffers and pools";
	ret = __setup_recv_request(rh);
	if (ret) 
		goto out_err;

	step = "connect";
	{
		struct rdma_conn_param conn_param = {
			.private_data = &my_nid,
			.private_data_len = sizeof(my_nid),
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

		rh_evic->state = RDMA_CONNECTING;
		ret = rdma_connect(rh_evic->cm_id, &conn_param);
		if (ret) 
			goto out_err;
		ret = wait_for_completion_interruptible(&rh_evic->cm_done);
		if (ret) 
			goto out_err;
		if (rh_evic->state != RDMA_CONNECTED) {
			ret = -ETIMEDOUT;
			goto out_err;
		}
	}

	printk(PFX "Connected to %d\n", nid);
	return 0;

out_err:
	printk(KERN_ERR PFX "Unable to %s, %pI4, %d\n", step, ip_table + nid, ret);
	return ret;
}


/****************************************************************************
 * Server-side connection handling
 */
static int __send_request_pool_addr(struct rdma_handles *rh)
{
	struct ib_send_wr wr;
	const struct ib_send_wr *bad_wr;
	struct ib_sge sgl;
	struct request_pool rp = {
		.rkey = rh->mr->rkey;
		.addr = rh->request_buffer_dma_addr;
		.size = rh->size;
	};

	dma_addr = ib_dma_map_single(rh->device, &rp,
			sizeof (struct request_pool, DMA_FROM_DEVICE);
	ret = ib_dma_mapping_error(rh->device, dma_addr);
	if (ret)
		goto err;

	sgl.addr = 

}

static int __accept_client(int nid)
{
	struct rdma_handle *rh = rdma_handles[nid];
	struct rdma_conn_param conn_param = {};
	int ret;

	ret = wait_for_completion_io_timeout(&rh->cm_done, 60 * HZ);
	if (!ret) return -ETIMEDOUT;
	if (rh->state != RDMA_ROUTE_RESOLVED) return -EINVAL;

	ret = __setup_pd_cq_qp(rh);
	if (ret) return ret;

	ret = __setup_buffers_and_pools(rh);
	if (ret) return ret;

	rh->state = RDMA_CONNECTING;
	ret = rdma_accept(rh->cm_id, &conn_param);
	if (ret) return ret;

	ret = wait_for_completion_interruptible(&rh->cm_done);
	if (ret) return ret;

	ret = __send_request_pool_addr(rh);
	if (ret) return ret;

	return 0;
}

static int __on_client_connecting(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	//int peer_nid = *(int *)cm_event->param.conn.private_data;
	static atomic_t num_request = 0;
	struct rdma_handle *rh;

	if (atomic_read(&num_reqeust) == MAX_NUM_NODES)
		return 0;

	*rh = rdma_handles[atomic_inc_return(&num_request)];
	cm_id->context = rh;
	rh->cm_id = cm_id;
	rh->device = cm_id->device;
	rh->state = RDMA_ROUTE_RESOLVED;

	complete(&rh->cm_done);
	return 0;
}

static int __on_client_connected(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	struct rdma_handle *rh = cm_id->context;
	rh->state = RDMA_CONNECTED;
	complete(&rh->cm_done);

	printk(PFX "Connected to %d\n", rh->nid);
	return 0;
}

static int __on_client_disconnected(struct rdma_cm_id *cm_id, struct rdma_cm_event *cm_event)
{
	struct rdma_handle *rh = cm_id->context;
	rh->state = RDMA_INIT;

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
		.sin_addr.s_addr = ip_table[0],
	};

	struct rdma_cm_id *cm_id = rdma_create_id(&init_net,
			cm_server_event_handler, NULL, RDMA_PS_IB, IB_QPT_RC);
	if (IS_ERR(cm_id)) 
		return PTR_ERR(cm_id);
	rdma_handles[0]->cm_id = cm_id;

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

	if (server) {
		ret = __listen_to_connection();
		if (ret) 
			return ret;

		for (;;) {
			if (num_client == MAX_NUM_NODES) {
				printk(KERN_INFO PFX "Cannot accepts no more clients\n");

			if ((ret = __accept_client(++num_client))) 
				return ret;
		}
	}
	else {
		for (i = 1; i < MAX_NUM_NODES + 1; i++) {
			if ((ret = __connect_to_server(i))) 
				return ret;
		}
	}

	printk(PFX "Connections are established.\n");
	return 0;
}

void __exit exit_rmm_rdma(void)
{
	int i;

	/* Detach from upper layer to prevent race condition during exit */

	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh = rdma_handles[i];
		if (!rh) continue;

		if (rh->recv_buffer) {
			ib_dma_unmap_single(rh->device, rh->recv_buffer_dma_addr,
					PCN_KMSG_MAX_SIZE * MAX_RECV_DEPTH, DMA_FROM_DEVICE);
			kfree(rh->recv_buffer);
			kfree(rh->recv_works);
		}

		if (rh->qp && !IS_ERR(rh->qp)) rdma_destroy_qp(rh->cm_id);
		if (rh->cq && !IS_ERR(rh->cq)) ib_destroy_cq(rh->cq);
		if (rh->cm_id && !IS_ERR(rh->cm_id)) rdma_destroy_id(rh->cm_id);

		kfree(rdma_handles[i]);
	}

	/* MR is set correctly iff rdma buffer and pd are correctly allocated */
	if (rdma_mr && !IS_ERR(rdma_mr)) {
		ib_dereg_mr(rdma_mr);
		ib_dma_unmap_single(rdma_pd->device, __rdma_sink_dma_addr,
				1 << (PAGE_SHIFT + MAX_ORDER - 1), DMA_FROM_DEVICE);
		free_pages((unsigned long)__rdma_sink_addr, MAX_ORDER - 1);
		ib_dealloc_pd(rdma_pd);
	}

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
	}

	MSGPRINTK("Popcorn message layer over RDMA unloaded\n");
	return;
}


int __init init_rmm_rdma(void)
{
	int i;

	if (!identify_myself()) 
		return -EINVAL;

	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh;
		rh = rdma_handles[i] = kzalloc(sizeof(struct rdma_handle), GFP_KERNEL);
		if (!rh) 
			goto out_free;

		rh->nid = i;
		rh->state = RDMA_INIT;
		init_completion(&rh->cm_done);
	}
	for (i = 0; i < MAX_NUM_NODES; i++) {
		struct rdma_handle *rh;
		rh = rdma_handles_evic[i] = kzalloc(sizeof(struct rdma_handle), GFP_KERNEL);
		if (!rh) 
			goto out_free;

		rh->nid = i;
		rh->state = RDMA_INIT;
		init_completion(&rh->cm_done);
	}

	if (__establish_connections())
		goto out_free;

	/*
	if (__setup_rdma_buffer(1))
		goto out_free;

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
