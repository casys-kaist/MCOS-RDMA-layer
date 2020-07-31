#ifndef __RMM_H__
#define __RMM_H__

#define RDMA_PORT 11453
#define RDMA_ADDR_RESOLVE_TIMEOUT_MS 5000

#define IMM_DATA_SIZE 4 /* bytes */
#define RPC_ARGS_SIZE 16 /* bytes */

#define DMA_BUFFER_SIZE		(PAGE_SIZE * 8192)

#define RDMA_SLOT_SIZE	(PAGE_SIZE * 2)
//#define NR_RPC_SLOTS	(RPC_BUFFER_SIZE / RPC_ARGS_SIZE)
#define NR_RDMA_SLOTS	(5000)
//#define NR_SINK_SLOTS	(SINK_BUFFER_SIZE / PAGE_SIZE)
#define MAX_RECV_DEPTH	(NR_RDMA_SLOTS + 5)
#define MAX_SEND_DEPTH	(NR_RDMA_SLOTS + 5)

#define NR_WORKER_THREAD 1

#define ACC_CPU_ID 13
#define POLL_CPU_ID 14 
#define WORKER_CPU_ID 15 

#define QP_FETCH	0
#define QP_EVICT	1

#define PFX "rmm: "
#define DEBUG_LOG if (debug) printk
//#define RMM_TEST

#define FAKE_PA_START 0x20000000000UL

#define MAX_TICKS 1000

#ifdef CONFIG_RM
#define DMA_BUFFER_START (RM_PADDR_START + RM_PADDR_SIZE)
#else 
#define DMA_BUFFER_START (_AC(1, UL) << 36) /* 64GB */
#endif

//#define CONFIG_MCOS_IRQ_LOCK

/* rpage flags */
#define RPAGE_PREFETCHED        0x00000001
#define RPAGE_EVICTED           0x00000002
#define RPAGE_PREFETCHING       0x00000004
#define RPAGE_EVICTING          0x00000008
#define RPAGE_ALLOCATING        0x00000010
#define RPAGE_ALLOCED           0x00000020
#define RPAGE_FREEZE_FAIL       0x00000040
#define RPAGE_ALLOC_FAILED      0x00000080
#define RPAGE_FREED             0x00000100
#define RPAGE_FREE_FAILED       0x00000200
#define RPAGE_FETCHED           0x00000400

enum connection_type {
	PRIMARY,
	SYNC,
	ASYNC,
};

struct connection_info {
	int nid;
	enum connection_type c_type;
};

enum wr_type {
	WORK_TYPE_REG,
	WORK_TYPE_RPC_ADDR,
	WORK_TYPE_SINK_ADDR,
	WORK_TYPE_IMM,
	WORK_TYPE_SEND_ADDR,
	WORK_TYPE_RPC_REQ,
	WORK_TYPE_RPC_ACK,
};

struct recv_work {
	enum wr_type work_type;
	struct rdma_handle *rh;
	struct ib_sge sgl;
	struct ib_recv_wr wr;
};

struct recv_work_addr {
	enum wr_type work_type;
	struct rdma_handle *rh;
	struct ib_sge sgl;
	struct ib_recv_wr wr;

	dma_addr_t dma_addr;
	void * addr;
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
	struct rdma_handle *rh;
	struct rdma_work *next;
	struct ib_sge sgl;
	struct ib_rdma_wr wr;
};

struct worker_thread {
	struct list_head work_head;
	int num_queued;
	struct task_struct *task;
	spinlock_t lock_wt;

	/* debug */
	unsigned long delay;
	unsigned long num_handled;
	unsigned long total_queued;
	int cpu;
};

struct args_worker {
	struct rdma_handle *rh;
	uint32_t imm_data;

	unsigned long time_enqueue_ns;
	unsigned long time_dequeue_ns;

	struct list_head next;
};

struct pool_info {
	u32 rkey;
	dma_addr_t addr;
	size_t size;
};

struct evict_info {
	u64 l_vaddr;
	u64 r_vaddr;

	struct list_head next;
};

#pragma pack(push, 1)
struct conn_private_data {
	unsigned int qp_type : 1;
	unsigned int c_type : 3;
	unsigned int nid : 28;
};
#pragma pack(pop)

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
	struct completion init_done;
	struct recv_work *recv_works;

	int qp_type;
	enum connection_type c_type;

	/* local */
	void *recv_buffer;
	void *dma_buffer;
	u64 vaddr_start;

	/* point to dma_buffer */
	void *rpc_buffer;
	void *sink_buffer;
	void *evict_buffer;

	dma_addr_t recv_buffer_dma_addr;
	dma_addr_t dma_addr;

	/* point to dma_buffer */
	dma_addr_t rpc_dma_addr;
	dma_addr_t sink_dma_addr;
	dma_addr_t evict_dma_addr;

	struct rdma_work *rdma_work_head;
	struct rdma_work *rdma_work_pool;
	spinlock_t rdma_work_head_lock;

	size_t recv_buffer_size;
	size_t rpc_buffer_size;

	/* remote */
	dma_addr_t remote_dma_addr;
	dma_addr_t remote_rpc_dma_addr;
	size_t remote_rpc_size;
	u32 rpc_rkey;
	/*************/

	struct ring_buffer *rb;
	struct ring_buffer *rb_sink;

	struct rdma_cm_id *cm_id;
	struct ib_device *device;
	struct ib_cq *cq;
	struct ib_qp *qp;
	struct ib_mr *mr;
};

static inline int nid_to_rh(int nid)
{
	return nid * 2;
}



/* prototype of symbol */
/*
   int ib_dereg_mr_user(struct ib_mr *mr);
   int ib_destroy_cq_user(struct ib_cq *cq);
   void ib_dealloc_pd_user(struct ib_pd *pd);
   struct ib_mr *ib_alloc_mr_user(struct ib_pd *pd,
   enum ib_mr_type mr_type,
   u32 max_num_sg);

   static inline int ib_dereg_mr_dummy(struct ib_mr *mr)
   {
   return ib_dereg_mr_user(mr);
   }

   static inline int ib_destroy_cq_dummy(struct ib_cq *cq)
   {
   return ib_destroy_cq_user(cq);
   }

   struct ib_mr *ib_alloc_mr_dummy(struct ib_pd *pd,
   enum ib_mr_type mr_type,
   u32 max_num_sg)
   {
   return ib_alloc_mr_user(pd, mr_type, max_num_sg);
   }

   void ib_dealloc_pd_dummy(struct ib_pd *pd)
   {
   return ib_dealloc_pd_user(pd);
   }
 */

#endif