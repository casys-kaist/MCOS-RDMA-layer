#ifndef __RMM_H__
#define __RMM_H__

#define RDMA_PORT 11453
#define RDMA_ADDR_RESOLVE_TIMEOUT_MS 5000

#define IMM_DATA_SIZE 4 /* bytes */

#define DMA_BUFFER_SIZE		(PAGE_SIZE * 8192)

#define RDMA_SLOT_SIZE	(PAGE_SIZE * 2)
#define NR_RDMA_SLOTS	(5000)
#define MAX_RECV_DEPTH	(NR_RDMA_SLOTS + 5)
#define MAX_SEND_DEPTH	(NR_RDMA_SLOTS + 5)
#define NR_RESPONDER_RESOURCES 16

#define NR_WORKER_THREAD 1

#define ACC_CPU_ID 13
#define POLL_CPU_ID 14 
#define WORKER_CPU_ID 15 

#define MEM_GID 0 /* memory server only has a single group */
#define MAX_GROUP_SIZE 5

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

#define PADDR_SIZE ((1UL << 30) * 64)

#define CONFIG_MCOS_RMM_PREFETCH

//#define CONFIG_MCOS_IRQ_LOCK
#define CONFIG_MCOS_WO_RPAGE_LOCK_RMM

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

#define MCOS_MAX_PREFETCH_SIZE	32 // FIXME

#ifdef CONFIG_RM
enum remote_page_flags {
	RP_PREFETCHED   = 0,
	RP_EVICTED      = 1,
	RP_PREFETCHING  = 2,
	RP_EVICTING     = 3,
	RP_ALLOCATING   = 4,
	RP_ALLOCED      = 5,
	RP_FREEZE_FAIL  = 6,
	RP_ALLOC_FAILED = 7,
	RP_FREED        = 8,
	RP_FREE_FAILED  = 9,
	RP_FETCHED      = 10, 
	RP_LOCK         = 11, 
	RP_HIT          = 12, 
	RP_IN_BUF       = 13, 
	RP_EVER_EVICTED = 14, 
};
#endif

enum connection_type {
	PRIMARY, /* accept all types of RPCs */
	SECONDARY, /* only accept fetch */
	BACKUP_SYNC,
	BACKUP_ASYNC,
	NUM_CTYPE,
};

struct node_info {
	int nids[MAX_GROUP_SIZE];
	int size;
};

struct connection_config {
	int nid;
	int gid;
	enum connection_type ctype;
}; 

struct connection_info {
	struct node_info infos[NUM_CTYPE];
};

extern struct connection_info c_infos[];

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
	unsigned long *rpage_flags;
	u64 raddr;
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

#ifdef CONFIG_RM
struct evict_info {
	u64 l_vaddr;
	u64 r_vaddr;

	struct list_head next;
};
#endif

#pragma pack(push, 1)
struct conn_private_data {
	unsigned int qp_type : 1;
	unsigned int c_type : 3;
	unsigned int nid : 28;
};
#pragma pack(pop)

#ifdef CONFIG_RM
struct fetch_info {
	u64 l_vaddr;	// virtual address to be received
	u64 r_vaddr;	// virtual address (fake pa)
	unsigned long *rpage_flags;

	//struct list_head next;
};
#endif

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
	dma_addr_t mem_dma_addr;
	dma_addr_t evict_dma_addr;

	struct rdma_work *rdma_work_head;
	struct rdma_work *rdma_work_pool;
	spinlock_t rdma_work_head_lock;

	size_t recv_buffer_size;
	size_t rpc_buffer_size;

	/* remote */
	dma_addr_t remote_dma_addr;
	dma_addr_t remote_mem_dma_addr;
	dma_addr_t remote_rpc_dma_addr;
	size_t remote_rpc_size;
	u32 rpc_rkey;
	u32 mem_rkey;
	/*************/

	atomic_t pending_reads;

	struct ring_buffer *rb;

	struct rdma_cm_id *cm_id;
	struct ib_device *device;
	struct ib_cq *cq;
	struct ib_qp *qp;
	struct ib_mr *mr;
	struct ib_mr *mem_mr;
};

static inline int nid_to_rh(int nid)
{
	return nid * 2;
}

static inline struct node_info *get_node_infos(int gid, enum connection_type ctype)
{
	struct node_info *ret_nid;
	extern spinlock_t cinfos_lock;
	
	if (ctype >= NUM_CTYPE)
		return NULL;

	spin_lock(&cinfos_lock);
	ret_nid = &c_infos[gid].infos[ctype];
	spin_unlock(&cinfos_lock);
		
	return ret_nid;
}

static inline int add_node_to_group(int gid, int nid, enum connection_type ctype)
{
	struct node_info *infos = get_node_infos(gid, ctype);
	extern spinlock_t cinfos_lock;

	if (infos->size == MAX_GROUP_SIZE)
		return -ENOMEM;

	spin_lock(&cinfos_lock);
	infos->nids[infos->size] = nid;
	infos->size++;
	spin_unlock(&cinfos_lock);
	
	return 0;
}

static inline int remove_node_from_group(int gid, int nid, enum connection_type ctype)
{
	int i;
	struct node_info *infos = get_node_infos(gid, ctype);
	extern spinlock_t cinfos_lock;

	spin_lock(&cinfos_lock);
	for (i = 0; i < infos->size; i++) {
		if (infos->nids[i] == nid) {
			memcpy(&infos->nids[i], &infos->nids[i+1], 
					sizeof(infos->nids[0] * (MAX_GROUP_SIZE - 1 - i)));
		}

	}
	infos->size--;
	spin_unlock(&cinfos_lock);

	return 0;
}

static inline int wait_for_ack_timeout(int *done, u64 ticks)
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

static inline void __put_rdma_work_nonsleep(struct rdma_handle *rh, struct rdma_work *rw)
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

static inline void rmm_yield_cpu(void)
{
	cond_resched();
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
