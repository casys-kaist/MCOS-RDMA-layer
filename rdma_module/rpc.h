#ifndef __RPC_H__

#define __RPC_H__

#include "main.h"

typedef int (*Rpc_handler)(struct rdma_handle *, uint32_t);

enum rpc_opcode {
	RPC_OP_FETCH,
	RPC_OP_EVICT,
	RPC_OP_ALLOC,
	RPC_OP_FREE,
	RPC_OP_RECOVERY,
	NUM_RPC,
};

void regist_handler(Rpc_handler rpc_table[]);

#pragma pack(push, 1)
struct rpc_header {
	int nid;
	enum rpc_opcode op;
	bool async;
};

union rpc_tail {
	u64 rpage_flags;
	int done;
};

struct fetch_args {
	u64 r_vaddr;
	u32 order;
};

struct fetch_aux {
	u64 l_vaddr;
	union {
		u64 rpage_flags;
		int done;
	} async;
};

struct mem_args {
	u64 vaddr;
};

struct mem_aux {
	union  {
		u64 rpage_flags;
		int done;
	} async;
};
#pragma pack(pop)


int rmm_alloc(int nid, u64 vaddr);
int rmm_alloc_async(int nid, u64 vaddr, unsigned long *rpage_flags);
int rmm_free(int nid, u64 vaddr);
int rmm_free_async(int nid, u64 vaddr, unsigned long *rpage_flags);
int rmm_fetch(int nid, void *l_vaddr, void * r_vaddr, unsigned int order);
int rmm_fetch_async(int nid, void *l_vaddr, void * r_vaddr, unsigned int order, unsigned long *rpage_flags);
int rmm_evict(int nid, struct list_head *evict_list, int num_page);
int rmm_evict_async(int nid, struct list_head *evict_list, int num_page, int *done);
int rmm_evict_forward(int nid, void *src_buffer, int payload_size, int *done);
int rmm_recovery(int nid);

#endif
