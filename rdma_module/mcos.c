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

#include "rpc.h"
#include "main.h"
#include "mcos.h"

#ifdef CONFIG_MCOS
extern int (*remote_alloc)(int, u64);
extern int (*remote_free)(int, u64);
extern int (*remote_fetch)(int, void *, void *, unsigned int);
extern int (*remote_evict)(int, struct list_head *, int);

extern int (*remote_alloc_async)(int, u64, unsigned long *);
extern int (*remote_free_async)(int, u64, unsigned long *);
extern int (*remote_fetch_async)(int, void *, void *, unsigned int, unsigned long *);
extern int (*remote_prefetch_async)(int, struct fetch_info *, int);
#endif

extern bool rpc_blocked;

#ifdef LOAD_BAL
static inline int select_fetch_node(int gid)
{
	static int i = 0;
	int size;
	struct node_info *p_info, *s_info;
	int nid;

	p_info = get_node_infos(gid, PRIMARY);
	s_info = get_node_infos(gid, SECONDARY);
	size = p_info->size + s_info->size; 
	if (i % size < p_info->size)
		nid  = p_info->nids[i%size];        
	else
		nid = s_info->nids[i%size - p_info->size];

	i++;
	return nid;
}
#else
static inline int select_fetch_node(int gid)
{
	return get_node_infos(gid,PRIMARY)->nids[0];
}
#endif

static inline int check_rpc_status(void)
{
	if (unlikely(rpc_blocked)) {
		while (rpc_blocked)
			cpu_relax();
	}
}

int mcos_rmm_alloc(int gid, u64 vaddr)
{
	struct node_info *infos;
	int ret;

	check_rpc_status();

retry:
	infos = get_node_infos(gid, PRIMARY);

	ret =  rmm_alloc(infos->nids[0], vaddr - FAKE_PA_START);
	if (ret == -ETIME)
		goto retry;

	return ret;
}

int mcos_rmm_free(int gid, u64 vaddr)
{
	struct node_info *infos;
	int ret;

	check_rpc_status();
retry:	
	infos = get_node_infos(gid, PRIMARY);

	ret = rmm_free(infos->nids[0], vaddr - FAKE_PA_START);
	if (ret == -ETIME)
		goto retry;

	return ret;
}

int mcos_rmm_fetch(int gid, void *l_vaddr, void * r_vaddr, unsigned int order)
{
	int nid, ret;

	check_rpc_status();
retry:
	nid = select_fetch_node(gid);

	ret = rmm_fetch(nid, l_vaddr, r_vaddr - FAKE_PA_START, order);
	if (ret == -ETIME)
		goto retry;

	return ret;
}

int mcos_rmm_evict(int gid, struct list_head *evict_list, int num_page)
{
	struct list_head *l;
	struct node_info *infos;
	int ret;

	check_rpc_status();
retry:
	infos = get_node_infos(gid, PRIMARY);

	list_for_each(l, evict_list) {
		struct evict_info *e = list_entry(l, struct evict_info, next);
		e->r_vaddr -= FAKE_PA_START;
	}

	ret = rmm_evict(infos->nids[0], evict_list, num_page);
	if (ret == -ETIME) 
		goto retry;	

	return ret;
}

int mcos_rmm_evict_fanout(int gid, struct list_head *evict_list, int num_page)
{
	struct list_head *l;
	struct node_info *infos;
	int ret = 0;
	int i;
	int *done;

	check_rpc_status();

	infos = get_node_infos(gid, PRIMARY);
	done = kmalloc(infos->size * sizeof(int), GFP_ATOMIC);
	if (!done)
		return -ENOMEM;
	memset(done, 0, infos->size * sizeof(int));

	list_for_each(l, evict_list) {
		struct evict_info *e = list_entry(l, struct evict_info, next);
		e->r_vaddr -= FAKE_PA_START;
	}

	for (i = 0; i < infos->size; i++)  {
		ret = rmm_evict_async(infos->nids[i], evict_list, num_page, &done[i]);
		if (ret < 0)
			goto err;
	}

	for (i = 0; i < infos->size; i++)  {
		ret = wait_for_ack_timeout(&done[i], 20000);
		if (ret < 0)
			goto err;
	}

err:
	kfree(done);
	return ret;
}

int mcos_rmm_alloc_async(int gid, u64 vaddr, unsigned long *rpage_flags)
{
	struct node_info *infos; 

	check_rpc_status();

	infos = get_node_infos(gid, PRIMARY);

	return rmm_alloc_async(infos->nids[0], vaddr - FAKE_PA_START, rpage_flags);
}

int mcos_rmm_free_async(int gid, u64 vaddr, unsigned long *rpage_flags)
{
	struct node_info *infos;

	check_rpc_status();

	infos = get_node_infos(gid, PRIMARY);

	return rmm_free_async(infos->nids[0], vaddr - FAKE_PA_START, rpage_flags);
}

int mcos_rmm_fetch_async(int gid, void *l_vaddr, void * r_vaddr, unsigned int order, unsigned long *rpage_flags)
{
	int nid; 

	check_rpc_status();

	nid = select_fetch_node(gid);

	return rmm_fetch_async(nid, l_vaddr, r_vaddr - FAKE_PA_START, order, rpage_flags);
}

// TODO
int mcos_rmm_prefetch_async(int gid, struct fetch_info *fi_array, int num_page)
{
	int nid;

	check_rpc_status();

	nid = select_fetch_node(gid);
	return rmm_prefetch_async(nid, fi_array, num_page);
}

#ifdef CONFIG_MCOS
void init_mcos(void)
{
	remote_fetch = mcos_rmm_fetch;
	remote_evict = mcos_rmm_evict;
	remote_alloc = mcos_rmm_alloc;
	remote_free = mcos_rmm_free;
	remote_fetch_async = mcos_rmm_fetch_async;
	remote_prefetch_async = mcos_rmm_prefetch_async;
}
#else
void init_mcos(void)
{
	return;
}
#endif

