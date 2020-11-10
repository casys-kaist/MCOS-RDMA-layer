#include <linux/kthread.h>
#include <linux/module.h>
#include <linux/random.h>
#include <linux/atomic.h>
#include <linux/io.h>
#include <linux/string.h>
#include <linux/kernel.h>
#include <linux/sched_clock.h>
#include <linux/jiffies.h>

#include <rdma/rdma_cm.h>
#include <rdma/ib_verbs.h>

#include "rpc.h"
#include "main.h"

extern int debug;
char **my_data;

#ifdef RMM_TEST
int init_for_test(int nr_nodes)
{
	int i;

	my_data = kmalloc(sizeof (char *) * nr_nodes, GFP_KERNEL);
	if (!my_data)
		return -ENOMEM;
	for (i = 0; i < nr_nodes; i++) {
		my_data[i] = kmalloc(PAGE_SIZE * 1024, GFP_KERNEL);
		if (!my_data[i])
			return -1;
	}

	return 0;
}
#else
int init_for_test(int nr_nodes)
{
	return 0;
}
#endif

/*buffer for test */

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

int dummy_thread(void *args)
{
	int i;
	int nid = (int) args;
	//const int fetch_size = 4096 * 16;
	const int fetch_size = 4096;
	const int boundary = (4 * (1 << 20)) / fetch_size;
	unsigned long rpage_flags;

	i = 0;
	while (1)  {
		if (kthread_should_stop())
			return 0;
		rpage_flags = 0;
		rmm_read(nid, my_data[nid] + (i * fetch_size), (void *) (i * fetch_size), 0, &rpage_flags);
		while (!test_bit(RP_FETCHED, &rpage_flags))
			cpu_relax();

		i = (i + 1) % boundary;
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
		rmm_fetch(nid, my_data[nid] + (random_index1[i] * (PAGE_SIZE * num_page)), 
				(void *) (random_index2[i] * (PAGE_SIZE * num_page)), wi->order);
	}

	if (atomic_inc_return(&num_done) == wi->test_size) {
		complete(wi->done);
	}

	return 0;
}

void test_throughput(int test_size, int order)
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

	printk(KERN_INFO PFX "test size: %d, elapsed(ns) %llu\n", test_size, elapsed);


out_free:
	for (i = 0; i < test_size; i++) {
		if (random_index1[i])
			kfree(random_index1[i]);
		if (random_index2[i])
			kfree(random_index2[i]);
	}
}

void test_fetch(int order)
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

	elapsed = 0;
	printk(PFX "fetch start\n");
	getnstimeofday(&start_tv);
	for (i = 0; i < 512; i++) {
		rmm_fetch(0, my_data[0] + (i * size), (void *) (i * size), order);
		printk(PFX "fetched data %s\n", my_data[0] + (i * size));
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

int test_evict(void)
{
	int i, j, num;
	uint16_t index;
	struct timespec start_tv, end_tv;
	unsigned long elapsed, total;
	struct evict_info *ei;
	struct list_head *pos, *n;
	LIST_HEAD(addr_list);

	elapsed = 0;
	total = 0;
	printk(PFX "evict start\n");
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

void test_read(int order)
{
	int i;
	int num_page, size;
	struct timespec start_tv, end_tv;
	unsigned long elapsed;
	unsigned long *flags;

	num_page = (1 << order);
	size = PAGE_SIZE * num_page;

	flags = kmalloc(sizeof(unsigned long) * 512, GFP_KERNEL);
	memset(flags, 0, sizeof(unsigned long) * 512);

	elapsed = 0;
	DEBUG_LOG(PFX "read start\n");
	getnstimeofday(&start_tv);
	for (i = 0; i < 512; i++) {
		rmm_read(1, my_data[1] + (i * size), (void *) (i * size), order, &flags[i]);
	}

	for (i = 0; i < 512; i++) {
		while (!test_bit(RP_FETCHED, &flags[i]))
			cpu_relax();
		DEBUG_LOG(PFX "fetched data %s\n", my_data[1] + (i * size));
	}
	getnstimeofday(&end_tv);
	elapsed = (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
		(end_tv.tv_nsec - start_tv.tv_nsec);

	printk(KERN_INFO PFX "total elapsed time %lu (ns)\n", elapsed);
	printk(KERN_INFO PFX "average elapsed time %lu (ns)\n", elapsed / 512);

	kfree(flags);
}

int test_write(void)
{
	int i, num;
	struct timespec start_tv, end_tv;
	unsigned long elapsed, total;
	unsigned long *rpage_flags;

	num = 1024;
	rpage_flags = kzalloc(sizeof(unsigned long) * num, GFP_KERNEL);
	if (!rpage_flags) {
		printk(PFX "fail to alloc in %s\n", __func__);
		return -ENOMEM;
	}

	elapsed = 0;
	total = 0;
	printk(PFX "evict start\n");
	for (i = 0; i < 1024; i++) {
		sprintf(my_data[0] + (i * PAGE_SIZE), "This is %d", i);

	}

	getnstimeofday(&start_tv);
	for (i = 0; i < num; i++) {
		rmm_write(1, my_data[0] + (i * PAGE_SIZE), (void *) (i * PAGE_SIZE), &rpage_flags[i]);

	}
	getnstimeofday(&end_tv);
	elapsed += (end_tv.tv_sec - start_tv.tv_sec) * 1000000000 +
			(end_tv.tv_nsec - start_tv.tv_nsec);

	for (i = 0; i < num; i++) {
		while (!test_bit(RP_EVICTED, &rpage_flags[i]))
			cpu_relax();
	}

	printk(KERN_INFO PFX "average elapsed time(evict) %lu (ns)\n", elapsed / num);


	return 0;
}

int test_write_read(void)
{
	int i;
	u64 dest;
	char test_string[] = "qqwweerrttyy";
	unsigned long rpage_flags;

	sprintf(my_data[0], test_string);
	for (i = 0; i < PADDR_SIZE / PAGE_SIZE; i++) {
		dest = ((u64) i) * PAGE_SIZE;
		printk(PFX "dest: %llX\n", dest);

		rpage_flags = 0;
		rmm_write(1, my_data[0], (void *) dest, &rpage_flags);
		while (!test_bit(RP_EVICTED, &rpage_flags))
			cpu_relax();

		rpage_flags = 0;
		sprintf(my_data[1], "aassddffgghh");
		rmm_read(1, my_data[1], (void *) dest, 0, &rpage_flags);
		while (!test_bit(RP_FETCHED, &rpage_flags))
			cpu_relax();

		if (memcmp(my_data[0], my_data[1], PAGE_SIZE)) {
			printk(PFX "data miss match\n");
			return -1;
		}

		rmm_yield_cpu();
	}

	return 0;
}
