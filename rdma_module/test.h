#ifndef _TEST_H_

#define _TEST_H_

int init_for_test(int nr_nodes);
int dummy_thread(void *args);
void test_throughput(int test_size, int order);
void test_fetch(int order);
int test_evict(void);
void test_read(int order);

#endif

