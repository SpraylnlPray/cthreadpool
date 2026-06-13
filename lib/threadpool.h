#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <unistd.h>

int setup_threadpool(void *hThreadpool, size_t num_threads, void (*on_worker_finished)(void));
void wait_for_join(void *hThreadpool);
int add_workload(void *hThreadpool, void *(*func)(void*), void *arg);

#endif