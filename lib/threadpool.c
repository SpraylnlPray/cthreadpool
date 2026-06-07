#define _GNU_SOURCE
#include "threadpool.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <pthread.h>

struct thread_info
{
    pthread_t thread_handle;
    int thread_has_work;
    int joined;
    int exited;
    int thread_idx;
    struct threadpool *pool;
};

struct threadpool
{
    struct thread_info *tinfos;
    struct workload *workloads;
    pthread_mutex_t workload_mutex;
    pthread_cond_t sig_wakeup;
    int next_workload_idx;
    int workload_fill_idx;
    int num_threads;
    int joining;
};

struct workload
{
    void *(*func)(void *);
    void *arg;
};

static void *worker_thread(void *arg);

int add_workload(void * hThreadpool, void *(*func)(void*), void *arg)
{
    if (!hThreadpool || !func || !arg)
        return -EINVAL;

    struct threadpool *threadpool = (struct threadpool*)hThreadpool;
    pthread_mutex_lock(&threadpool->workload_mutex);

    if (threadpool->workload_fill_idx == (threadpool->next_workload_idx - 1))
    {
        // threadpool is full, either grow or (for now) return error
        return -ENOMEM;
    }

    threadpool->workloads[threadpool->workload_fill_idx].arg = arg;
    threadpool->workloads[threadpool->workload_fill_idx].func = func;
    threadpool->workload_fill_idx++;
    threadpool->workload_fill_idx %= threadpool->num_threads;

    pthread_cond_signal(&threadpool->sig_wakeup);
    pthread_mutex_unlock(&threadpool->workload_mutex);

    return 0;
}

void wait_for_join(void * hThreadpool)
{
    if (!hThreadpool)
        return;

    struct threadpool *threadpool = (struct threadpool*)hThreadpool;
    int joined_threads = 0;
    threadpool->joining = 1;
    while (joined_threads != threadpool->num_threads)
    {
        for (int i = 0; i < threadpool->num_threads; i++)
        {
            if (threadpool->tinfos[i].joined || threadpool->tinfos[i].thread_has_work)
                continue;

            pthread_cond_signal(&threadpool->sig_wakeup);
            if (!threadpool->tinfos[i].exited)
                continue;

            pthread_join(threadpool->tinfos[i].thread_handle, NULL);
            threadpool->tinfos[i].joined = 1;
            joined_threads++;
        }
    }
}

int setup_threadpool(void * hThreadpool, size_t num_threads)
{
    int res = 0;
    if (!hThreadpool || num_threads == 0)
    {
        res = -EINVAL;
        goto out;
    }

    struct threadpool *threadpool = (struct threadpool*)hThreadpool;
    threadpool->next_workload_idx = 0;
    threadpool->workload_fill_idx = 0;
    threadpool->joining = 0;

    struct thread_info *tinfos = (struct thread_info *)calloc(num_threads, sizeof(struct thread_info));
    if (tinfos == NULL)
    {
        res = -ENOMEM;
        goto out_cleanup_tinfos;
    }

    struct workload *workloads = (struct workload *)calloc(num_threads, sizeof(struct workload));
    if (workloads == NULL)
    {
        res = -ENOMEM;
        goto out_cleanup_workloads;
    }

    res = pthread_cond_init(&threadpool->sig_wakeup, NULL);
    if (res != 0)
        goto out_cleanup_threads;

    res = pthread_mutex_init(&threadpool->workload_mutex, NULL);
    if (res != 0)
        goto out_cleanup_mutex;

    for (int tidx = 0; tidx < num_threads; tidx++)
    {
        tinfos[tidx].thread_idx = tidx;
        tinfos[tidx].thread_has_work = 0;
        tinfos[tidx].pool = threadpool;
        tinfos[tidx].exited = 0;

        res = pthread_create(&tinfos[tidx].thread_handle, NULL, &worker_thread, &tinfos[tidx]);
        if (res)
            goto out_cleanup_threads;

        threadpool->num_threads++;

        char tname[16]; // according to manpage the threadname can be max 16 characters
        res = snprintf(tname, sizeof(tname), "Worker %d", tidx);
        if (res != 0)
            pthread_setname_np(tinfos[tidx].thread_handle, tname);

        workloads[tidx].arg = NULL;
        workloads[tidx].func = NULL;
    }

    threadpool->tinfos = tinfos;
    threadpool->workloads = workloads;

    res = 0;
    goto out;

out_cleanup_threads:
    wait_for_join(threadpool);

out_cleanup_mutex:
    pthread_cond_destroy(&threadpool->sig_wakeup);

out_cleanup_workloads:
    if (workloads)
    {
        free(workloads);
        threadpool->workloads = NULL;
    }

out_cleanup_tinfos:
    if (tinfos)
    {
        free(tinfos);
        threadpool->tinfos = NULL;
    }

out:
    return res;
}

void *worker_thread(void *arg)
{
    if (arg == NULL)
    {
        return NULL;
    }

    struct thread_info *tinfo = (struct thread_info *)arg;
    struct threadpool *pool = tinfo->pool;

    while (1)
    {
        pthread_mutex_lock(&pool->workload_mutex);
        while (pool->workload_fill_idx == pool->next_workload_idx || pool->joining)
        {
            if (pool->joining)
            {
                pthread_mutex_unlock(&pool->workload_mutex);
                goto out;
            }
            pthread_cond_wait(&pool->sig_wakeup, &pool->workload_mutex);
        }

        void *(*func)(void *) = pool->workloads[pool->next_workload_idx].func;
        void *arg = pool->workloads[pool->next_workload_idx].arg;
        pool->next_workload_idx++;
        pool->next_workload_idx %= pool->num_threads;

        pthread_mutex_unlock(&pool->workload_mutex);
        tinfo->thread_has_work = 1;
        func(arg);
        tinfo->thread_has_work = 0;
    }

out:
    tinfo->exited = 1;
    return 0;
}