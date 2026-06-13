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

struct workload
{
    void *(*func)(void *);
    void *arg;
};

struct threadpool
{
    struct thread_info *tinfos;
    struct workload next_workload;
    pthread_mutex_t workload_mutex;
    pthread_cond_t sig_wakeup;
    int num_threads;
    int joining;
    void (*on_worker_finished)(void);
};

static void *worker_thread(void *arg);

static void on_worker_finished(struct threadpool *threadpool)
{
    if (!threadpool->on_worker_finished)
        return;
    threadpool->on_worker_finished();
}

int add_workload(void * hThreadpool, void *(*func)(void*), void *arg)
{
    int ret = 0;
    if (!hThreadpool || !func || !arg)
    {
        ret = -EINVAL;
        goto out;
    }

    struct threadpool *threadpool = (struct threadpool*)hThreadpool;
    pthread_mutex_lock(&threadpool->workload_mutex);

    int has_available_thread = 0;
    for (int i = 0 ; i < threadpool->num_threads; i++)
    {
        if (!threadpool->tinfos[i].thread_has_work)
        {
            has_available_thread = 1;
            break;
        }
    }
    if (!has_available_thread)
    {
        ret = -ENOMEM;
        goto out_unlock;
    }

    threadpool->next_workload.arg = arg;
    threadpool->next_workload.func = func;

    pthread_cond_signal(&threadpool->sig_wakeup);

out_unlock:
    pthread_mutex_unlock(&threadpool->workload_mutex);

out:
    return ret;
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

int setup_threadpool(void *hThreadpool, size_t num_threads, void (*on_worker_finished)(void))
{
    int res = 0;
    if (!hThreadpool || num_threads == 0)
    {
        res = -EINVAL;
        goto out;
    }

    struct threadpool *threadpool = (struct threadpool*)hThreadpool;
    threadpool->joining = 0;
    threadpool->next_workload.arg = NULL;
    threadpool->next_workload.func = NULL;
    threadpool->on_worker_finished = on_worker_finished;

    struct thread_info *tinfos = (struct thread_info *)calloc(num_threads, sizeof(struct thread_info));
    if (tinfos == NULL)
    {
        res = -ENOMEM;
        goto out_cleanup_tinfos;
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
    }

    threadpool->tinfos = tinfos;

    res = 0;
    goto out;

out_cleanup_threads:
    wait_for_join(threadpool);

out_cleanup_mutex:
    pthread_cond_destroy(&threadpool->sig_wakeup);

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
        return NULL;

    struct thread_info *tinfo = (struct thread_info *)arg;
    struct threadpool *pool = tinfo->pool;

    while (1)
    {
        pthread_mutex_lock(&pool->workload_mutex);
        while ((pool->next_workload.func == NULL && pool->next_workload.arg == NULL) || pool->joining)
        {
            if (pool->joining)
            {
                pthread_mutex_unlock(&pool->workload_mutex);
                goto out;
            }
            pthread_cond_wait(&pool->sig_wakeup, &pool->workload_mutex);
        }

        void *(*func)(void *) = pool->next_workload.func;
        void *arg = pool->next_workload.arg;
        pool->next_workload.func = NULL;
        pool->next_workload.arg = NULL;

        tinfo->thread_has_work = 1;
        pthread_mutex_unlock(&pool->workload_mutex);

        func(arg);

        pthread_mutex_lock(&pool->workload_mutex);
        tinfo->thread_has_work = 0;
        pthread_mutex_unlock(&pool->workload_mutex);
        on_worker_finished(pool);
    }

out:
    tinfo->exited = 1;
    return 0;
}