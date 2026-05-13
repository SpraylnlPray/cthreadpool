#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#define NUM_THREADS 20
#define EOK 0

struct thread_info
{
    pthread_t thread_handle;
    int thread_has_work;
    size_t thread_idx;
    struct thread_pool *pool;
};

struct workload
{
    void *(*func)(void*);
    void *arg;
};

struct thread_pool
{
    struct thread_info *tinfos;
    struct workload *workloads;
    pthread_mutex_t workload_mutex;
    pthread_cond_t sig_workload_available;
    size_t next_workload_idx;
    size_t workload_fill_idx;
};

void *worker_thread(void *arg)
{
    if (arg == NULL)
    {
        printf("Worker was not passed any argument\n");
        return 0;
    }

    struct thread_info *tinfo = (struct thread_info*)arg;
    struct thread_pool *pool = tinfo->pool;

    printf("Worker ready!\n");
    while (1)
    {
        pthread_mutex_lock(&pool->workload_mutex);
        while (pool->workload_fill_idx == pool->next_workload_idx)
        {
            printf("Worker #%d going to sleep\n", tinfo->thread_idx);
            pthread_cond_wait(&pool->sig_workload_available, &pool->workload_mutex);
            printf("Worker #%d woke up, fill_idx is %d, next_idx is %d\n", tinfo->thread_idx, pool->workload_fill_idx, pool->next_workload_idx);
        }
        
        void *(*func)(void*) = pool->workloads[pool->next_workload_idx].func;
        void *arg = pool->workloads[pool->next_workload_idx].arg;
        pool->next_workload_idx++;
        printf("Worker #%d next_idx after incrementing is %d\n", pool->next_workload_idx);
        pool->next_workload_idx %= NUM_THREADS;
        printf("Worker #%d next_idx after mod is %d\n", pool->next_workload_idx);

        pthread_mutex_unlock(&pool->workload_mutex);
        printf("Worker #%d starting work on function\n");
        func(arg);
        printf("Worker #%d finished work on function\n");

    }
    printf("Worker #%d finished\n", tinfo->thread_idx);
    return 0;
}

int add_workload(struct thread_pool *pool, void *(*func)(void*), void *arg)
{
    if (!pool || !func || !arg)
        return EINVAL;

    pthread_mutex_lock(&pool->workload_mutex);

    if (pool->workload_fill_idx == (pool->next_workload_idx - 1))
    {
        // pool is full, either grow or (for now) return error
        return -1;
    }

    printf("add_workload adding item\n");
    pool->workloads[pool->workload_fill_idx].arg = arg;
    pool->workloads[pool->workload_fill_idx].func = func;
    pool->workload_fill_idx++;
    printf("add_workload fill_idx after incrementing is %d\n", pool->workload_fill_idx);
    pool->workload_fill_idx %= NUM_THREADS;
    printf("add_workload fill_idx after mod is %d, will signal now\n", pool->workload_fill_idx);

    pthread_cond_signal(&pool->sig_workload_available);

    printf("add_workload, unlock mutex\n");
    pthread_mutex_unlock(&pool->workload_mutex);
}

int setup_thread_pool(struct thread_pool *pool, size_t num_threads)
{
    int res = EOK;
    if (!pool || num_threads == 0)
    {
        res = EINVAL;
        goto out;
    }

    struct thread_info *tinfos = (struct thread_info*)calloc(num_threads, sizeof(struct thread_info));
    if (tinfos == NULL)
    {
        printf("setup_thread_pool failed to allocate memory for threads\n");
        res = ENOMEM;
        goto out_cleanup_tinfos;
    }

    struct workload *workloads = (struct workload*)calloc(num_threads, sizeof(struct workload));
    if (workloads == NULL)
    {
        printf("setup_thread_pool failed to allocate memory for workloads\n");
        res = ENOMEM;
        goto out_cleanup_workloads;
    }

    size_t tidx;
    for (tidx = 0; tidx < num_threads; tidx++)
    {
        tinfos[tidx].thread_idx = tidx;
        res = pthread_create(&tinfos[tidx].thread_handle, NULL, &worker_thread, &tinfos[tidx]);
        if (res != EOK)
        {
            printf("Failed to create thread #%d: %s\n", strerror(res));
            goto out_cleanup_threads;
        }

        tinfos[tidx].thread_handle = 0;
        tinfos[tidx].thread_has_work = 0;
        tinfos[tidx].pool = pool;

        workloads[tidx].arg = NULL;
        workloads[tidx].func = NULL;
    }

    res = pthread_cond_init(&pool->sig_workload_available, NULL);
    if (res != EOK)
    {
        printf("Failed to initialized cvar: %s\n", strerror(res));
        goto out_cleanup_threads;
    }

    res = pthread_mutex_init(&pool->workload_mutex, NULL);
    if (res != EOK)
    {
        printf("Failed to initialize mutex: %s\n", strerror(res));
        goto out_cleanup_mutex;
    }

    pool->tinfos = tinfos;
    pool->workloads = workloads;
    pool->next_workload_idx = 0;
    pool->workload_fill_idx = 0;

    res = EOK;
    goto out;

out_cleanup_mutex:
    pthread_cond_destroy(&pool->sig_workload_available);

out_cleanup_threads:
    for (size_t i = 0; i < tidx; i++)
    {
        // TODO: Cleanup threads on error
    }

out_cleanup_workloads:
    if (workloads)
    {
        free(workloads);
        pool->workloads = NULL;
    }

out_cleanup_tinfos:
    if (tinfos)
    {
        free(tinfos);
        pool->tinfos = NULL;
    }

out:
    return res;
}

void wait_for_join(struct thread_pool *pool, size_t num_threads)
{
    printf("Wait for join\n");
    void *res;
    for (size_t i = 0; i < num_threads; i++)
    {
        printf("Awaiting worker #%d\n", i);
        pthread_join(pool->tinfos[i].thread_handle, NULL);
        printf("Worker #%d joined\n", i);
    }
}

void *test1(void *arg)
{
    long long int iarg = (long long int)arg;
    printf("This is test1, argument is %d\n", iarg);
    volatile unsigned long long counter = 0;
    while (1)
    {
        counter++;
        if (counter % 10000000ULL == 0)
            printf("test1 still running\n");
        if (counter % 10000000000ULL == 0)
        {
            printf("test1 did %llu iterations\n", counter);
            break;
        }
    }

    printf("test1 finished: %llu\n", counter);
    return 0;
}

// TODO: Support cli argument for number of threads
int main(void)
{
    int num_threads = NUM_THREADS;
    struct thread_pool pool;
    if (setup_thread_pool(&pool, num_threads) != EOK)
    {
        printf("Failed to setup thread pool\n");
        return -1;
    }

    printf("Initialized thread pool with %d workers\n", num_threads);

    long long int arg1 = 1;
    add_workload(&pool, &test1, (void*)arg1);

    wait_for_join(&pool, num_threads);

    return 0;
}