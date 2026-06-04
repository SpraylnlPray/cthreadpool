#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>

#define NUM_THREADS 20
#define EOK 0

struct thread_info
{
    pthread_t thread_handle;
    int thread_has_work;
    int joined;
    int exited;
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
    pthread_cond_t sig_wakeup;
    size_t next_workload_idx;
    size_t workload_fill_idx;
    int joining;
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

    printf("Worker #%d ready\n", tinfo->thread_idx);
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
            printf("Worker #%d going to sleep, sig address is 0x%x\n", tinfo->thread_idx, &pool->sig_wakeup);
            pthread_cond_wait(&pool->sig_wakeup, &pool->workload_mutex);
            printf("Worker #%d woke up, fill_idx is %d, next_idx is %d\n", tinfo->thread_idx, pool->workload_fill_idx, pool->next_workload_idx);
        }
        
        void *(*func)(void*) = pool->workloads[pool->next_workload_idx].func;
        void *arg = pool->workloads[pool->next_workload_idx].arg;
        pool->next_workload_idx++;
        printf("Worker #%d next_idx after incrementing is %d\n", tinfo->thread_idx, pool->next_workload_idx);
        pool->next_workload_idx %= NUM_THREADS;
        printf("Worker #%d next_idx after mod is %d\n", tinfo->thread_idx, pool->next_workload_idx);

        pthread_mutex_unlock(&pool->workload_mutex);
        printf("Worker #%d starting work on function\n", tinfo->thread_idx);
        tinfo->thread_has_work = 1;
        func(arg);
        tinfo->thread_has_work = 0;
        printf("Worker #%d finished work on function\n", tinfo->thread_idx);
    }

out:
    tinfo->exited = 1;
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

    pthread_cond_signal(&pool->sig_wakeup);

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

    pool->next_workload_idx = 0;
    pool->workload_fill_idx = 0;
    pool->joining = 0;

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

    res = pthread_cond_init(&pool->sig_wakeup, NULL);
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

    size_t tidx;
    for (tidx = 0; tidx < num_threads; tidx++)
    {
        tinfos[tidx].thread_idx = tidx;
        tinfos[tidx].thread_has_work = 0;
        tinfos[tidx].pool = pool;
        tinfos[tidx].exited = 0;

        res = pthread_create(&tinfos[tidx].thread_handle, NULL, &worker_thread, &tinfos[tidx]);
        if (res != EOK)
        {
            printf("Failed to create thread #%d: %s\n", strerror(res));
            goto out_cleanup_threads;
        }

        char tname[16]; // according to manpage the threadname can be max 16 characters
        res = snprintf(tname, sizeof(tname), "Worker %d\0", tidx);
        if (!res)
        {
            printf("Failed to format threadname: %s\n", strerror(res));
        }
        res = pthread_setname_np(tinfos[tidx].thread_handle, tname);
        if (res != EOK)
        {
            printf("Failed to create thread #%d: %s\n", strerror(res));
        }

        workloads[tidx].arg = NULL;
        workloads[tidx].func = NULL;
    }

    pool->tinfos = tinfos;
    pool->workloads = workloads;

    res = EOK;
    goto out;

out_cleanup_mutex:
    pthread_cond_destroy(&pool->sig_wakeup);

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
    int joined_threads = 0;
    pool->joining = 1;
    while (joined_threads != num_threads)
    {
        for (size_t i = 0; i < num_threads; i++)
        {
            if (pool->tinfos[i].joined || pool->tinfos[i].thread_has_work)
                continue;
            
            pthread_cond_signal(&pool->sig_wakeup);
            if (!pool->tinfos[i].exited)
                continue;

            pthread_join(pool->tinfos[i].thread_handle, NULL);
            pool->tinfos[i].joined = 1;
            printf("Worker #%d joined\n", i);
            joined_threads++;
        }
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
        if (counter % 10000000000ULL == 0)
        {
            printf("test1 did %llu iterations\n", counter);
            break;
        }
    }

    printf("test1 finished: %llu\n", counter);
    return 0;
}

int cancel = 0;
void set_cancel_flag(int signum)
{
    cancel = 1;
}

struct thread_pool pool;
long long int arg1 = 1;
void add_workload_handler(int signum)
{
    add_workload(&pool, &test1, (void*)arg1);
}

// simulate some sort of workload that accepts incoming traffic and hands it over to the workers
int handle_incoming(void)
{
    struct sigaction sigint_action = {0};
    sigint_action.sa_handler = &set_cancel_flag;
    if (sigaction(SIGINT, &sigint_action, NULL))
    {
        printf("Failed to set up SIGINT handler\n");
        return -1;
    }

    struct sigaction add_workload_action = {0};
    add_workload_action.sa_handler = &add_workload_handler;
    if (sigaction(SIGUSR1, &add_workload_action, NULL))
    {
        printf("Failed to set up SIGUSR1 handler\n");
        return -1;
    }

    while (cancel == 0) {}

    return 0;
}

// TODO: Support cli argument for number of threads
int main(void)
{
    int num_threads = NUM_THREADS;
    if (setup_thread_pool(&pool, num_threads) != EOK)
    {
        printf("Failed to setup thread pool\n");
        return -1;
    }

    printf("Initialized thread pool with %d workers\n", num_threads);

    handle_incoming();

    wait_for_join(&pool, num_threads);

    return 0;
}