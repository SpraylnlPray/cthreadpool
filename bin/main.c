#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>

#include "threadpool.h"


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

int hThreadpool;
long long int arg1 = 1;
void add_workload_handler(int signum)
{
    add_workload(&hThreadpool, &test1, (void*)arg1);
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
    int num_threads = 20;
    if (setup_threadpool(&hThreadpool, num_threads) != 0)
    {
        printf("Failed to setup thread pool\n");
        return -1;
    }

    printf("Initialized thread pool with %d workers\n", num_threads);

    handle_incoming();

    wait_for_join(&hThreadpool);

    return 0;
}