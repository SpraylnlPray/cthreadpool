#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#define NUM_THREADS 20

struct thread_info {
    pthread_t thread_id;
    int thread_num;
};

static void* thread_start(void *arg)
{
    if (arg == NULL)
    {
        printf("Thread wasn't passed any info\n");
        return 0;
    }

    struct thread_info *tinfo = (struct thread_info*)arg;
    volatile unsigned long long counter = 0;

    printf("Hello from thread #%d\n", tinfo->thread_num);
    while (1)
    {
        counter++;  // prevent optimization

        // Optional: print occasionally so you see it's alive
        if (counter % 10000000000ULL == 0)
        {
            break;
        }
    }
    printf("Thread #%d finished\n", tinfo->thread_num);

    return (void*)(long long)0;
}

int main()
{
    printf("Hello World\n");
    pthread_attr_t attr;
    struct thread_info *tinfo = calloc(NUM_THREADS, sizeof(struct thread_info));
    if (tinfo == NULL)
    {
        printf("Failed to allocate memory for thread info array: %s", strerror(errno));
        return -1;
    }

    int res = pthread_attr_init(&attr);
    if (res != 0)
    {
        printf("Failed to initialize pthread_attr: %s\n", strerror(errno));
        return -1;
    } 

    struct sched_param s_param;
    res = pthread_attr_getschedparam(&attr, &s_param);
    if (res != 0)
    {
        printf("Failed to get sched param: %s\n", strerror(errno));
        exit -1;
    }

    int s_policy;
    res = pthread_attr_getschedpolicy(&attr, &s_policy);
    if (res != 0)
    {
        printf("Failed to get sched policy: %s\n", strerror(errno));
        exit -1;
    }

    res = pthread_attr_destroy(&attr);
    if (res != 0)
    {
        printf("Failed to destroy attribute: %s\n", strerror(res));
        return -1;
    }

    printf("Start creation of threads\n");

    for (int i = 0; i < NUM_THREADS; i++)
    {
        res = pthread_attr_init(&attr);
        if (res != 0)
        {
            printf("Thread #d failed to initialize attribute: %s\n", i, strerror(res));
            return -1;   
        }

        if (i == 7)
        {
            res = pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
            if (res != 0)
            {
                printf("Failed to set explicit scheduling: %s\n", strerror(res));
                return -1;
            }

            printf("Setting priority to %d and policy to %d\n", 55, SCHED_FIFO);
            res = pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
            if (res != 0)
            {
                printf("Failed to set sched_policy: %s\n", strerror(res));
                return -1;
            }
            s_param.sched_priority = 55;
            res = pthread_attr_setschedparam(&attr, &s_param);
            if (res != 0)
            {
                printf("Failed to set sched_priority: %s\n", strerror(res));
                return -1;
            }
        }

        tinfo[i].thread_num = i;
        int res = pthread_create(&tinfo[i].thread_id, &attr, thread_start, &tinfo[i]);
        if (res != 0)
        {
            printf("Failed to create thread: %s\n", strerror(errno));
            return -1;
        }
    }

    printf("Finished creating threads, joining them now...\n");
    for (int i = 0; i < NUM_THREADS; i++)
    {
        printf("Joining thread #%d\n", i);
        long long ret = -1;
        int res = pthread_join(tinfo[i].thread_id, (void*)&ret);
        if (res != 0)
        {
            printf("Failed to join thread #%d: %s\n", i, strerror(errno));
            return -1;
        }

        printf("Joined thread #%d, return value was %d\n", i, ret);
    }

    free(tinfo);
    return 0;
}