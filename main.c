#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#define NUM_THREADS 10

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
    printf("Hello from thread #%d with ID %lu\n", tinfo->thread_num, tinfo->thread_id);

    return (void*)(long long)tinfo->thread_num;
}

int main()
{
    printf("Hello World\n");
    struct thread_info *tinfo = calloc(NUM_THREADS, sizeof(struct thread_info));
    if (tinfo == NULL)
    {
        printf("Failed to allocate memory for thread info array: %s", strerror(errno));
        return -1;
    }

    printf("Start creation of threads\n");

    for (int i = 0; i < NUM_THREADS; i++)
    {
        tinfo[i].thread_num = i;
        int res = pthread_create(&tinfo[i].thread_id, NULL, thread_start, &tinfo[i]);
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
        int ret = -1;
        int res = pthread_join(tinfo[i].thread_id, (void*)&ret);
        if (res != 0)
        {
            printf("Failed to join thread #%d: %s\n", i, strerror(errno));
            return -1;
        }

        printf("Joined thread #%d with ID %lu, return value was %d\n", ret);
    }

    free(tinfo);
    return 0;
}