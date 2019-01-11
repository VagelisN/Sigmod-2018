#ifndef SCHEDULER_H
#define SCHEDULER_H
#include "structs.h"

/*
 * Initializes a scheduler struct. it creates num_of_threads threads,
 * initializes the condition variable the mutex and the semaphore.
 */
int SchedulerInit(scheduler** sched, int num_of_threads);

/*
 * Pushes a job to the queue of the scheduler. It locks the mutex
 * queue_access before pushing the job and unlocks it afterwards.
 */
int PushJob(scheduler* sched, void *function, void *arguments);

/*
 * Used by the threads in ThreadFunction. locks the mutex
 * queue access pops a job from the job queue of the scheduler 
 * and unlocks the mutex.
 */
jobqueue_node* PopJob(scheduler* sched);

/*
 * Function used by a thread in ThreadFunction when a job is done.
 * it decreases the answers_waiting variable of the scheduler
 * and if answers_waiting drops to zero it wakes the barrier condition 
 */
void JobDone(scheduler *sched);

/*
 * When all jobs have been pushed to the queue of the scheduler
 * this function waits at the barrier condition varriable until
 * all threads have finished all the jobs.
 */
void Barrier(scheduler *sched);

/*
 * The threads wait at the semaphore queue_sem until either
 * a job is available or all jobs are done and they need to exit
 * if there jobs are available threads take a job using PushJob
 * else they free their allocated space and exit.
 */
void* ThreadFunction(void* arg);

/*
 * Function that terminates all the threads and
 * frees the allocated space of the scheduler
 */
int SchedulerDestroy(scheduler* sched);

#endif
