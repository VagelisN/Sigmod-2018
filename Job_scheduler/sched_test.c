#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include "scheduler.h"

int main(void)
{
	scheduler* sched = NULL;
	scheduler_init(&sched,4);
	void* args = NULL;

	sched->answers_waiting = 16;

	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);
	push_job(sched, 0,args);

	pthread_mutex_lock(&(sched->queue_access));
	printf("barriered\n");
	pthread_cond_wait(&(sched->barrier_cond),&(sched->queue_access));

	pthread_mutex_unlock(&(sched->queue_access));


	pthread_mutex_lock(&(sched->queue_access));
	sched->exit_all = 1;
	pthread_cond_broadcast(&(sched->empty));
	pthread_mutex_unlock(&(sched->queue_access));

	for (int i = 0; i < sched->num_of_threads; i++)
		pthread_join(sched->thread_array[i], NULL);//wait for workers to shutdown

	printf("father exited\n");
	return 0;
}