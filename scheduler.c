#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include "preprocess.h"
#include "rhjoin.h"
#include "scheduler.h"

int SchedulerInit(scheduler** sched, int num_of_threads)
{
	(*sched) = malloc(sizeof(scheduler));
	(*sched)-> thread_array = malloc(num_of_threads * sizeof(pthread_t));
	(*sched)->num_of_threads = num_of_threads;
	(*sched)->job_queue = NULL;

	pthread_mutex_init(&((*sched)->queue_access), NULL);
	pthread_cond_init(&((*sched)->barrier_cond), NULL);
	sem_init (&((*sched)->queue_sem),0,0);

	(*sched)->exit_all = 0;
	(*sched)->answers_waiting = 0;

	for (int i = 0; i < num_of_threads; ++i)
		pthread_create(&((*sched)->thread_array[i]),NULL,ThreadFunction,(void*)(*sched));
}

int PushJob(scheduler* sched, int function, void *arguments)
{
	pthread_mutex_lock(&(sched->queue_access));
	if( (sched->job_queue) == NULL)
	{
		(sched->job_queue) = malloc (sizeof(jobqueue_node));
		(sched->job_queue)->function = function;
		(sched->job_queue)->arguments = arguments;
		(sched->job_queue)->next = NULL;
	}

	// Insert at end
	else
	{
		jobqueue_node* temp = (sched->job_queue);
		while( temp->next != NULL)
		{
			temp = temp->next;
		}

		temp->next = malloc (sizeof(jobqueue_node));
		temp->next->function = function;
		temp->next->arguments = arguments;
		temp->next->next = NULL;
	}
	// signal a thread that waits at the semaphore to take the job
	sem_post(&(sched->queue_sem));

	pthread_mutex_unlock(&(sched->queue_access));
}

jobqueue_node* PopJob(scheduler* sched)
{
	pthread_mutex_lock(&(sched->queue_access));

	jobqueue_node* temp = sched->job_queue;
	sched->job_queue = sched->job_queue->next;

	pthread_mutex_unlock(&(sched->queue_access));
	return temp;
}

void JobDone(scheduler *sched)
{
	pthread_mutex_lock(&(sched->queue_access));

	sched->answers_waiting--;
	//if this was the last job signal the barier condition
	if (sched->answers_waiting == 0)
		pthread_cond_signal(&(sched->barrier_cond));

	pthread_mutex_unlock(&(sched->queue_access));
}

void Barrier(scheduler *sched)
{

	pthread_mutex_lock(&(sched->queue_access));
	// waits until all the jobs are done. 
	// the thread that finishes the last job will signal the barrier cond
	while(sched->answers_waiting != 0)
		pthread_cond_wait(&(sched->barrier_cond),&(sched->queue_access));

	pthread_mutex_unlock(&(sched->queue_access));
}

int SchedulerDestroy(scheduler* sched)
{
	// when the threads pass the semaphore will see exit_all and exit
	sched->exit_all = 1;

	// wake all the threads waiting at the semaphore
	for (int i = 0; i < sched->num_of_threads; ++i)
		sem_post(&(sched->queue_sem));

	// wait for all the threads to exit
	for (int i = 0; i < sched->num_of_threads; i++)
		pthread_join(sched->thread_array[i], NULL);//wait for workers to shutdown

	pthread_mutex_destroy(&(sched->queue_access));
	pthread_cond_destroy(&(sched->barrier_cond));
	sem_destroy(&(sched->queue_sem));

	free(sched->thread_array);
	free(sched);
}

void* ThreadFunction(void* arg)
{
	scheduler* sched = (scheduler*)arg;
	while(1)
	{
		// wait until a job is available at the queue of the scheduler
		sem_wait(&(sched->queue_sem));

		// all jobs are done exit
		if(sched->exit_all == 1)
			break;


		jobqueue_node* job = NULL;

		//get a job from the queue 
		job = PopJob(sched);

		switch(job->function)
		{
			case 0:
				HistJob(job->arguments);
				break;
			case 1:
				PartitionJob(job->arguments);
				break;
			case 2:
				JoinJob(job->arguments);
				break;
		}
		JobDone(sched);
		free(job);
	}
	pthread_exit(NULL);
}
