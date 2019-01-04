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
	if (sched->answers_waiting == 0)
		pthread_cond_signal(&(sched->barrier_cond));
	pthread_mutex_unlock(&(sched->queue_access));
}

void Barrier(scheduler *sched)
{
	pthread_mutex_lock(&(sched->queue_access));
	while(sched->answers_waiting != 0)
		pthread_cond_wait(&(sched->barrier_cond),&(sched->queue_access));

	pthread_mutex_unlock(&(sched->queue_access));
}

int SchedulerDestroy(scheduler* sched)
{
	pthread_mutex_lock(&(sched->queue_access));
	sched->exit_all = 1;

	for (int i = 0; i < sched->num_of_threads; ++i)
		sem_post(&(sched->queue_sem));

	pthread_mutex_unlock(&(sched->queue_access));

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
		sem_wait(&(sched->queue_sem));
	
		if(sched->exit_all == 1)
			break;


		jobqueue_node* job = NULL;
		job = PopJob(sched);

		if (job->function == 0)
			HistJob(job->arguments);
		else if(job->function == 1)
			PartitionJob(job->arguments);
		else if(job->function == 2)
			JoinJob(job->arguments);
		JobDone(sched);
		free(job);
	}
	//printf("Thread %lu exited\n",pthread_self());
	pthread_exit(NULL);
}

