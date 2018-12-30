#include <pthread.h>

typedef struct jobqueue_node
{
  int function;
  void *arguments;
  struct jobqueue_node* next;
}jobqueue_node;

typedef struct scheduler
{
	int num_of_threads;
	pthread_t *thread_array;
	jobqueue_node* job_queue;

	pthread_mutex_t barrier_mutex;
	pthread_cond_t barrier_cond;
	pthread_mutex_t queue_access;
	pthread_cond_t empty;

	int active_jobs;
	int exit_all;
	int answers_waiting;

	
}scheduler;

int scheduler_init(scheduler** sched, int num_of_threads); 

int push_job(scheduler* sched, int function, void *arguments);

jobqueue_node* pop_job(jobqueue_node** job_queue);