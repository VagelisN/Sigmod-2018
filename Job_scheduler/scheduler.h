#include <pthread.h>
#include "structs.h"


int scheduler_init(scheduler** sched, int num_of_threads); 

int push_job(scheduler* sched, int function, void *arguments);

jobqueue_node* pop_job(jobqueue_node** job_queue);