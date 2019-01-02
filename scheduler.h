#ifndef SCHEDULER_H
#define SCHEDULER_H
#include "structs.h"

void HistJob(void *arguments);

int SchedulerInit(scheduler** sched, int num_of_threads);

int PushJob(scheduler* sched, int function, void *arguments);

jobqueue_node* PopJob(jobqueue_node** job_queue);

#endif
