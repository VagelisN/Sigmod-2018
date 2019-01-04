#ifndef SCHEDULER_H
#define SCHEDULER_H
#include "structs.h"

int SchedulerInit(scheduler** sched, int num_of_threads);

int PushJob(scheduler* sched, int function, void *arguments);

jobqueue_node* PopJob(scheduler* sched);

void* ThreadFunction(void* arg);

int SchedulerDestroy(scheduler* sched);

void JobDone(scheduler *sched);
void Barrier(scheduler *sched);

#endif
