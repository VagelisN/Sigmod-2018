#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include "preprocess.h"
#include "scheduler.h"
#include "structs.h"
#include "rhjoin.h"

relation* ToRow(int** original_array, int row_to_join, relation* new_rel)
{
	for (int i = 0; i < new_rel->num_tuples; ++i)
	{
		new_rel->tuples[i].value = original_array[i][row_to_join];
		new_rel->tuples[i].row_id = i;
	}
	return new_rel;
}


void ReorderArray(relation* rel_array, int n_lsb, reordered_relation** new_rel, scheduler *sched)
{
	//Check the arguments
	if ((rel_array == NULL) || (n_lsb <= 0))
	{
		printf("Error in ReorderArray. Invalid arguments\n");
		exit(1);
	}
	int hist_size = 1;
	for (int i = 0; i < n_lsb; ++i)
		hist_size *= 2;

	fprintf(stderr, "Starting HistJobs\n");
	sched->answers_waiting = sched->num_of_threads;

	int **histograms = malloc(sched->num_of_threads * sizeof(int *));
	for (size_t i = 0; i < sched->num_of_threads; i++)
	  histograms[i] = NULL;

	//Split up the num_tuples so each thread gets the same
	int tuples_per_thread = (rel_array->num_tuples) / sched->num_of_threads;

	//For each thread
	for (size_t i = 0; i < sched->num_of_threads; i++)
	{
	  //Set the arguments
	  hist_arguments *arguments = malloc(sizeof(hist_arguments));
	  arguments->hist = &histograms[i];
	  arguments->n_lsb = n_lsb;
	  arguments->hist_size = hist_size;
	  arguments->rel = rel_array;
	  arguments->start = i * tuples_per_thread;
	  //If we are setting up the last thread, end is the end of the relation
	  if(i != sched->num_of_threads - 1)arguments->end = (i+1) * tuples_per_thread;
	  else arguments->end = rel_array->num_tuples;

	  //Enqueue it in the scheduler
	  PushJob(sched, 0,(void*)arguments);
	}

	//Wait for all threads to finish building their work(barrier)
	pthread_mutex_lock(&(sched->queue_access));
	pthread_cond_wait(&(sched->barrier_cond),&(sched->queue_access));
	pthread_mutex_unlock(&(sched->queue_access));

	fprintf(stderr, "Hist jobs finished\n");


	//Allocate the Hist and Psum arrays
	int64_t *Psum = malloc(hist_size * sizeof(int64_t));
	uint64_t *Hist = calloc(hist_size, sizeof(uint64_t));
	memset(Psum, -1 , hist_size * sizeof(int64_t));

	//Build the whole Histogram from the
	for (size_t i = 0; i < sched->num_of_threads; i++)
	  for (size_t j = 0; j < hist_size; j++)
	    Hist[j] += histograms[i][j];
	//Free allocated space
	for (size_t i = 0; i < sched->num_of_threads; i++)
	  free(histograms[i]);
	free(histograms);

	//Build the Psum array
	int new_start = 0;
	for (int i = 0; i < hist_size; ++i)
	{
	  /*If the current bucket has 0 values allocated to it then leave
	   *psum[CurrentBucket][1] to -1.	*/
	  if (Hist[i] > 0)
	  {
	    Psum[i] = new_start;
	    //temp_psum[i] = new_start;
	    new_start += Hist[i];
	  }
	}
	//for (size_t i = 0; i < hist_size; i++) {
	//	fprintf(stderr, "Psum[%3lu]: %ld\n", i, Psum[i]);
	//}
	//exit(1);

	/*--------------------Build the reordered array----------------------*/

	//Allocate space for the ordered array in new_rel variable
	(*new_rel) = malloc(sizeof(reordered_relation));
	(*new_rel)->rel_array = malloc(sizeof(relation));
	(*new_rel)->rel_array->num_tuples = rel_array->num_tuples;
	(*new_rel)->rel_array->tuples = calloc(rel_array->num_tuples , sizeof(tuple));

	(*new_rel)->hist_size = hist_size;
	(*new_rel)->hist = Hist;
	(*new_rel)->psum = Psum;

	//Set up the arguments
	fprintf(stderr, "Starting PartitionJobs\n");
	sched->answers_waiting = sched->num_of_threads;

	//int64_t **tempPsum = malloc(thread_num * sizeof(int64_t*));
	for (size_t i = 0; i < sched->num_of_threads; i++)
	{
		int64_t *tempPsum = malloc(hist_size * sizeof(int64_t));
		part_arguments *arguments = malloc(sizeof(part_arguments));
		arguments->reordered = (*new_rel)->rel_array;
		arguments->hist_size = hist_size;
		arguments->original = rel_array;
		arguments->n_lsb = n_lsb;
		//tuples_per_thread is the same for both hist_jobs and partition_jobs
		arguments->start = i * tuples_per_thread;
		memcpy(tempPsum, Psum, hist_size * sizeof(int64_t));
		arguments->psum = tempPsum;
	  //If we are setting up the last thread, end is the end of the relation
	  if(i != sched->num_of_threads - 1)arguments->end = (i+1) * tuples_per_thread;
	  else arguments->end = rel_array->num_tuples;

		//Enqueue it in the scheduler
		PushJob(sched, 1, (void*) arguments);
	}


	//wait for all jobs to finish
	//Wait for all threads to finish building their work(barrier)
	pthread_mutex_lock(&(sched->queue_access));
	pthread_cond_wait(&(sched->barrier_cond),&(sched->queue_access));
	pthread_mutex_unlock(&(sched->queue_access));
	fprintf(stderr, "PartitionJobs jobs finished\n");
	/*for (size_t i = 0; i < rel_array->num_tuples; i++) {
		printf("Reordered[%2lu]: %lu\n", i, (*new_rel)->rel_array->tuples[i].row_id);
	}*/

}


void FreeReorderRelation(reordered_relation *rel)
{
	if (rel->hist_size > 0)
	{
		free(rel->psum);
		free(rel->hist);
	}
	if (rel->rel_array != NULL)
	{
		if (rel->rel_array->tuples != NULL)free(rel->rel_array->tuples);
		free(rel->rel_array);
	}
	free(rel);
}

void FreeRelation(relation *rel)
{
	if (rel == NULL) return;
	free(rel->tuples);
	free(rel);
}

void HistJob(void *arguments)
{
  hist_arguments *args = arguments;

  //Allocate memory for the thread's histogram and set each value to 0.
  (*args->hist) = calloc(args->hist_size, sizeof(int));

  for (size_t i = args->start; i < args->end; i++) {
    uint64_t hashed_value = HashFunction1(args->rel->tuples[i].value, args->n_lsb);
    (*args->hist)[hashed_value]++;
  }
	free(args);
	//fprintf(stderr, "\n\nFinished HistJob\n\n\n" );
  return;
}

void PartitionJob(void* args)//int start, int end, int size, int* Psum, int modulo, int **reordered, int *original)
{
	part_arguments *arguments = (part_arguments*)args;

	int current_bucket;
	//Find the starting bucket
	for (int i = 0; i < arguments->hist_size; ++i)
	{
		//printf("Start: %d, psum[%d]: %d\n", arguments->start, i, arguments->psum[i]);
		if (arguments->psum[i] == -1) continue;
		if (arguments->start == arguments->psum[i])
		{
			current_bucket = i;
			break;
		}
		if(arguments->start < arguments->psum[i])
		{
			//fprintf(stderr, "In start: %lu < psum[%d]: %lu\n", arguments->start, i, arguments->psum[i]);
			int j = i - 1;
			//Find the previous active bucket
			while(arguments->psum[j] == -1)j--;
			current_bucket = j;
			break;
		}
	}
	//printf("Start: %d and Psum[%d] = %d\n", start, current_bucket, Psum[current_bucket]);
	//printf("end - start= %d\n", end - start);
	//To skip_Values den ypologizetai swsta
	int skip_values = arguments->start - arguments->psum[current_bucket];
	//fprintf(stderr, "Start = %lu , skip_values = %d , CurrentBucket = %d, hist_size = %lu\n", arguments->start, skip_values, current_bucket, arguments->hist_size);
	arguments->psum[current_bucket] = arguments->start;
	int previous_encounter = 0;
	/* This thread is responsible to find all the correct values from start to end.*/
	for (int i = arguments->start; i < arguments->end; ++i)
	{
		//printf("i = [%d], current_bucket = %d\n", i, current_bucket);
		//printf("hist_size: %d\n", arguments->hist_size);
		for (int j = previous_encounter; j < arguments->original->num_tuples; ++j)
		{
			int hash_value = HashFunction1(arguments->original->tuples[j].value, arguments->n_lsb);
			//printf("HV: %d| CB: %d\n", hash_value,current_bucket);
			//If the hash value is the one we are looking for
			if (hash_value == current_bucket)
			{
				//If we don't have any more values to skip
				if(skip_values-- > 0)continue;
				//printf("Inserting %lu to %d position\n", arguments->original->tuples[j].row_id, i);
				arguments->reordered->tuples[i].value = arguments->original->tuples[j].value;
				arguments->reordered->tuples[i].row_id = arguments->original->tuples[j].row_id;
				arguments->psum[current_bucket]++;
				previous_encounter = j + 1;
				//If its the last bucket skip the check of bucket change
				if(current_bucket == arguments->hist_size - 1)break;
				if (arguments->psum[current_bucket] >= arguments->psum[current_bucket + 1])
				{
					previous_encounter = 0;
					current_bucket++;
				}
				break;
			}
		}
	}
	free(arguments->psum);
	free(arguments);
	//fprintf(stderr, "\n\nFinished PartitionJob\n\n\n\n");
	return;
}
