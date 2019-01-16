#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include "preprocess.h"
#include "scheduler.h"
#include <unistd.h>
#include "structs.h"
#include "rhjoin.h"
#include "results.h"

void ReorderArray(relation* rel_array, reordered_relation** new_rel, scheduler *sched)
{
	int hist_size = 1;
	for (int i = 0; i < N_LSB; ++i)
		hist_size *= 2;

	// the number of hist jobs is equal to the number of threads
	sched->answers_waiting = sched->num_of_threads;

	uint64_t **histograms = calloc(sched->num_of_threads , sizeof(uint64_t *));

	// Split up the num_tuples so each thread gets the same
	int tuples_per_thread = (rel_array->num_tuples) / sched->num_of_threads;

	// For each thread
	for (size_t i = 0; i < sched->num_of_threads; i++)
	{
	  // Set the arguments
	  hist_arguments *arguments = malloc(sizeof(hist_arguments));
	  arguments->hist = &histograms[i];
	  arguments->n_lsb = N_LSB;
	  arguments->hist_size = hist_size;
	  arguments->rel = rel_array;
	  arguments->start = i * tuples_per_thread;

	  // If we are setting up the last thread, end is the end of the relation
	  if(i != sched->num_of_threads - 1)arguments->end = (i+1) * tuples_per_thread;
	  else arguments->end = rel_array->num_tuples;

	  // Enqueue it in the scheduler
	  PushJob(sched, &HistJob,(void*)arguments);
	}

	// Allocate the Hist and Psum arrays
	int64_t *Psum = malloc(hist_size * sizeof(int64_t));
	uint64_t *Hist = calloc(hist_size, sizeof(uint64_t));
	memset(Psum, -1 , hist_size * sizeof(int64_t));

	// Wait for all threads to finish building their work(barrier)
	Barrier(sched);

	// Build the whole Histogram from the
	short int flag = 1;
	int new_start = 0;
	for (size_t i = 0; i < hist_size; ++i)
	{
		for (size_t j = 0; j < sched->num_of_threads; ++j)
			Hist[i] += histograms[j][i];
		if (Hist[i] > 0)
	  {
			flag = 0;
	    Psum[i] = new_start;
	    new_start += Hist[i];
	  }
	}
	if (flag == 1)
	{
		(*new_rel) = NULL;
		return;
	}

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
	sched->answers_waiting = sched->num_of_threads;

	for (size_t i = 0; i < sched->num_of_threads; i++)
	{
		part_arguments *arguments = malloc(sizeof(part_arguments));
		arguments->reordered = (*new_rel)->rel_array;
		arguments->hist_size = hist_size;
		arguments->original = rel_array;
		arguments->n_lsb = N_LSB;

		// tuples_per_thread is the same for both hist_jobs and partition_jobs
		arguments->start = i * tuples_per_thread;
		arguments->psum = Psum;

	  // If we are setting up the last thread, end is the end of the relation
	  if(i != sched->num_of_threads - 1)arguments->end = (i+1) * tuples_per_thread;
	  else arguments->end = rel_array->num_tuples;

		// Enqueue it in the scheduler
		PushJob(sched, &PartitionJob, (void*) arguments);
	}

	// Free allocated space
	for (size_t i = 0; i < sched->num_of_threads; i++)
	  free(histograms[i]);
	free(histograms);

	// Wait for all threads to finish building their work(barrier)
	Barrier(sched);
}

void HistJob(void *arguments)
{
  hist_arguments *args = arguments;

  // Allocate memory for the thread's histogram and set each value to 0.
	uint64_t **hist = args->hist;
  (*hist) = calloc(args->hist_size , sizeof(uint64_t));

  for (size_t i = args->start; i < args->end; i++) {
    uint64_t hashed_value = HashFunction1(args->rel->tuples[i].value, args->n_lsb);
    (*hist)[hashed_value]++;
  }
	free(args);
  return;
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



void PartitionJob(void* args)// int start, int end, int size, int* Psum, int modulo, int **reordered, int *original)
{
	part_arguments *arguments = (part_arguments*)args;
	int64_t current_bucket = -3;

	// Find the starting bucket
	for (int i = 0; i < arguments->hist_size; ++i)
	{
		if (arguments->psum[i] == -1) continue;
		if (arguments->start == arguments->psum[i])
		{
			current_bucket = i;
			break;
		}
		if(arguments->start < arguments->psum[i])
		{
			int j = i - 1;

			// Find the previous active bucket
			while(arguments->psum[j] == -1)j--;
			current_bucket = j;
			break;
		}
	}

	// If no value has been assigned to current_bucket then find the only active node
	if (current_bucket == -3)
	{
		for (size_t i = 0; i < arguments->hist_size; i++) {
			if (arguments->psum[i] != -1){
				 current_bucket = i;
				 break;
			 }
		}
	}
	int skip_values = arguments->start - arguments->psum[current_bucket];
	uint64_t checked_values = skip_values;
	int previous_encounter = 0;

	/* This thread is responsible to find all the correct values from start to end.*/
	for (int i = arguments->start; i < arguments->end; ++i)
	{
		for (int j = previous_encounter; j < arguments->original->num_tuples; ++j)
		{
			int hash_value = HashFunction1(arguments->original->tuples[j].value, arguments->n_lsb);
			// If the hash value is the one we are looking for
			if (hash_value == current_bucket)
			{
				// If we don't have any more values to skip
				if(skip_values-- > 0)continue;
				arguments->reordered->tuples[i].value = arguments->original->tuples[j].value;
				arguments->reordered->tuples[i].row_id = arguments->original->tuples[j].row_id;
				checked_values++;
				previous_encounter = j + 1;

				// Find the next active bucket
				short int flag = 0;
				for (size_t iter = current_bucket+1; iter < arguments->hist_size; iter++)
				{
					if(arguments->psum[iter] == -1)continue;
					if (arguments->psum[iter] <= (arguments->psum[current_bucket] + checked_values))
					{
						current_bucket = iter;
						previous_encounter = 0;
						checked_values = 0;
						flag = 1;
					}
					// When the first active bucket is encountered, break
					break;
				}
				// Ιf we didn't change bucket, then continue, else break
				break;
			}
		}
	}
	free(arguments);
	return;
}


void SerialReorderArray(relation* rel_array, reordered_relation** new_rel)
{
	int i = 0;
	uint64_t hashed_value = 0;

	(*new_rel) = malloc(sizeof(reordered_relation));
	//Find the size of the psum and the hist arrays
	(*new_rel)->hist_size = 1;
	for (i = 0; i < N_LSB; ++i)
		(*new_rel)->hist_size *= 2;

	// Allocate space for the hist and psum arrays
	(*new_rel)->psum = malloc((*new_rel)->hist_size * sizeof(int64_t));
	(*new_rel)->hist = calloc((*new_rel)->hist_size , sizeof(uint64_t));
	//temp_psum array is used only in this function for faster reordering of the array
	int64_t* temp_psum = malloc((*new_rel)->hist_size * sizeof(int64_t));

	memset((*new_rel)->psum, -1, (*new_rel)->hist_size * sizeof(int64_t));
	memset(temp_psum, -1, (*new_rel)->hist_size * sizeof(int64_t));

	//Run rel_array with the hash function and build the histogram
	for (i = 0; i < rel_array->num_tuples; ++i)
	{
		hashed_value = HashFunction1((int32_t) rel_array->tuples[i].value, N_LSB);
		(*new_rel)->hist[hashed_value]++;
	}

	//Build the psum array using the histogram
	int NewStartingPoint = 0;
	for (i = 0; i < (*new_rel)->hist_size; ++i)
	{
		/*If the current bucket has 0 values allocated to it then leave
		 *psum[CurrentBucket][1] to -1.	*/
		if ((*new_rel)->hist[i] > 0)
		{
			(*new_rel)->psum[i] = NewStartingPoint;
			temp_psum[i] = NewStartingPoint;
			NewStartingPoint += (*new_rel)->hist[i];
		}
	}

	/*--------------------Build the reordered array----------------------*/

	//Allocate space for the ordered array in new_rel variable
	(*new_rel)->rel_array = malloc(sizeof(relation));
	(*new_rel)->rel_array->num_tuples = rel_array->num_tuples;
	(*new_rel)->rel_array->tuples = malloc(rel_array->num_tuples * sizeof(tuple));
	//Traverse through the original array
	for (i = 0; i < rel_array->num_tuples; ++i)
	{
		//Find the hash value of the current tuple
		hashed_value = HashFunction1(rel_array->tuples[i].value, N_LSB);
		//Using the hash value find the insert position using the temp_psum
		int InsertPos = temp_psum[hashed_value];
		(*new_rel)->rel_array->tuples[InsertPos].value = rel_array->tuples[i].value;
		(*new_rel)->rel_array->tuples[InsertPos].row_id = rel_array->tuples[i].row_id;
		temp_psum[hashed_value]++;//The InsertPos for the same bucket goes up 1 position
	}
	//Free temp_psum array
	free(temp_psum);
}
