#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "preprocess.h"
#include "scheduler.h"

relation* ToRow(int** original_array, int row_to_join, relation* new_rel)
{
	for (int i = 0; i < new_rel->num_tuples; ++i)
	{
		new_rel->tuples[i].value = original_array[i][row_to_join];
		new_rel->tuples[i].row_id = i;
	}
	return new_rel;
}


void ReorderArray(relation* rel_array, int n_lsb, reordered_relation** new_rel,scheduler *sched)
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
	int thread_num = sched->num_of_threads;
	int **histograms = malloc(thread_num * sizeof(int *));
	for (size_t i = 0; i < thread_num; i++)
	  histograms[i] = NULL;

	//Split up the num_tuples so each thread gets the same
	int tuples_per_thread = rel_array->num_tuples/thread_num;

	//For each thread
	for (size_t i = 0; i < thread_num; i++) 
	{
	  //Set the arguments
	  hist_arguments *arguments = malloc(sizeof(hist_arguments));
	  arguments->hist = &histograms[i];
	  arguments->n_lsb = n_lsb;
	  arguments->hist_size = hist_size;
	  arguments->rel = rel_array;
	  arguments->start = i * tuples_per_thread;
	  //If we are setting up the last thread, end is the end of the relation
	  if(i != thread_num - 1)arguments->end = (i+1) * tuples_per_thread;
	  else arguments->end = rel_array->num_tuples;

	  //Enqueue it in the scheduler
	  push_job(sched, 0,(void*)arguments);
	}

	//Wait for all threads to finish building their work(barrier)
	pthread_mutex_lock(&(sched->queue_access));
	pthread_cond_wait(&(sched->barrier_cond),&(sched->queue_access));
	pthread_mutex_unlock(&(sched->queue_access));

	for (int i = 0; i < thread_num; ++i)
	{
		printf("HIST [%d]\n",i );
		for (int j = 0; j < hist_size; ++j)
		{
			printf("%d\n",histograms[i][j] );
		}
	}

	printf("done\n");
	exit(1);


	//Allocate the Hist and Psum arrays
	int *Psum = malloc(hist_size * sizeof(int));
	int *Hist = calloc(hist_size, sizeof(int));
	for (size_t i = 0; i < hist_size; i++)
	  Psum[i] = -1;

	//Build the whole Histogram from the
	for (size_t i = 0; i < thread_num; i++)
	  for (size_t j = 0; j < hist_size; j++)
	    Hist[j] += histograms[i][j];

	//Free allocated space
	for (size_t i = 0; i < thread_num; i++)
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

	/*--------------------Build the reordered array----------------------*/

	//Allocate space for the ordered array in new_rel variable
	(*new_rel)->rel_array = malloc(sizeof(relation));
	CheckMalloc((*new_rel)->rel_array, "*new_rel->rel_array (preprocess.c)");
	(*new_rel)->rel_array->num_tuples = rel_array->num_tuples;
	(*new_rel)->rel_array->tuples = malloc(rel_array->num_tuples * sizeof(tuple));
	CheckMalloc((*new_rel)->rel_array->tuples, "*new_rel->rel_array->tuples (preprocess.c)");

	//Traverse through the original array
	uint64_t hashed_value;
	for (int i = 0; i < rel_array->num_tuples; ++i)
	{
		//Find the hash value of the current tuple
		hashed_value = HashFunction1(rel_array->tuples[i].value, n_lsb);
		//Using the hash value find the insert position using the temp_psum
		//int InsertPos = temp_psum[hashed_value];
		//(*new_rel)->rel_array->tuples[InsertPos].value = rel_array->tuples[i].value;
		//(*new_rel)->rel_array->tuples[InsertPos].row_id = rel_array->tuples[i].row_id;
		//temp_psum[hashed_value]++;//The InsertPos for the same bucket goes up 1 position
	}
	//Free temp_psum array
	//free(temp_psum);
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

int CheckMalloc(void* ptr, char* txt)
{
	if (ptr == NULL)
	{
		fprintf(stderr,"Error in malloc! In: %s \n",txt);
		exit(-1);
	}
	return 0;//if ptr != null return 0
}

/*
void CheckBucketSizes(int* hist, int hist_size)
{
	int i, flag = 0;
	for (i = 0; i < hist_size; ++i)
	{
		if ((hist[i] * sizeof(tuple)) >= CACHE_SIZE)
		{
			flag = 1;
			break;
		}
	}
	if (flag == 1)
	{
		printf("Warning! Bucket size exceeds L1 cache size (aprox. 32 KB). Consider increasing the N_LSB for the H1 hash function.\n");
	}
}*/

int* HistJob(void *arguments)
{
  hist_arguments *args = arguments;

  //Allocate memory for the thread's histogram and set each value to 0.
  args->(*hist) = calloc(args->hist_size, sizeof(int));

  for (size_t i = args->start; i < args->end; i++) {
    uint64_t hashed_value = HashFunction1(args->rel[i], args->n_lsb);
    args->(*hist)[hashed_value]++;
  }
  return;
}
