#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct partition_arguments
{
	relation **reordered;
	relation *original;
	uint rel_size;
	uint start;
	uint end;
  uint hist_size;
	int *psum;
}part_arguments;

void PartitionJob(void* args)//int start, int end, int size, int* Psum, int modulo, int **reordered, int *original)
{
	part_arguments *arguments = (part_arguments*)args;

	int current_bucket;
	//Find the bucket
	for (int i = 0; i < arguments->hist_size; ++i)
	{
		printf("psum[%lu]: %d\n", i, arguments->psum[i]);
		if (start == arguments->psum[i])
		{
			current_bucket = i;
			break;
		}
		if(start < arguments->psum[i])
		{
			current_bucket = i - 1;
			break;
		}
	}
	//printf("Start: %d and Psum[%d] = %d\n", start, current_bucket, Psum[current_bucket]);
	//printf("end - start= %d\n", end - start);
	//To skip_Values den ypologizetai swsta
	int skip_values = arguments->start - arguments->psum[current_bucket];
	//printf("Start = %d , skip_values = %d\n", start, skip_values);
	arguments->psum[current_bucket] = arguments->start;
	int previous_encounter = 0;
	for (int i = arguments->start; i < arguments->end; ++i)
	{
		//printf("i = [%d], current_bucket = %d\n", i, current_bucket);
		for (int j = previous_encounter; j < arguments->rel_size; ++j)
		{
			int hash_value = Hash(arguments->original[j], arguments->hist_size);
			if (hash_value == current_bucket)
			{
				if(skip_values-- > 0)continue;
				//printf("Inserting %d to %d position\n", original[j], i);
				(*arguments->reordered)[i] = arguments->original[j];
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
	return;
}

int main(int argc, char const *argv[])
{
	clock_t start_time = clock();
	int tuples = 5000000;
	int modulo = 1000;
	int *R = malloc(tuples * sizeof(int));
	int *reordered = calloc(tuples , sizeof(int));
	int *Hist = malloc(modulo * sizeof(int));
	int *Psum = malloc(modulo * sizeof(int));
	for (int i = 0; i < modulo; ++i)
	{
		Hist[i] = 0;
		Psum[i] = -1;
	}
	for (int i = 0; i < tuples; ++i)
	{
		R[i] = i;
		int hash_value = Hash(R[i], modulo);
		Hist[hash_value]++;
	}
	//Build the Psum
	for (int i = 0; i < modulo; ++i)
	{
		if (i == 0)
			Psum[i] = 0;
		else
			Psum[i] = Psum[i-1] + Hist[i-1];
		//printf("Psum[%d]: %d\n", i, Psum[i]);
	}
	int thread_num = 15;
	int tuples_per_thread = tuples / thread_num;
	int *tempPsum = malloc(modulo * sizeof(int));
	for (int i = 0; i < thread_num; ++i)
	{
		int start = i * tuples_per_thread;
		int end;
		if(i != thread_num - 1)
			end = start + tuples_per_thread;
		else
			end = tuples;
		//Make a copy of Psum
		memcpy(tempPsum, Psum, modulo * sizeof(int));
		//for (int i = 0; i < modulo; ++i)
		//	tempPsum[i] = Psum[i];
		if (start < 0 || end > tuples)
		{
			printf("Egine malakia!\n");
			printf("thread_num: %d, tuples_per_thread: %d, i: %d\n",thread_num, tuples_per_thread, i );
			printf("start: %d, end: %d\n", start, end);
			exit(-1);
		}
		PartitionJob(start, end, tuples, tempPsum, modulo, &reordered, R);
	}
	//for (int i = 0; i < tuples; ++i)
	//	printf("R'[%2d] = %2d\n", i, reordered[i]);
	free(R);
	free(reordered);
	free(Hist);
	free(Psum);
	free(tempPsum);
	printf("Running time: %f\n",(double) (clock() - start_time)/ CLOCKS_PER_SEC );
	return 0;
}



void JoinJob(void *arguments)
{
	join_arguments *args = (join_arguments*) arguments;
	bc_index* ind = NULL;
	//if R is bigger than S
	if( args->NewR->hist[args->bucket_num] >= args->NewS->hist[args->bucket_num])
	{

		//Create a second layer index for the respective bucket of S
		InitIndex(&ind, args->NewS->hist[args->bucket_num], args->NewS->psum[args->bucket_num]);
		CreateIndex(args->NewS,&ind,args->bucket_num);
		//Get results
		GetResults(args->NewR,args->NewS,ind,&args->res,args->bucket_num,0);
	}
	//if S is bigger than R
	else
	{
		//Create a second layer index for the respective bucket of R
		InitIndex(&ind, args->NewR->hist[args->bucket_num], args->NewR->psum[args->bucket_num]);
		CreateIndex(NewR,&ind,args->bucket_num);
		//GetResults
		GetResults(args->NewS, args->NewR, ind, &args->res, args->bucket_num,1);
	}
	DeleteIndex(&ind);
	free(args);
}

void MergeResults(result **res, result **res_array, int size)
{
	bool found = 0;
	result* previous_tail = NULL;
	for (size_t i = 0; i < size; i++)
	{
		//First active position is the final_results head node
		if(res_array[i] == NULL)continue;
		if (!found)
		{
				(*res) = res_array[i];
				found = 1;
		}
		//If the previous_tail is active then point it to the current head
		if (previous_tail != NULL) previous_tail->next = res_array[i];
		//Find the last node of the res_array[i]
		while(res_array[i]->next != NULL)res_array[i] = res_array[i]->next;
		previous_tail = res_array[i];
	}
	return;
}
