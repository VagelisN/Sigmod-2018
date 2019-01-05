#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdbool.h>
#include "rhjoin.h"
#include "preprocess.h"
#include "results.h"
#include "scheduler.h"
#include "structs.h"

result* RadixHashJoin(relation *relR, relation* relS)
{
	scheduler* sched = NULL;
	SchedulerInit(&sched, 2);

	if (relR->num_tuples == 0 || relS->num_tuples == 0)
		return NULL;
	int i;
	reordered_relation* NewR = NULL;
	reordered_relation* NewS = NULL;
	//Create histogram,psum,R',S'
	ReorderArray(relR, N_LSB, &NewR, sched);
	ReorderArray(relS, N_LSB, &NewS, sched);


	if (NewR == NULL || NewS == NULL)
	{
		SchedulerDestroy(sched);
		return NULL;
	}
	struct result* results= NULL;

	//Create an array with hist_size result lists


	uint answers = 0;
	for (size_t i = 0; i < NewR->hist_size; i++)
		if (NewR->hist[i] != 0 && NewS->hist[i] != 0)
			answers++;

	result **res_array = calloc(answers, sizeof(result*));
	sched->answers_waiting = answers;
	//For every bucket
	int count = 0;
	for (i = 0; i < NewR->hist_size; ++i)
	{
		//If both relations have elements in this bucket
		if (NewR->hist[i] != 0 && NewS->hist[i] != 0)
		{
			//Set up the arguments
			join_arguments *arguments = malloc(sizeof(join_arguments));
			arguments->NewR = NewR;
			arguments->NewS = NewS;
			arguments->res = &res_array[count++];
			arguments->bucket_num = i;

			//PushJob
			PushJob(sched, 2, (void*)arguments);
		}
	}

	//Wait for all threads to finish building their work(barrier)
	Barrier(sched);

	result *final_results = MergeResults( res_array, answers);
	SchedulerDestroy(sched);

	//Free NewS and NewR
	FreeReorderRelation(NewS);
	FreeReorderRelation(NewR);
	return final_results;
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
		GetResults(args->NewR,args->NewS,ind,args->res,args->bucket_num,0);
		//PrintResult(*(args->res));
	}
	//if S is bigger than R
	else
	{
		//Create a second layer index for the respective bucket of R
		InitIndex(&ind, args->NewR->hist[args->bucket_num], args->NewR->psum[args->bucket_num]);
		CreateIndex(args->NewR, &ind, args->bucket_num);
		//GetResults
		GetResults(args->NewS, args->NewR, ind, args->res, args->bucket_num,1);
	//	PrintResult(*(args->res));
	}
	DeleteIndex(&ind);
	free(args);
}



int GetResults(reordered_relation* full_rel,reordered_relation* indexed_rel,bc_index * ind,
			   struct result ** res,int curr_bucket,int r_s)
{
	uint64_t i, start_full, sp, value1, value2;

	//the start of the non indexed current bucket
	start_full = full_rel->psum[curr_bucket];

	tuple* full_tuples = full_rel->rel_array->tuples;
	tuple* ind_tuples = indexed_rel->rel_array->tuples;

	//for every element of the non indexed relation's bucket
	result *cur_node = NULL;
	for (i = 0; i < full_rel->hist[curr_bucket]; i++)
	{
		//check the index of the second relation
		uint64_t hash_value = HashFunction2((full_tuples[(start_full + i)].value), ind->index_size);

		//if there are elements in this bucket of the indexed relation
		if( ind->bucket[hash_value] != -1)
		{
			//scan the values starting from the bucket and following the chain for equality
			value1 =ind_tuples[(ind->start + (ind->bucket[hash_value])-1)].value;
			value2 =full_tuples[(start_full + i)].value;
			if(value1 == value2)
			{
				result_tuple curr_res;
				// S has the second layer index
				if (r_s == 0)
				{
					curr_res.row_idR = full_tuples[start_full + i].row_id;
					curr_res.row_idS = ind_tuples[(ind->start + (ind->bucket[hash_value])-1)].row_id;
				}
				else
				{
					curr_res.row_idS = full_tuples[start_full + i].row_id;
					curr_res.row_idR = ind_tuples[(ind->start + (ind->bucket[hash_value])-1)].row_id;
				}
				if(cur_node == NULL)
					cur_node = InsertResult(res,&curr_res);
				else
				{
					cur_node = InsertResult(&cur_node, &curr_res);
				}
			}

			//follow the chain
			sp = (ind->bucket[hash_value]-1);
			while( ind->chain[sp] != 0)
			{
				value1 = ind_tuples[(ind->start + (ind->chain[sp]-1))].value;
				value2 = full_tuples[start_full + i].value;
				if( value1 == value2)
				{
					result_tuple curr_res;
					if (r_s == 0)
					{
						curr_res.row_idR = full_tuples[start_full + i].row_id;
						curr_res.row_idS = ind_tuples[(ind->start + (ind->chain[sp]-1))].row_id;
					}
					else
					{
						curr_res.row_idS = full_tuples[start_full + i].row_id;
						curr_res.row_idR = ind_tuples[(ind->start + (ind->chain[sp]-1))].row_id;
					}
					if(cur_node == NULL)
						cur_node = InsertResult(res, &curr_res);
					else
					{
						cur_node = InsertResult(&cur_node, &curr_res);
					}
				}
				sp = (ind->chain[sp]-1);
			}
		}
	}
}

int CreateIndex(reordered_relation *rel, bc_index** ind,int curr_bucket)
{
	int start, i;

	//the start of the current bucket is in psum
	start = rel->psum[curr_bucket];

	//Hash every value of the bucket from last to first with H2 and set up bucket and chain
	for (i = (rel->hist[curr_bucket]-1); i >= 0; i--)
	{
		uint64_t hash_value = HashFunction2(rel->rel_array->tuples[start+i].value,(*ind)->index_size);
		//last encounter
		if ((*ind)->bucket[hash_value] == -1 )
		{
			(*ind)->bucket[hash_value] = i+1;
			(*ind)->chain[i] = 0;
		}
		//second or later encounter ->also need to adjust chain
		else
		{
			//while chain is not 0 go to the next
			int sp = (*ind)->bucket[hash_value] - 1;
			while( (*ind)->chain[sp] != 0)
			{
				sp = (*ind)->chain[sp]-1;
			}
			(*ind)->chain[sp]=i+1;
			(*ind)->chain[i] = 0;
		}
	}
	//PrintIndex((*ind));
	return 0;
}


int InitIndex(bc_index** ind, int bucket_size, int start)
{
	//allocate the index
	(*ind) = malloc(sizeof(bc_index));

	//the size of the bucket array  is the next prime after the size of the bucket
	uint32_t hash_size = FindNextPrime(bucket_size);
	(*ind)->bucket = malloc(hash_size * sizeof(int32_t));

	//the size of the chain is equal to the number of elements in the bucket
	(*ind)->chain = (int32_t*) malloc(bucket_size * sizeof(int32_t));

	(*ind)->start = start;
	(*ind)->end = start + bucket_size;

	int i;
	(*ind)->index_size = hash_size;
	for (i = 0; i < hash_size; ++i)(*ind)->bucket[i] = -1;
	for (i = 0; i < bucket_size; ++i) (*ind)->chain[i] = -1;
	return 0;
}

int DeleteIndex(bc_index** ind)
{
	if ((*ind != NULL) && ((*ind)->bucket != NULL))
	{
		free((*ind)->bucket);
		free((*ind)->chain);
		free(*ind);
	}
	else
	{
		printf("Error, ind or ind->bucket = NULL\n");
		exit(2);
	}
}

void PrintIndex(bc_index* ind)
{
	int i;
	int sp;
	printf("Printing index for bucket with start: %d\n",ind->start );
	for (i = 0; i < ind->index_size; i++)
	{
		if (ind->bucket[i] != -1)
		{
			printf("bucket[%d] = %d\n",i , ind->bucket[i] );
			sp = (ind->bucket[i]-1);
			while( ind->chain[sp] != 0)
			{
				printf("chain[%d] = %d\n",ind->bucket[i]-1, ind->chain[ind->bucket[i]-1]);
				if(sp != 0)sp = (ind->chain[sp]-1);
				else break;
			}
		}
	}
}

uint64_t HashFunction1(uint64_t num, uint64_t n)
{
	//mask is 64 ones
	uint64_t mask =
	0b1111111111111111111111111111111111111111111111111111111111111111;

	//keep only n ones on the right
	mask = mask << 64-n;
	mask = mask >> 64-n;

	//num AND mask only keeps the n rightmost bits of num
	uint64_t hash_value = num & mask;

	return hash_value;
}

uint64_t FindNextPrime(uint64_t num)
{
	short int found;
	if (num % 2 == 0)num++;
	while(1)
	{
		found = 1;
		for (int i = 3; i*i < num; ++i)
		{
			if(num % i == 0)
			{
				found = 0;
				num += 2;
				break;
			}
		}
		if (found)break;
	}
	return num;
}

uint64_t HashFunction2(uint64_t num, uint64_t prime)
{
	return num % prime;
}


result* MergeResults(result **res_array, int size)
{
	result* head = malloc(sizeof(result));
	head->current_load = 0;
	head->next = NULL;
	head->buff = malloc(RESULT_FINAL_BUFFER);
	result *head_temp = head;
	for (size_t i = 0; i < size; i++)
	{
		if(res_array[i] == NULL)continue;
		//Run the result list and copy everything to head
		result *temp = res_array[i];
		while(temp != NULL)
		{
			//If it fits in the head
			uint head_load = head_temp->current_load * sizeof(result_tuple);
			uint temp_load = temp->current_load * sizeof(result_tuple);
			if(head_load + temp_load < RESULT_FINAL_BUFFER)
			{
				memcpy(head_temp->buff + head_load, temp->buff, temp_load);
				head_temp->current_load += temp->current_load;
			}
			else
			{
				head_temp->next = malloc(sizeof(result));
				head_temp = head_temp->next;
				head_temp->current_load = 0;
				head_temp->next = NULL;
				head_temp->buff = malloc(RESULT_FINAL_BUFFER);
				memcpy(head_temp->buff, temp->buff, temp_load);
				head_temp->current_load += temp->current_load;
			}
			temp = temp->next;
		}
		FreeResult(res_array[i]);
	}
	free(res_array);
	return head;
}
