#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rhjoin.h"
#include "preprocess.h"
#include "results.h"
#include "scheduler.h"

result* RadixHashJoin(relation *relR, relation* relS)
{
	scheduler* sched = NULL;
	SchedulerInit(&sched,4);

	if (relR->num_tuples == 0 || relS->num_tuples == 0)
		return NULL;
	int i;
	reordered_relation* NewR = NULL;
	reordered_relation* NewS = NULL;
	//Create histogram,psum,R',S'
	ReorderArray(relR, N_LSB, &NewR, sched);
	ReorderArray(relS, N_LSB, &NewS, sched);
	if (NewR == NULL || NewS == NULL)
		return NULL;
	struct result* results= NULL;

	//for every bucket
	for (i = 0; i < NewR->hist_size; ++i)
	{
		//if both relations have elements in this bucket
		if (NewR->hist[i] != 0 && NewS->hist[i] != 0)
		{
			bc_index* ind = NULL;
			//if R is bigger than S
			if( NewR->hist[i] >= NewS->hist[i])
			{

				//Create a second layer index for the respective bucket of S
				InitIndex(&ind, NewS->hist[i], NewS->psum[i]);
				CreateIndex(NewS,&ind,i);
				//Get results
				GetResults(NewR,NewS,ind,&results,i,0);
			}
			//if S is bigger than R
			else
			{
				//Create a second layer index for the respective bucket of R
				InitIndex(&ind, NewR->hist[i], NewR->psum[i]);
				CreateIndex(NewR,&ind,i);
				//GetResults
				GetResults(NewS,NewR,ind,&results,i,1);
			}
			DeleteIndex(&ind);
		}
	}
	//Free NewS and NewR
	FreeReorderRelation(NewS);
	FreeReorderRelation(NewR);
	return results;
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
				InsertResult(res,&curr_res);
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
					InsertResult(res, &curr_res);
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
	if(num == 1) return 1;
	if(num == 2) return 2;

	uint64_t next_prime;
	if (num % 2 == 0) next_prime = num + 1;
	else next_prime = num;

	int found = 0 , i,is_prime = 1;
	while ( found == 0 )
	{
    	for (i = 3; i <= next_prime / 2; i += 2)
	    {
	        if (next_prime % i == 0)
	        {
	        	is_prime = 0;
	        	break;
	        }
	    }
	    if (is_prime == 1)found = 1;
	    else
		{
			next_prime += 2;
			is_prime =1;
		}
	}
	return next_prime;
}

uint64_t HashFunction2(uint64_t num, uint64_t prime)
{
	return num % prime;
}
