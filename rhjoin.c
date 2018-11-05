#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rhjoin.h"
#include "preprocess.h"
#include "results.h"

result* RadixHashJoin(relation *relR, relation* relS)
{
	int i;
	//Create histogram,psum,R',S'
	reordered_relation* newR = NULL;
	reordered_relation* newS = NULL;

	//newR and newS both have the first index 
	ReorderArray(relR, N_LSB, &newR);
	ReorderArray(relS, N_LSB, &newS);
	/*
	for (i = 0; i < relR->num_tuples; ++i)
	{
		printf("%2d %2d || %2d %2d\n", relR->tuples[i].value, relR->tuples[i].row_id, newR->rel_array->tuples[i].value, newR->rel_array->tuples[i].row_id);
	}
	printf("\n");
	for (i = 0; i < relS->num_tuples; ++i)
	{
		printf("%2d %2d || %2d %2d\n", relS->tuples[i].value, relS->tuples[i].row_id, newS->rel_array->tuples[i].value, newS->rel_array->tuples[i].row_id);
	}

	printf("hist:\n");
	for (i = 0; i < newR->hist_size; ++i)
	{
		printf("%d %d\n", newR->hist[i][0], newR->hist[i][1]);
	}
	printf("--------------------------------------\n");
	printf("psum:\n");
	for (i = 0; i < newR->hist_size; ++i)
	{
		printf("%d %d\n", newR->psum[i][0], newR->psum[i][1]);
	}
		printf("hist:\n");
	for (i = 0; i < newS->hist_size; ++i)
	{
		printf("%d %d\n", newS->hist[i][0], newS->hist[i][1]);
	}
	printf("--------------------------------------\n");
	printf("psum:\n");
	for (i = 0; i < newS->hist_size; ++i)
	{
		printf("%d %d\n", newS->psum[i][0], newS->psum[i][1]);
	}
	*/
	struct result* results= NULL;

	//for every bucket
	for (i = 0; i < newR->hist_size; ++i)
	{
		//if both relations have elements in this bucket
		if (newR->hist[i][1] != 0 && newS->hist[i][1] != 0)
		{
			//if R is bigger than S
			if( newR->hist[i][1] >= newS->hist[i][1])
			{
				bc_index* indS = NULL;
				//Create a second layer index for the respective bucket of S
				
				InitIndex(&indS, newS->hist[i][1], newS->psum[i][1]);

				CreateIndex(newS,&indS,i);

				//Get results
				GetResults(newR,newS,indS,&results,i,0);
				DeleteIndex(&indS);
			}
			//if S is bigger than R
			else
			{
				bc_index* indR = NULL;
				//Initialize the 2nd layer index
				InitIndex(&indR, newR->hist[i][1], newR->psum[i][1]);

				//Create a second layer index for the respective bucket of R
				CreateIndex(newR,&indR,i);

				//GetResults
				GetResults(newS,newR,indR,&results,i,1);

				//Delete the 2nd layer index
				DeleteIndex(&indR);

			}
		}
	}
	PrintResult(results);
	CheckResult(results);
	FreeResult(results);

	//Free newS and newR
	FreeReorderRelation(newS);
	FreeReorderRelation(newR);
}


int GetResults(reordered_relation* full_relation,reordered_relation* indexed_relation,bc_index * ind,struct result ** res,int curr_bucket,int r_s)
{
	int i, start_full, hash_value, sp;

	//the start of the non indexed current bucket
	start_full = full_relation->psum[curr_bucket][1];

	//for every element of the non indexed relation's bucket
	for (i = 0; i < full_relation->hist[curr_bucket][1]; i++)
	{
		//check the index of the second relation
		hash_value = HashFunction2((full_relation->rel_array->tuples[(start_full + i)].value), ind->index_size);

		//if there are elements in this bucket of the indexed relation
		if( ind->bucket[hash_value] != -1)
		{
			//scan the values following the chain for equality
			if(indexed_relation->rel_array->tuples[(ind->start + (ind->bucket[hash_value])-1)].value == full_relation->rel_array->tuples[(start_full + i)].value)
			{
				result_tuples curr_res;

				//index is built on relation S
				if (r_s == 0)
				{
					curr_res.tuple_R.row_id = full_relation->rel_array->tuples[start_full + i].row_id;
					curr_res.tuple_R.value = full_relation->rel_array->tuples[start_full + i].value;

					curr_res.tuple_S.row_id = indexed_relation->rel_array->tuples[(ind->start + (ind->bucket[hash_value])-1)].row_id;
					curr_res.tuple_S.value = indexed_relation->rel_array->tuples[(ind->start + (ind->bucket[hash_value])-1)].value;
				}
				//index is built on relation R
				else 
				{
					curr_res.tuple_S.row_id = full_relation->rel_array->tuples[start_full + i].row_id;
					curr_res.tuple_S.value = full_relation->rel_array->tuples[start_full + i].value;

					curr_res.tuple_R.row_id = indexed_relation->rel_array->tuples[(ind->start + (ind->bucket[hash_value])-1)].row_id;
					curr_res.tuple_R.value = indexed_relation->rel_array->tuples[(ind->start + (ind->bucket[hash_value])-1)].value;
				}

				//insert the result in the list
				InsertResult(res,&curr_res);
			}

			//follow the chain
			sp = (ind->bucket[hash_value]-1);			
			while( ind->chain[sp] != 0)
			{
				if(indexed_relation->rel_array->tuples[(ind->start + (ind->chain[sp]-1))].value == full_relation->rel_array->tuples[start_full + i].value)
				{
					result_tuples curr_res;
					if (r_s == 0)
					{
						curr_res.tuple_R.row_id = full_relation->rel_array->tuples[start_full + i].row_id;
						curr_res.tuple_R.value = full_relation->rel_array->tuples[start_full + i].value;

						curr_res.tuple_S.row_id = indexed_relation->rel_array->tuples[(ind->start + (ind->chain[sp]-1))].row_id;
						curr_res.tuple_S.value = indexed_relation->rel_array->tuples[(ind->start + (ind->chain[sp]-1))].value;
					}
					else 
					{
						curr_res.tuple_S.row_id = full_relation->rel_array->tuples[start_full + i].row_id;
						curr_res.tuple_S.value = full_relation->rel_array->tuples[start_full + i].value;

						curr_res.tuple_R.row_id = indexed_relation->rel_array->tuples[(ind->start + (ind->chain[sp]-1))].row_id;
						curr_res.tuple_R.value = indexed_relation->rel_array->tuples[(ind->start + (ind->chain[sp]-1))].value;
					}
					//insert the result in the list
					InsertResult(res, &curr_res);
				}	
				sp = (ind->chain[sp]-1);
			}
		}
	}
}

int CreateIndex(reordered_relation *rel, bc_index** ind,int curr_bucket)
{
	int start, i, hash_value;

	//the start of the current bucket is in psum
	start = rel->psum[curr_bucket][1];
	
	//Hash every value of the bucket from last to first with H2 and set up bucket and chain
	for (i = (rel->hist[curr_bucket][1]-1); i >= 0; i--)
	{
		hash_value = HashFunction2(rel->rel_array->tuples[start+i].value,(*ind)->index_size);

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
	CheckMalloc((*ind), "*ind (rhjoin.c)");

	//the size of the bucket array  is the next prime after the size of the bucket
	uint32_t hash_size = FindNextPrime(bucket_size);
	(*ind)->bucket = malloc(hash_size * sizeof(int32_t));
	CheckMalloc((*ind)->bucket, "*ind->bucket (rhjoin.c)");

	//the size of the chain is equal to the number of elements in the bucket
	(*ind)->chain = (int32_t*) malloc(bucket_size * sizeof(int32_t));
	CheckMalloc((*ind)->chain, "*ind->chain (rhjoin.c)");

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

uint32_t HashFunction1(int32_t num, int n)
{
	//mask is 32 ones
	uint32_t mask = 0b11111111111111111111111111111111;

	//keep only n ones on the right   
	mask = mask << 32-n;
	mask = mask >> 32-n;

	//num AND mask only keeps the n rightmost bits of num
	uint32_t hash_value = num & mask;

	return hash_value;
}

uint32_t FindNextPrime(uint32_t num)
{
	if(num == 1) return 1;
	if(num == 2) return 2;

	uint32_t next_prime;
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

uint32_t HashFunction2(int32_t num, uint32_t prime)
{
	return num % prime;
}