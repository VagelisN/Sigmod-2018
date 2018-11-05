#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rhjoin.h"
#include "preprocess.h"
#include "results.h"

result* RadixHashJoin(relation *relR, relation* relS)
{
	int i;
	//Create Histogram,Psum,R',S'
	ReorderedRelation* NewR = NULL;
	ReorderedRelation* NewS = NULL;

	//NewR and NewS both have the first index 
	ReorderArray(relR, N_LSB, &NewR);
	ReorderArray(relS, N_LSB, &NewS);
	/*
	for (i = 0; i < relR->num_tuples; ++i)
	{
		printf("%2d %2d || %2d %2d\n", relR->tuples[i].Value, relR->tuples[i].RowId, NewR->RelArray->tuples[i].Value, NewR->RelArray->tuples[i].RowId);
	}
	printf("\n");
	for (i = 0; i < relS->num_tuples; ++i)
	{
		printf("%2d %2d || %2d %2d\n", relS->tuples[i].Value, relS->tuples[i].RowId, NewS->RelArray->tuples[i].Value, NewS->RelArray->tuples[i].RowId);
	}

	printf("Hist:\n");
	for (i = 0; i < NewR->Hist_size; ++i)
	{
		printf("%d %d\n", NewR->Hist[i][0], NewR->Hist[i][1]);
	}
	printf("--------------------------------------\n");
	printf("Psum:\n");
	for (i = 0; i < NewR->Hist_size; ++i)
	{
		printf("%d %d\n", NewR->Psum[i][0], NewR->Psum[i][1]);
	}
		printf("Hist:\n");
	for (i = 0; i < NewS->Hist_size; ++i)
	{
		printf("%d %d\n", NewS->Hist[i][0], NewS->Hist[i][1]);
	}
	printf("--------------------------------------\n");
	printf("Psum:\n");
	for (i = 0; i < NewS->Hist_size; ++i)
	{
		printf("%d %d\n", NewS->Psum[i][0], NewS->Psum[i][1]);
	}
	*/
	struct result* results= NULL;

	//for every bucket
	for (i = 0; i < NewR->Hist_size; ++i)
	{
		//if both relations have elements in this bucket
		if (NewR->Hist[i][1] != 0 && NewS->Hist[i][1] != 0)
		{
			//if R is bigger than S
			if( NewR->Hist[i][1] >= NewS->Hist[i][1])
			{
				bc_index* indS = NULL;
				//Create a second layer index for the respective bucket of S
				
				InitIndex(&indS, NewS->Hist[i][1], NewS->Psum[i][1]);

				CreateIndex(NewS,&indS,i);

				//Get results
				GetResults(NewR,NewS,indS,&results,i,0);
				DeleteIndex(&indS);
			}
			//if S is bigger than R
			else
			{
				bc_index* indR = NULL;
				//Initialize the 2nd layer index
				InitIndex(&indR, NewR->Hist[i][1], NewR->Psum[i][1]);

				//Create a second layer index for the respective bucket of R
				CreateIndex(NewR,&indR,i);

				//GetResults
				GetResults(NewS,NewR,indR,&results,i,1);

				//Delete the 2nd layer index
				DeleteIndex(&indR);

			}
		}
	}
	PrintResult(results);
	CheckResult(results);
	FreeResult(results);

	//Free NewS and NewR
	FreeReorderRelation(NewS);
	FreeReorderRelation(NewR);
}


int GetResults(ReorderedRelation* full_relation,ReorderedRelation* indexed_relation,bc_index * ind,struct result ** res,int curr_bucket,int r_s)
{
	int i, start_full, hash_value, sp;

	//the start of the non indexed current bucket
	start_full = full_relation->Psum[curr_bucket][1];

	//for every element of the non indexed relation's bucket
	for (i = 0; i < full_relation->Hist[curr_bucket][1]; i++)
	{
		//check the index of the second relation
		hash_value = HashFunction2((full_relation->RelArray->tuples[(start_full + i)].Value), ind->index_size);

		//if there are elements in this bucket of the indexed relation
		if( ind->bucket[hash_value] != -1)
		{
			//scan the values following the chain for equality
			if(indexed_relation->RelArray->tuples[(ind->start + (ind->bucket[hash_value])-1)].Value == full_relation->RelArray->tuples[(start_full + i)].Value)
			{
				result_tuples curr_res;

				//index is built on relation S
				if (r_s == 0)
				{
					curr_res.tuple_R.RowId = full_relation->RelArray->tuples[start_full + i].RowId;
					curr_res.tuple_R.Value = full_relation->RelArray->tuples[start_full + i].Value;

					curr_res.tuple_S.RowId = indexed_relation->RelArray->tuples[(ind->start + (ind->bucket[hash_value])-1)].RowId;
					curr_res.tuple_S.Value = indexed_relation->RelArray->tuples[(ind->start + (ind->bucket[hash_value])-1)].Value;
				}
				//index is built on relation R
				else 
				{
					curr_res.tuple_S.RowId = full_relation->RelArray->tuples[start_full + i].RowId;
					curr_res.tuple_S.Value = full_relation->RelArray->tuples[start_full + i].Value;

					curr_res.tuple_R.RowId = indexed_relation->RelArray->tuples[(ind->start + (ind->bucket[hash_value])-1)].RowId;
					curr_res.tuple_R.Value = indexed_relation->RelArray->tuples[(ind->start + (ind->bucket[hash_value])-1)].Value;
				}

				//insert the result in the list
				InsertResult(res,&curr_res);
			}

			//follow the chain
			sp = (ind->bucket[hash_value]-1);			
			while( ind->chain[sp] != 0)
			{
				if(indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].Value == full_relation->RelArray->tuples[start_full + i].Value)
				{
					result_tuples curr_res;
					if (r_s == 0)
					{
						curr_res.tuple_R.RowId = full_relation->RelArray->tuples[start_full + i].RowId;
						curr_res.tuple_R.Value = full_relation->RelArray->tuples[start_full + i].Value;

						curr_res.tuple_S.RowId = indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].RowId;
						curr_res.tuple_S.Value = indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].Value;
					}
					else 
					{
						curr_res.tuple_S.RowId = full_relation->RelArray->tuples[start_full + i].RowId;
						curr_res.tuple_S.Value = full_relation->RelArray->tuples[start_full + i].Value;

						curr_res.tuple_R.RowId = indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].RowId;
						curr_res.tuple_R.Value = indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].Value;
					}
					//insert the result in the list
					InsertResult(res, &curr_res);
				}	
				sp = (ind->chain[sp]-1);
			}
		}
	}
}

int CreateIndex(ReorderedRelation *rel, bc_index** ind,int curr_bucket)
{
	int start, i, hash_value;

	//the start of the current bucket is in Psum
	start = rel->Psum[curr_bucket][1];
	
	//Hash every value of the bucket from last to first with H2 and set up bucket and chain
	for (i = (rel->Hist[curr_bucket][1]-1); i >= 0; i--)
	{
		hash_value = HashFunction2(rel->RelArray->tuples[start+i].Value,(*ind)->index_size);

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