#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rhjoin.h"
#include "preprocess.h"
#include "results.h"

result* RadixHashJoin(relation *relR, relation* relS)
{

	//Create Histogram,Psum,R',S'
	ReorderedRelation* NewR = NULL;
	ReorderedRelation* NewS = NULL;

	//NewR and NewS both have the first index 
	ReorderArray(relR, N_LSB, &NewR);
	ReorderArray(relS, N_LSB, &NewS);


	int i;
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

	struct result* results= NULL;
	uint32_t index_size;

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
	FreeResult(results);
	//Free NewS
	if (NewS != NULL)
	{
		//Free Psum and Hist
		if (NewS->Hist_size != -1)
		{
			for (int i = 0; i < NewS->Hist_size; ++i)
			{
				free(NewS->Hist[i]);
				free(NewS->Psum[i]);
			}
			free(NewS->Psum);
			free(NewS->Hist);
		}
		//Free RelArray
		if (NewS->RelArray != NULL)
		{
			if (NewS->RelArray->tuples != NULL)free(NewS->RelArray->tuples);
			free(NewS->RelArray);
		}
		free(NewS);
	}

	//Free NewR
	if (NewR != NULL)
	{
		//Free Psum and Hist
		if (NewR->Hist_size != -1)
		{
			for (int i = 0; i < NewR->Hist_size; ++i)
			{
				free(NewR->Hist[i]);
				free(NewR->Psum[i]);
			}
			free(NewR->Psum);
			free(NewR->Hist);
		}
		//Free RelArray
		if (NewR->RelArray != NULL)
		{
			if (NewR->RelArray->tuples != NULL)free(NewR->RelArray->tuples);
			free(NewR->RelArray);
		}
		free(NewR);
	}
}

/**
 * Gets two relations. The first one only has a first layer index and the second one
 * also has a second layer index (ind) for a bucket (curr_bucket). For every element
 * of the first relation's bucket it checks for equalityin the second layer index 
 * of the second relation and returns the results 
*/

int GetResults(ReorderedRelation* full_relation,ReorderedRelation* indexed_relation,bc_index * ind,struct result ** res,int curr_bucket,int r_s)
{
	int i ,j, start , end, hash_value, sp;

	//the start of the current bucket is in Psum
	start = full_relation->Psum[curr_bucket][1];

	//the end is at Psum + the number of elements in current bucket
	end = start + full_relation->Hist[curr_bucket][1];

	//for every element of the first relation's bucket
	for (i = 0; i < full_relation->Hist[curr_bucket][1]; i++)
	{
		//check the second layer of the second relation
		hash_value = HashFunction2((full_relation->RelArray->tuples[((ind->start) + i)].Value), ind->index_size);
		//if there are elements in this second layer's hash value
		if( ind->bucket[hash_value] != -1)
		{
			//scan the values following the chain for equality
			if(indexed_relation->RelArray->tuples[(start + (ind->bucket[hash_value])-1)].Value == full_relation->RelArray->tuples[((ind->start) + i)].Value)
			{
				result_tuples curr_res;

				//index is on relation S
				if (r_s == 0)
				{
					curr_res.tuple_R.RowId = full_relation->RelArray->tuples[((ind->start) + i)].RowId;
					curr_res.tuple_R.Value = full_relation->RelArray->tuples[((ind->start) + i)].Value;

					curr_res.tuple_S.RowId = indexed_relation->RelArray->tuples[(start + (ind->bucket[hash_value])-1)].RowId;
					curr_res.tuple_S.Value = indexed_relation->RelArray->tuples[(start + (ind->bucket[hash_value])-1)].Value;
				}
				else 
				{
					curr_res.tuple_S.RowId = full_relation->RelArray->tuples[((ind->start) + i)].RowId;
					curr_res.tuple_S.Value = full_relation->RelArray->tuples[((ind->start) + i)].Value;

					curr_res.tuple_R.RowId = indexed_relation->RelArray->tuples[(start + (ind->bucket[hash_value])-1)].RowId;
					curr_res.tuple_R.Value = indexed_relation->RelArray->tuples[(start + (ind->bucket[hash_value])-1)].Value;
				}
				InsertResult(res,&curr_res);
			}
			sp = (ind->bucket[hash_value]-1);
			while( ind->chain[sp] != 0)
			{
				if(indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].Value == full_relation->RelArray->tuples[((ind->start) + i)].Value)
				{
					result_tuples curr_res;
					if (r_s == 0)
					{
						curr_res.tuple_R.RowId = full_relation->RelArray->tuples[((ind->start) + i)].RowId;
						curr_res.tuple_R.Value = full_relation->RelArray->tuples[((ind->start) + i)].Value;

						curr_res.tuple_S.RowId = indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].RowId;
						curr_res.tuple_S.Value = indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].Value;
					}
					else 
					{
						curr_res.tuple_S.RowId = full_relation->RelArray->tuples[((ind->start) + i)].RowId;
						curr_res.tuple_S.Value = full_relation->RelArray->tuples[((ind->start) + i)].Value;

						curr_res.tuple_R.RowId = indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].RowId;
						curr_res.tuple_R.Value = indexed_relation->RelArray->tuples[(ind->start + (ind->chain[sp]-1))].Value;
					}
					InsertResult(res, &curr_res);
				}	
				sp = (ind->chain[sp]-1);
			}
		}
	}
}


/**
 * Creates the second layer index for a bucket of a relation
 * that already has a first layer index
 */

int CreateIndex(ReorderedRelation *rel, bc_index** ind,int curr_bucket)
{
	int start,end,i,hash_value;

	//the start of the current bucket is in Psum
	start = rel->Psum[curr_bucket][1];

	//the end is at Psum + the number of elements in current bucket
	end = start + rel->Hist[curr_bucket][1];
	//create an index
	//InitIndex(ind, rel->Hist[curr_bucket][1]);
	
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
	PrintIndex((*ind));
	return 0;
}

/** Prints the second layer index of a bucket*/

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
	uint32_t mask = 0b11111111111111111111111111111111;
	mask = mask<<32-n;
	mask =mask >> 32-n;
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
	        if (next_prime % i == 0)     //found a factor that isn't 1 or n, therefore not prime
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

int InitIndex(bc_index** ind, int bucket_size, int start)
{
	uint32_t hash_size = FindNextPrime(bucket_size);
	printf("ELA MWRE %d\n",hash_size );
	(*ind) = malloc(sizeof(bc_index));
	CheckMalloc((*ind), "*ind (rhjoin.c)");
	(*ind)->bucket = malloc(hash_size * sizeof(int32_t));
	CheckMalloc((*ind)->bucket, "*ind->bucket (rhjoin.c)");
	(*ind)->chain = (int32_t*) malloc(bucket_size * sizeof(int32_t));
	CheckMalloc((*ind)->chain, "*ind->chain (rhjoin.c)");
	(*ind)->start = start;
	(*ind)->end = start + bucket_size;
	int i;
	for (i = 0; i < hash_size; ++i)
	{
		(*ind)->bucket[i] = -1;
	}
	(*ind)->index_size = hash_size;
	for (i = 0; i < bucket_size; ++i)
	{
		(*ind)->chain[i] = -1;
	}
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
