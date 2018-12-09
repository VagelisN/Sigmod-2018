#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "preprocess.h"

void ReorderArray(relation* rel_array, int n_lsb, reordered_relation** new_rel)
{
	//Check the arguments
	if ((rel_array == NULL) || (n_lsb <= 0))
	{
		printf("Error in ReorderArray. Invalid arguments\n");
		exit(1);
	}
	int i = 0;
	uint32_t hashed_value = 0;

	(*new_rel) = malloc(sizeof(reordered_relation));
	CheckMalloc((*new_rel), "*new_rel (preprocess.c)");
	//Find the size of the psum and the hist arrays
	(*new_rel)->hist_size = 1;
	for (i = 0; i < n_lsb; ++i)
		(*new_rel)->hist_size *= 2;

	// Allocate space for the hist and psum arrays
	(*new_rel)->psum = malloc((*new_rel)->hist_size * sizeof(int));
	CheckMalloc((*new_rel)->psum, "*new_rel->psum (preprocess.c)");
	(*new_rel)->hist = malloc((*new_rel)->hist_size * sizeof(int));
	CheckMalloc((*new_rel)->hist, "*new_rel->hist (preprocess.c)");

	//temp_psum array is used only in this function for faster reordering of the array
	int* temp_psum = malloc((*new_rel)->hist_size * sizeof(int));
	CheckMalloc(temp_psum, "*temp_psum (preprocess.c)");

	for (i = 0; i < (*new_rel)->hist_size; ++i)
	{
		(*new_rel)->psum[i] = -1; //Initialize the starting point of each bucket in the reordered array to -1
		(*new_rel)->hist[i] = 0; // Each bucket starts with 0 allocated values
		temp_psum[i] = -1;
	}

	//Run rel_array with the hash function and build the histogram
	for (i = 0; i < rel_array->num_tuples; ++i)
	{
		hashed_value = HashFunction1((int32_t) rel_array->tuples[i].value, n_lsb);
		(*new_rel)->hist[hashed_value]++;
	}

	//CheckBucketSizes((*new_rel)->hist, (*new_rel)->hist_size);

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
	CheckMalloc((*new_rel)->rel_array, "*new_rel->rel_array (preprocess.c)");
	(*new_rel)->rel_array->num_tuples = rel_array->num_tuples;
	(*new_rel)->rel_array->tuples = malloc(rel_array->num_tuples * sizeof(tuple));
	CheckMalloc((*new_rel)->rel_array->tuples, "*new_rel->rel_array->tuples (preprocess.c)");

	//Traverse through the original array
	for (i = 0; i < rel_array->num_tuples; ++i)
	{
		//Find the hash value of the current tuple
		hashed_value = HashFunction1(rel_array->tuples[i].value, n_lsb);
		//Using the hash value find the insert position using the temp_psum
		int InsertPos = temp_psum[hashed_value];
		(*new_rel)->rel_array->tuples[InsertPos].value = rel_array->tuples[i].value;
		(*new_rel)->rel_array->tuples[InsertPos].row_id = rel_array->tuples[i].row_id;
		temp_psum[hashed_value]++;//The InsertPos for the same bucket goes up 1 position
	}
	//Free temp_psum array
	free(temp_psum);
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
