#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "preprocess.h"

relation* ToRow(int** original_array, int row_to_join, relation* new_rel)
{
	for (int i = 0; i < new_rel->num_tuples; ++i)
	{
		new_rel->tuples[i].value = original_array[i][row_to_join];
		new_rel->tuples[i].row_id = i;
	}
	return new_rel;
}


void ReorderArray(relation* rel_array, int n_lsb, reordered_relation** new_rel)
{
	//Check the arguments
	if ((rel_array == NULL) || (rel_array->num_tuples == 0) || (n_lsb <= 0))
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
	{
		(*new_rel)->hist_size *= 2;
	}
	
	// Allocate space for the hist and psum arrays
	(*new_rel)->psum = malloc((*new_rel)->hist_size * sizeof(int*));
	CheckMalloc((*new_rel)->psum, "*new_rel->psum (preprocess.c)");
	(*new_rel)->hist = malloc((*new_rel)->hist_size * sizeof(int*));
	CheckMalloc((*new_rel)->hist, "*new_rel->hist (preprocess.c)");
	
	//temp_sum array is used only in this function for faster reordering of the array
	int** temp_sum = malloc((*new_rel)->hist_size * sizeof(int*));
	CheckMalloc(temp_sum, "*temp_sum (preprocess.c)");
	for (i = 0; i < (*new_rel)->hist_size; ++i)
	{
		temp_sum[i] = malloc(2 * sizeof(int));
		CheckMalloc(temp_sum[i], "*temp_sum[i] (preprocess.c)");
		(*new_rel)->psum[i] = malloc(2 * sizeof(int));
		CheckMalloc((*new_rel)->psum[i], "*new_rel->psum[i] (preprocess.c)");
		(*new_rel)->psum[i][0] = i; //Bucket number
		(*new_rel)->psum[i][1] = -1; //Initialize the starting point of each bucket in the reordered array to -1
		temp_sum[i][0] = i;
		temp_sum[i][1] = -1;

		(*new_rel)->hist[i] = malloc(2 * sizeof(int));
		CheckMalloc((*new_rel)->hist[i], "*new_rel->hist[i] (preprocess.c)");
		(*new_rel)->hist[i][0] = i; // Bucket number
		(*new_rel)->hist[i][1] = 0; // Each bucket starts with 0 allocated values
	}

	//Run rel_array with the hash function and build the histogram
	for (i = 0; i < rel_array->num_tuples; ++i)
	{
		hashed_value = HashFunction1((int32_t) rel_array->tuples[i].value, n_lsb);
		(*new_rel)->hist[hashed_value][1]++;
	}
	CheckBucketSizes((*new_rel)->hist, (*new_rel)->hist_size);
	//Build the psum array using the histogram
	int new_starting_point = 0;
	for (i = 0; i < (*new_rel)->hist_size; ++i)
	{
		if ((*new_rel)->psum[i][1] != -1)
		{
			printf("Error in initialization of psum\n");
			exit(1);
		}
		/*
		 *If the current bucket has 0 values allocated to it then leave 
		 *psum[CurrentBucket][1] to -1.
		*/
		if ((*new_rel)->hist[i][1] > 0)
		{
			(*new_rel)->psum[i][1] = new_starting_point;
			temp_sum[i][1] = new_starting_point;
			new_starting_point += (*new_rel)->hist[i][1];
		}
	}
	/*
	printf("hist:\n");
	for (i = 0; i < (*new_rel)->hist_size; ++i)
	{
		printf("%d %d\n", (*new_rel)->hist[i][0], (*new_rel)->hist[i][1]);
	}
	printf("--------------------------------------\n");
	printf("psum:\n");
	for (i = 0; i < (*new_rel)->hist_size; ++i)
	{
		printf("%d %d\n", (*new_rel)->psum[i][0], (*new_rel)->psum[i][1]);
	}
	*/

	/*--------------------Build the reordered array----------------------*/

	//Allocate space for the ordered array in new_rel variable
	(*new_rel)->rel_array = malloc(sizeof(relation));
	CheckMalloc((*new_rel)->rel_array, "*new_rel->rel_array (preprocess.c)");
	(*new_rel)->rel_array->num_tuples = rel_array->num_tuples;
	(*new_rel)->rel_array->tuples = malloc(rel_array->num_tuples * sizeof(tuple));
	CheckMalloc((*new_rel)->rel_array->tuples, "*new_rel->rel_array->tuples (preprocess.c)");

	//Initialize the array
	for (i = 0; i < rel_array->num_tuples; ++i)
	{
		(*new_rel)->rel_array->tuples[i].value = -1;
		(*new_rel)->rel_array->tuples[i].row_id = -1;
	}
	int insert_pos = 0;

	//Traverse through the original array
	for (i = 0; i < rel_array->num_tuples; ++i)
	{
		//Find the hash value of the current tuple
		hashed_value = HashFunction1((int32_t) rel_array->tuples[i].value, n_lsb);
		//Using the hash value find the insert position using the temp_sum
		insert_pos = temp_sum[hashed_value][1];
		if (insert_pos < 0 || insert_pos > rel_array->num_tuples)
		{
			printf("Error, hash value is outside of array borders!\n");
			exit(1);
		}

		if ((*new_rel)->rel_array->tuples[insert_pos].value == -1)
		{
			(*new_rel)->rel_array->tuples[insert_pos].value = rel_array->tuples[i].value;
			(*new_rel)->rel_array->tuples[insert_pos].row_id = rel_array->tuples[i].row_id;
		}
		else
		{
			printf("Error, insert_pos has already an assigned value\n");
			exit(1);
		}
		temp_sum[hashed_value][1] ++;//The insert_pos for the same bucket goes up 1 position
	}
	//Free temp_sum array
	for (i = 0; i < (*new_rel)->hist_size; ++i)
	{
		free(temp_sum[i]);
	}
	free(temp_sum);
}


void FreeReorderRelation(reordered_relation *rel)
{
	if (rel->hist_size > 0)
	{
		for (int i = 0; i < rel->hist_size; ++i)
		{
			free(rel->hist[i]);
			free(rel->psum[i]);
		}
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


int CheckMalloc(void* ptr, char* txt)
{
	if (ptr == NULL)
	{
		fprintf(stderr,"Error in malloc! In: %s \n",txt);
		exit(-1);
	}
	return 0;//if ptr != null return 0
}


void CheckBucketSizes(int** hist, int hist_size)
{
	int i, flag = 0;
	for (i = 0; i < hist_size; ++i)
	{
		if ((hist[i][1] * sizeof(tuple)) >= CACHE_SIZE)
		{
			flag = 1;
			break;
		}
	}
	if (flag == 1)
	{
		printf("Warning! Bucket size exceeds L1 cache size (aprox. 32 KB). Consider increasing the N_LSB for the H1 hash function.\n");
	}
}