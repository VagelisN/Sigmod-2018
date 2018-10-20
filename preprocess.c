#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "preprocess.h"

// Takes a table and converts it to an array of tuples for faster join access 
relation* ToRow(int** original_array, int row_to_join, relation* NewRel)
{
	for (int i = 0; i < NewRel->num_tuples; ++i)
	{
		NewRel->tuples[i].Value = original_array[i][row_to_join];
		NewRel->tuples[i].RowId = i;
	}
	return NewRel;
}


/*
 * Reorders an row stored array so it is sorted based on the values that belong to the 
 * same bucket. Stores the ordered array , the histogram and the Psum array in a 
 * ReorderedRelation variable.
 */
void ReorderArray(relation* RelArray, int n_lsb, ReorderedRelation** NewRel)
{
	//Check the arguments
	if ((RelArray == NULL) || (RelArray->num_tuples <= 0) || (n_lsb <= 0))
	{
		printf("Error in ReorderArray. Invalid arguments\n");
		exit(1);
	}
	int i = 0, flag = 0;
	uint32_t hashed_value = 0;

	(*NewRel) = malloc(sizeof(ReorderedRelation));
	(*NewRel)->Hist_size = -1;
	//Find the size of the Psum and the Hist arrays
	(*NewRel)->Hist_size = 1;
	for (i = 0; i < n_lsb; ++i)
	{
		(*NewRel)->Hist_size *= 2;
	}
	
	// Allocate space for the Hist and Psum arrays
	(*NewRel)->Psum = malloc((*NewRel)->Hist_size * sizeof(int*));
	(*NewRel)->Hist = malloc((*NewRel)->Hist_size * sizeof(int*));
	
	//TempPsum array is used only in this function for faster reordering of the array
	int** TempPsum = malloc((*NewRel)->Hist_size * sizeof(int*));
	for (i = 0; i < (*NewRel)->Hist_size; ++i)
	{
		TempPsum[i] = malloc(2 * sizeof(int));
		(*NewRel)->Psum[i] = malloc(2 * sizeof(int));
		(*NewRel)->Psum[i][0] = i; //Bucket number
		(*NewRel)->Psum[i][1] = -1; //Initialize the starting point of each bucket in the reordered array to -1
		TempPsum[i][0] = i;
		TempPsum[i][1] = -1;

		(*NewRel)->Hist[i] = malloc(2 * sizeof(int));
		(*NewRel)->Hist[i][0] = i; // Bucket number
		(*NewRel)->Hist[i][1] = 0; // Each bucket starts with 0 allocated values
	}

	//Run RelArray with the hash function and build the histogram
	for (i = 0; i < RelArray->num_tuples; ++i)
	{
		hashed_value = HashFunction1((int32_t) RelArray->tuples[i].Value, n_lsb);
		(*NewRel)->Hist[hashed_value][1]++;
	}

	//Build the Psum array using the histogram
	int NewStartingPoint = 0;
	for (i = 0; i < (*NewRel)->Hist_size; ++i)
	{
		if ((*NewRel)->Psum[i][1] != -1)
		{
			printf("Error in initialization of Psum\n");
			exit(1);
		}
		/*
		 *If the current bucket has 0 values allocated to it then leave 
		 *Psum[CurrentBucket][1] to -1.
		*/
		if ((*NewRel)->Hist[i][1] > 0)
		{
			(*NewRel)->Psum[i][1] = NewStartingPoint;
			TempPsum[i][1] = NewStartingPoint;
			NewStartingPoint += (*NewRel)->Hist[i][1];
		}
	}
	/*
	printf("Hist:\n");
	for (i = 0; i < (*NewRel)->Hist_size; ++i)
	{
		printf("%d %d\n", (*NewRel)->Hist[i][0], (*NewRel)->Hist[i][1]);
	}
	printf("--------------------------------------\n");
	printf("Psum:\n");
	for (i = 0; i < (*NewRel)->Hist_size; ++i)
	{
		printf("%d %d\n", (*NewRel)->Psum[i][0], (*NewRel)->Psum[i][1]);
	}
	*/

	/*--------------------Build the reordered array----------------------*/

	//Allocate space for the ordered array in NewRel variable
	(*NewRel)->RelArray = malloc(sizeof(relation));
	(*NewRel)->RelArray->num_tuples = RelArray->num_tuples;
	(*NewRel)->RelArray->tuples = malloc(RelArray->num_tuples * sizeof(tuple));

	//Initialize the array
	for (i = 0; i < RelArray->num_tuples; ++i)
	{
		(*NewRel)->RelArray->tuples[i].Value = -1;
		(*NewRel)->RelArray->tuples[i].RowId = -1;
	}
	int InsertPos = 0;
	int NextActiveBucket = 0;
	int UpperBound = -1;

	//Traverse through the original array
	for (i = 0; i < RelArray->num_tuples; ++i)
	{
		//Find the hash value of the current tuple
		hashed_value = HashFunction1((int32_t) RelArray->tuples[i].Value, n_lsb);
		//Using the hash value find the insert position using the TempPsum
		InsertPos = TempPsum[hashed_value][1];
		if (InsertPos < 0 || InsertPos > RelArray->num_tuples)
		{
			printf("Error, hash value is outside of array borders!\n");
			exit(1);
		}

		if ((*NewRel)->RelArray->tuples[InsertPos].Value == -1)
		{
			(*NewRel)->RelArray->tuples[InsertPos].Value = RelArray->tuples[i].Value;
			(*NewRel)->RelArray->tuples[InsertPos].RowId = RelArray->tuples[i].RowId;
		}
		else
		{
			printf("Error, InsertPos has already an assigned value\n");
			exit(1);
		}
		TempPsum[hashed_value][1] ++;//The InsertPos for the same bucket goes up 1 position
	}
	//Free TempPsum array
	for (i = 0; i < (*NewRel)->Hist_size; ++i)
	{
		free(TempPsum[i]);
	}
	free(TempPsum);
}