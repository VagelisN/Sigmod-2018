#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "results.h"
#include "preprocess.h"




int main(int argc, char const *argv[])
{
	// Malloc and initialize: OriginalArray, array_R ,array_S
	int NumOfRows = 100000;
	int NumOfColumns = 10;


	int **OriginalArray = malloc(NumOfRows * sizeof(int*));
	CheckMalloc(OriginalArray, "**OriginalArray (main.c)");

	relation* array_R = malloc(sizeof(relation));
	CheckMalloc(array_R, "*array_R (main.c)");

	array_R->tuples = malloc(NumOfRows * sizeof(tuple));
	CheckMalloc(array_R->tuples, "array_R->tuples (main.c)");

	array_R->num_tuples = NumOfRows;

	relation* array_S = malloc(sizeof(relation));
	CheckMalloc(array_S, "*array_S (main.c)");

	array_S->tuples = malloc(NumOfRows * sizeof(tuple));
	CheckMalloc(array_S->tuples, "array_S->tuples (main.c)");

	array_S->num_tuples = NumOfRows;

	for (int i = 0; i < NumOfRows; ++i)
	{
		array_R->tuples[i].Value = -1;
		array_R->tuples[i].RowId = -1;
		array_S->tuples[i].Value = -1;
		array_S->tuples[i].RowId = -1;
		OriginalArray[i] = malloc(NumOfColumns * sizeof(int));
		CheckMalloc(OriginalArray[i], "OriginalArray[i] (main.c)");
		for (int j = 0; j < NumOfColumns; ++j) OriginalArray[i][j] = i;//(i*j)/3;
	}
	//Transform the original array to row stored array using relation struct

	/****************** Test case 1*******************/
	/* 
	 * Both columns have ones
	 * expected results: numofrows*numofrows
	 */
	
	/*for (int i = 0; i < NumOfRows; ++i)
	{

		OriginalArray[i][1] = 1;
		OriginalArray[i][0] = 1;
	}*/
	/****************** Test case 1*******************/

	/****************** Test case 2*******************/

	/* 
	 * The first column has odd nubers the second column has even numbers
	 * expected results: 0
	 */

	/*int j=1,k=0;
	for (int i = 0; i < NumOfRows; ++i)
	{

		OriginalArray[i][1] = j;
		OriginalArray[i][0] = k;
		k += 2;
		j += 2;
	}*/
	/****************** Test case 2*******************/

	/****************** Test case 3*******************/
	/*
	 * first half of the first column is equal to the second half of the second column
	 * expected results : NumOfRows/2 * NumOfRows/2
	 */
	
	/*for (int i = 0; i < NumOfRows/2; ++i)
	{
		OriginalArray[i][1] = 2;
		OriginalArray[i][0] = 1;
	}

	for (int i = NumOfRows/2; i < NumOfRows; ++i)
	{
		OriginalArray[i][1] = 1;
		OriginalArray[i][0] = 2;
	}*/
	/****************** Test case 3*******************/
	
	/*
	printf("Original array:\n");
	for (int i = 0; i < NumOfRows; ++i)
	{
		for (int j = 0; j < NumOfColumns; ++j)
		{

			printf("%d |", OriginalArray[i][j]);
		}
		printf("\n");
	}
	printf("-----------------------------------------------\n");
*/
	array_R = ToRow(OriginalArray, 0, array_R);
	array_S = ToRow(OriginalArray, 1, array_S);

	//Join the 2 row-stored arrays using RHJ
	RadixHashJoin(array_R, array_S);


	/*-------------------------Free allocated space-------------------------*/
	//Free Original array
	for (int i = 0; i < NumOfRows; ++i)
	{
		free(OriginalArray[i]);
	}
	free(OriginalArray);

	//Free array_S
	if (array_S != NULL)
	{
		free(array_S->tuples);
		free(array_S);
	}

	//Free array_R
	if (array_R != NULL)
	{
		free(array_R->tuples);
		free(array_R);
	}
}