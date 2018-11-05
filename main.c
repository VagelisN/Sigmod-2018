#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "results.h"
#include "preprocess.h"




int main(void)
{
	// Malloc and initialize: OriginalArray, array_R ,array_S
	int NumOfRowsR = 100;
	int NumOfRowsS = 200;
	int NumOfColumns = 10;


	int **OriginalArrayR = malloc(NumOfRowsR * sizeof(int*));
	CheckMalloc(OriginalArrayR, "**OriginalArrayR (main.c)");

	int **OriginalArrayS = malloc(NumOfRowsS * sizeof(int*));
	CheckMalloc(OriginalArrayS, "**OriginalArrayS (main.c)");

	relation* array_R = malloc(sizeof(relation));
	CheckMalloc(array_R, "*array_R (main.c)");

	array_R->tuples = malloc(NumOfRowsR * sizeof(tuple));
	CheckMalloc(array_R->tuples, "array_R->tuples (main.c)");

	array_R->num_tuples = NumOfRowsR;

	relation* array_S = malloc(sizeof(relation));
	CheckMalloc(array_S, "*array_S (main.c)");

	array_S->tuples = malloc(NumOfRowsS * sizeof(tuple));
	CheckMalloc(array_S->tuples, "array_S->tuples (main.c)");

	array_S->num_tuples = NumOfRowsS;

	for (int i = 0; i < NumOfRowsR; ++i)
	{
		array_R->tuples[i].Value = -1;
		array_R->tuples[i].RowId = -1;
		OriginalArrayR[i] = malloc(NumOfColumns * sizeof(int));
		CheckMalloc(OriginalArrayR[i], "OriginalArrayR[i] (main.c)");
		for (int j = 0; j < NumOfColumns; ++j) OriginalArrayR[i][j] = i;
	}
	for (int i = 0; i < NumOfRowsS; ++i)
	{
		array_S->tuples[i].Value = -1;
		array_S->tuples[i].RowId = -1;
		OriginalArrayS[i] = malloc(NumOfColumns * sizeof(int));
		CheckMalloc(OriginalArrayS[i], "OriginalArrayS[i] (main.c)");
		for (int j = 0; j < NumOfColumns; ++j) OriginalArrayS[i][j] = i;
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
	
	array_R = ToRow(OriginalArrayR, 0, array_R);
	array_S = ToRow(OriginalArrayS, 1, array_S);

	//Join the 2 row-stored arrays using RHJ
	RadixHashJoin(array_R, array_S);


	/*-------------------------Free allocated space-------------------------*/
	//Free Original array R
	for (int i = 0; i < NumOfRowsR; ++i)
	{
		free(OriginalArrayR[i]);
	}
	free(OriginalArrayR);

	//Free Original array R
	for (int i = 0; i < NumOfRowsS; ++i)
	{
		free(OriginalArrayS[i]);
	}
	free(OriginalArrayS);

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