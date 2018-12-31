#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "results.h"
#include "preprocess.h"
#include "scheduler.h"




int main(void)
{
	// Malloc and initialize: original_array, array_R ,array_S
	int num_of_rows_R = 100;
	int num_of_rows_S = 200;
	int num_of_columns = 10;

	int **original_array_R = malloc(num_of_rows_R * sizeof(int*));

	int **original_array_S = malloc(num_of_rows_S * sizeof(int*));

	relation* array_R = malloc(sizeof(relation));

	array_R->tuples = malloc(num_of_rows_R * sizeof(tuple));

	array_R->num_tuples = num_of_rows_R;

	relation* array_S = malloc(sizeof(relation));

	array_S->tuples = malloc(num_of_rows_S * sizeof(tuple));

	array_S->num_tuples = num_of_rows_S;

	for (int i = 0; i < num_of_rows_R; ++i)
	{
		array_R->tuples[i].value = -1;
		array_R->tuples[i].row_id = -1;
		original_array_R[i] = malloc(num_of_columns * sizeof(int));
		for (int j = 0; j < num_of_columns; ++j) original_array_R[i][j] = i;
	}

	for (int i = 0; i < num_of_rows_S; ++i)
	{
		array_S->tuples[i].value = -1;
		array_S->tuples[i].row_id = -1;
		original_array_S[i] = malloc(num_of_columns * sizeof(int));
		for (int j = 0; j < num_of_columns; ++j) original_array_S[i][j] = i;
	}

	//Transform the original array to row stored array using relation struct

	/****************** Test case 1*******************/
	/* 
	 * Both columns have ones
	 * expected results: num_of_rows*num_of_rows
	 */
	
	/*for (int i = 0; i < num_of_rows; ++i)
	{

		original_array[i][1] = 1;
		original_array[i][0] = 1;
	}*/
	/****************** Test case 1*******************/


	/****************** Test case 2*******************/

	/* 
	 * The first column has odd nubers the second column has even numbers
	 * expected results: 0
	 */

	/*int j=1,k=0;
	for (int i = 0; i < num_of_rows; ++i)
	{

		original_array[i][1] = j;
		original_array[i][0] = k;
		k += 2;
		j += 2;
	}*/
	/****************** Test case 2*******************/


	/****************** Test case 3*******************/
	/*
	 * first half of the first column is equal to the second half of the second column
	 * expected results : num_of_rows/2 * num_of_rows/2
	 */
	
	/*for (int i = 0; i < num_of_rows/2; ++i)
	{
		original_array[i][1] = 2;
		original_array[i][0] = 1;
	}

	for (int i = num_of_rows/2; i < num_of_rows; ++i)
	{
		original_array[i][1] = 1;
		original_array[i][0] = 2;
	}*/
	/****************** Test case 3*******************/
	
	array_R = ToRow(original_array_R, 0, array_R);
	array_S = ToRow(original_array_S, 1, array_S);

	//Join the 2 row-stored arrays using RHJ
	RadixHashJoin(array_R, array_S);


	/*-------------------------Free allocated space-------------------------*/
	//Free Original array R
	for (int i = 0; i < num_of_rows_R; ++i)
	{
		free(original_array_R[i]);
	}
	free(original_array_R);

	//Free Original array R
	for (int i = 0; i < num_of_rows_S; ++i)
	{
		free(original_array_S[i]);
	}
	free(original_array_S);

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