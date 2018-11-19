#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include "structs.h"
#include "rhjoin.h"
#include "results.h"
#include "preprocess.h"
#include "inter_res.h"
#include "filter.h"



int main(void)
{
	time_t t;
	srand(time(&t));
	// Malloc and initialize: original_array, array_R ,array_S
	int num_of_rows_R = 10;
	int num_of_rows_S = 20;
	int num_of_columns = 10;


	int **original_array_R = malloc(num_of_rows_R * sizeof(int*));
	CheckMalloc(original_array_R, "**original_array_R (main.c)");

	int **original_array_S = malloc(num_of_rows_S * sizeof(int*));
	CheckMalloc(original_array_S, "**original_array_S (main.c)");

	relation* array_R = malloc(sizeof(relation));
	CheckMalloc(array_R, "*array_R (main.c)");

	array_R->tuples = malloc(num_of_rows_R * sizeof(tuple));
	CheckMalloc(array_R->tuples, "array_R->tuples (main.c)");

	array_R->num_tuples = num_of_rows_R;

	relation* array_S = malloc(sizeof(relation));
	CheckMalloc(array_S, "*array_S (main.c)");

	array_S->tuples = malloc(num_of_rows_S * sizeof(tuple));
	CheckMalloc(array_S->tuples, "array_S->tuples (main.c)");

	array_S->num_tuples = num_of_rows_S;

	for (int i = 0; i < num_of_rows_R; ++i)
	{
		array_R->tuples[i].value = -1;
		array_R->tuples[i].row_id = -1;
		original_array_R[i] = malloc(num_of_columns * sizeof(int));
		CheckMalloc(original_array_R[i], "original_array_R[i] (main.c)");
		for (int j = 0; j < num_of_columns; ++j) original_array_R[i][j] = rand() % 50;
	}

	for (int i = 0; i < num_of_rows_S; ++i)
	{
		array_S->tuples[i].value = -1;
		array_S->tuples[i].row_id = -1;
		original_array_S[i] = malloc(num_of_columns * sizeof(int));
		CheckMalloc(original_array_S[i], "original_array_S[i] (main.c)");
		for (int j = 0; j < num_of_columns; ++j) original_array_S[i][j] = rand() % 50;
	}

	//Transform the original array to row stored array using relation struct
	array_R = ToRow(original_array_R, 0, array_R);
	array_S = ToRow(original_array_S, 1, array_S);

	result* results;

	/* RadixHashJoin must return the result* so we can access it accordingly */
	inter_res* intermediate = NULL;
	InitInterResults(&intermediate, 8);
	printf("Created intermediate results!\n");


	Filter(&intermediate, 0, array_R, '<', 20);


	//Join the 2 row-stored arrays using RHJ
	results = RadixHashJoin(array_R, array_S);
	PrintResult(results);
	InsertJoinToInterResults(&intermediate, 0, 1, results);
	printf("-------------------------------------------------\n\n");
	for (int i = 0; i < intermediate->data->num_tuples; i++) {
		printf("%d | %d\n",intermediate->data->table[0][i], intermediate->data->table[1][i] );
	}
	FreeResult(results);

	/*
	 take the results and insert them in the inter_res variable.
	 then update the functionality of the main function so it acts like a handler
	 which takes the queries , sets up the relations in the main memory , does a
	 basic querry optimization and then executes them in order while storing the results
	 the intermediate result variable. Debug everything that occurs.

	*/
	FreeInterResults(intermediate);

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
