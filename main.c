#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "results.h"
#include "preprocess.h"




int main(int argc, char const *argv[])
{
	// Malloc and initialize: OriginalArray, array_R ,array_S
	int NumOfRows = 100;
	int NumOfColumns = 5;
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
	printf("Original array:\n");
	for (int i = 0; i < NumOfRows; ++i)
	{
		array_R->tuples[i].Value = -1;
		array_R->tuples[i].RowId = -1;
		array_S->tuples[i].Value = -1;
		array_S->tuples[i].RowId = -1;
		OriginalArray[i] = malloc(NumOfColumns * sizeof(int));
		CheckMalloc(OriginalArray[i], "OriginalArray[i] (main.c)");
		for (int j = 0; j < NumOfColumns; ++j)
		{
			OriginalArray[i][j] = i;//(i*j)/3;
			printf("%d |", OriginalArray[i][j]);
		}
		printf("\n");
	}
	printf("-----------------------------------------------\n");
	//Transform the original array to row stored array using relation struct
	array_R = ToRow(OriginalArray, 0, array_R);
	array_S = ToRow(OriginalArray, 0, array_S);

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