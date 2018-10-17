#include <stdio.h>
#include <stdlib.h>
#include "structs.h"


// Takes a table and converts it to an array of tuples for faster join access 
relation* ToRow(int** OriginalArray, int NumOfRows, int RowToJoin, relation* NewRel)
{
	//Our new array
	for (int i = 0; i < NumOfRows; ++i)
	{
		NewRel->tuples[i].Value = OriginalArray[i][RowToJoin];
		NewRel->tuples[i].RowId = i;
	}
	return NewRel;
}

int main(int argc, char const *argv[])
{
	int NumOfRows = 20;
	int NumOfColumns = 40;
	int **OriginalArray = malloc(NumOfRows * sizeof(int*));
	relation* NewRel = malloc(sizeof(relation));
	NewRel->tuples = malloc(NumOfRows * sizeof(tuple));
	NewRel->num_tuples = NumOfRows;
	for (int i = 0; i < NumOfRows; ++i)
	{
		NewRel->tuples[i].Value = -1;
		NewRel->tuples[i].RowId = -1;
		OriginalArray[i] = malloc(NumOfColumns * sizeof(int));
		for (int j = 0; j < NumOfColumns; ++j)
		{
			OriginalArray[i][j] = i*j;
		}
	}

	NewRel = ToRow(OriginalArray, NumOfRows, 4, NewRel);
	for (int i = 0; i < NewRel->num_tuples; ++i)
	{
		printf("%d %d\n", NewRel->tuples[i].Value, NewRel->tuples[i].RowId);
	}



	//Free allocated space
	//Free Original array
	for (int i = 0; i < NumOfRows; ++i)
	{
		free(OriginalArray[i]);
	}
	free(OriginalArray);

	//Free NewRel
	free(NewRel->tuples);
	free(NewRel);
}