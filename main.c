#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "rhjoin.h"
#include "results.h"
#include "preprocess.h"


int main(int argc, char const *argv[])
{
	// Malloc and initialize the original array
	int NumOfRows = 10;
	int NumOfColumns = 50;
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
			OriginalArray[i][j] = (i*j)/3;
		}
	}

	relation* NewRel2 = malloc(sizeof(relation));
	NewRel2->tuples = malloc(NumOfRows * sizeof(tuple));
	NewRel2->num_tuples = NumOfRows;
	for (int i = 0; i < NumOfRows; ++i)
	{
		NewRel2->tuples[i].Value = -1;
		NewRel2->tuples[i].RowId = -1;
		OriginalArray[i] = malloc(NumOfColumns * sizeof(int));
		for (int j = 0; j < NumOfColumns; ++j)
		{
			OriginalArray[i][j] = (i*j)/3;
		}
	}

	//Transform the original array to row stored array using relation struct
	NewRel = ToRow(OriginalArray, 4, NewRel);
	NewRel2 = ToRow(OriginalArray, 2, NewRel);
	/*for (int i = 0; i < 10; ++i)
	{
		printf("%d %d\n",NewRel->tuples[i].RowId,NewRel->tuples[i].Value );
	}
	for (int i = 0; i < 10; ++i)
	{
		printf("%d %d\n",NewRel2->tuples[i].RowId,NewRel2->tuples[i].Value );
	}*/



	ReorderedRelation* Reordered = malloc(sizeof(ReorderedRelation));
	Reordered->Hist_size = -1;
	
	//Reorder the original array and store it in Reordered 
	/*ReorderArray(NewRel, 3, &Reordered);

	for (int i = 0; i < NewRel->num_tuples; ++i)
	{
		printf("%2d %2d || %2d %2d\n", NewRel->tuples[i].Value, NewRel->tuples[i].RowId, Reordered->RelArray->tuples[i].Value, Reordered->RelArray->tuples[i].RowId);
	}*/

	RadixHashJoin(NewRel,NewRel2);

	/*-------------------------Free allocated space-------------------------*/


	//Free Original array
	for (int i = 0; i < NumOfRows; ++i)
	{
		free(OriginalArray[i]);
	}
	free(OriginalArray);

	//Free NewRel
	free(NewRel->tuples);
	free(NewRel);

	//Free Reordered
	/*if (Reordered != NULL)
	{
		//Free Psum and Hist
		if (Reordered->Hist_size != -1)
		{
			for (int i = 0; i < Reordered->Hist_size; ++i)
			{
				free(Reordered->Hist[i]);
				free(Reordered->Psum[i]);
			}
			free(Reordered->Psum);
			free(Reordered->Hist);
		}
		//Free RelArray
		if (Reordered->RelArray != NULL)
		{
			if (Reordered->RelArray->tuples != NULL)free(Reordered->RelArray->tuples);
			free(Reordered->RelArray);
		}
		free(Reordered);
	}*/
}