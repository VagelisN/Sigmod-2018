#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "inter_res.h"
#include "filter.h"
#include "results.h"
#include "preprocess.h"

int InsertFilterRes(result** res, tuple* tup)
{
  if( (*res) == NULL )
	{
		(*res)=malloc(sizeof(result));
		CheckMalloc((*res), "*head (results.c)");
		(*res)->buff = malloc(RESULT_MAX_BUFFER * sizeof(char));
		CheckMalloc((*res)->buff, "*head->buff (results.c)");
		(*res)->current_load = 1;
		(*res)->next = NULL;

		memcpy((*res)->buff, tup, sizeof(tuple));

	}
	//else find the first node with available space
	else
	{
		result *temp = (*res);
		while( ((temp->current_load*sizeof(tuple)) + sizeof(tuple)) > RESULT_MAX_BUFFER)
		{
			if ( temp->next != NULL) temp = temp->next;
			//if all nodes are full create a new one
			else
			{
				temp->next = malloc(sizeof(result));
				CheckMalloc(temp->next, "temp->next (results.c)");
				temp->next->buff = malloc(RESULT_MAX_BUFFER * sizeof(char));
				CheckMalloc(temp->next->buff, "temp->next->buff (results.c)");
				temp->next->current_load = 1;
				temp->next->next = NULL;
				memcpy(temp->next->buff, tup, sizeof(tuple));
				return 0;
			}
		}
		//found the last, make the insertion
		void* data = temp->buff;
		data += (temp->current_load*sizeof(tuple));
		memcpy(data, tup, sizeof(tuple));
		temp->current_load ++;
		return 0;
	}
}

int InsertFilterToInterResult(inter_res** head, int relation_num, result* res)
{
  // Find the number of the results in res
  int i, j, num_of_results = GetResultNum(res);

  if ((*head)->active_relations[relation_num] == -1)
  {
    /* If the relation is inactive insert all results to the inter_res */
    (*head)->active_relations[relation_num] = 1;
    (*head)->data->num_tuples = num_of_results;
    (*head)->data->table[relation_num] = malloc(num_of_results * sizeof(uint64_t));
    // Insert results one by one
    for (size_t i = 0; i < num_of_results; i++)
      (*head)->data->table[relation_num][i] = FindResultRowId(res, i);
  }
  else
  {
  	/* If the bucket is already active then remove the tuples that dont fulfil the filter */
  	//Allocate and initialise the new inter_data variable.
  	inter_data *temp_array = malloc(sizeof(inter_data));
  	temp_array->num_tuples = GetResultNum(res);
  	temp_array->table = malloc((*head)->num_of_relations * sizeof(int64_t*));
  	for (size_t i = 0; i < (*head)->num_of_relations; i++)
  	{
  		/* Allocate memory for all the active relations */
  		if((*head)->active_relations[i] == 1)
  			temp_array->table[i] = malloc((*head)->data->num_tuples * sizeof(int64_t));
  		else temp_array->table[i] = NULL;
  	}
  	//Insert the results.
  	for (size_t i = 0; i < (*head)->data->num_tuples; i++)
  	{
  		//Insert the results that are stored in the res variable.
  		int64_t temp = FindResultRowId(res, i);
  		/* temp is a rowId which refers to the current result's row_id in the inter_res data table.*/
  		temp_array->table[relation_num][i] = (*head)->data->table[relation_num][temp];
  		for (size_t j = 0; j < (*head)->num_of_relations; j++)
  			if ((relation_num != j) && (*head)->active_relations[j] == 1)
  				temp_array->table[j][i] = (*head)->data->table[j][temp];
  	}
  }
}


int Filter(inter_res** head, int relation_num, relation* rel, char comperator, int constant)
{
	result *filter_res = NULL;
	int i;
	switch(comperator)
	{
		case '>':
      /* For every tuple in the relation, if it satisfies the comperator
       * insert it to the filter_res */
			for (i = 0; i < rel->num_tuples; i++)
				if (rel->tuples[i].value > constant)
          InsertFilterRes(&filter_res, &(rel->tuples[i]));
			break;
		case '<':
			for (i = 0; i < rel->num_tuples; i++)
				if (rel->tuples[i].value < constant)
          InsertFilterRes(&filter_res, &(rel->tuples[i]));
			break;
		default:
			printf("Wrong comperator in filter function\n");
      return -1;
	}
  /* Insert the results to the intermediate_results data structure */
  InsertFilterToInterResult(head, relation_num, filter_res);
  FreeResult(filter_res);
}
