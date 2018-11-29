#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "query.h"
#include "structs.h"
#include "inter_res.h"
#include "filter.h"
#include "results.h"
#include "preprocess.h"

int InsertSingleRowIdsToInterResult(inter_res** head, int relation_num, result* res)
{
  // Find the number of the results in res
  int i, j;
  uint64_t num_of_results = GetResultNum(res);
  //printf("Filter num_of_results: %lu\n", num_of_results);
  while(1)
  {
    // If its the first instance of the inter_res node
    if ((*head)->data->num_tuples == 0)
    {
      /* Insert all results to the inter_res */
      (*head)->data->num_tuples = num_of_results;
      (*head)->data->table[relation_num] = malloc(num_of_results * sizeof(uint64_t));
      // Insert results one by one
      for (size_t i = 0; i < num_of_results; i++)
        (*head)->data->table[relation_num][i] = FindResultRowId(res, i);
      return 1;
    }
    else if((*head)->data->table[relation_num] != NULL)
    {
    	/* If the bucket is already active then remove the tuples that dont fulfil the filter */
    	//Allocate and initialise the new inter_data variable.
      inter_data *temp_array = NULL;
      (*head)->data->num_tuples = GetResultNum(res);
      InitInterData(&temp_array, (*head)->num_of_relations, (*head)->data->num_tuples);
    	for (size_t i = 0; i < (*head)->num_of_relations; i++)
    	{
    		/* Allocate memory for all the active relations */
    		if((*head)->data->table[i] != NULL)
    			temp_array->table[i] = malloc((*head)->data->num_tuples * sizeof(uint64_t));
    	}
    	//Insert the results.
    	for (size_t i = 0; i < (*head)->data->num_tuples; i++)
    	{
    		//Insert the results that are stored in the res variable.
    		uint64_t temp = FindResultRowId(res, i);
    		/* temp is a rowId which refers to the current result's row_id in the inter_res data table.*/
    		temp_array->table[relation_num][i] = (*head)->data->table[relation_num][temp];
    		for (size_t j = 0; j < (*head)->num_of_relations; j++)
    			if ((relation_num != j) && (*head)->data->table[j] != NULL)
    				temp_array->table[j][i] = (*head)->data->table[j][temp];
    	}
      FreeInterData((*head)->data, (*head)->num_of_relations);
      (*head)->data = temp_array;
      return 1;
    }
    if ((*head)->next == NULL) break;
    (*head) = (*head)->next;
  }
  //Allocate a new inter_res node
  InitInterResults(&(*head)->next, (*head)->num_of_relations);
  //Insert the results to the new node
  InsertSingleRowIdsToInterResult(&(*head)->next, relation_num, res);
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
          InsertRowIdResult(&filter_res, &(rel->tuples[i].row_id));
			break;
		case '<':
			for (i = 0; i < rel->num_tuples; i++)
				if (rel->tuples[i].value < constant)
          InsertRowIdResult(&filter_res, &(rel->tuples[i].row_id));
			break;
    case '=':
      for (i = 0; i < rel->num_tuples; i++)
        if (rel->tuples[i].value == constant)
          InsertRowIdResult(&filter_res, &(rel->tuples[i].row_id));
      break;
		default:
			printf("Wrong comperator in filter function\n");
      return -1;
	}
  /* Insert the results to the intermediate_results data structure */
  InsertSingleRowIdsToInterResult(head, relation_num, filter_res);
  FreeResult(filter_res);
}
