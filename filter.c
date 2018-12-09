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
  uint64_t num_of_results = GetResultNum(res);
  //printf("Filter num_of_results: %lu\n", num_of_results);
  do
  {
    // If its the first instance of the inter_res node
    if ((*head)->data->num_tuples == 0)
    {
      /* Insert all results to the inter_res */
      (*head)->data->num_tuples = num_of_results;
      (*head)->data->table[relation_num] = malloc(num_of_results * sizeof(uint64_t));
      // Insert results one by one
      for (uint64_t i = 0; i < num_of_results; i++)
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
    (*head) = (*head)->next;
  }while((*head) != NULL);
  //Allocate a new inter_res node
  InitInterResults(&(*head)->next, (*head)->num_of_relations);
  //Insert the results to the new node
  InsertSingleRowIdsToInterResult(&(*head)->next, relation_num, res);
}


result* Filter(inter_res* head,filter_pred* filter_p, relation_map* map,int *query_relations)
{
  result *res = NULL;
  uint64_t relation = filter_p->relation,column = filter_p->column;
  uint64_t* col = map[query_relations[relation]].columns[column];

  int found_flag = 1;
  while(found_flag == 1 && head !=NULL)
  {
    if (head->data->table[relation] != NULL)
      found_flag = 0;
    else head = head->next;
  }

  uint64_t i;
  switch(filter_p->comperator)
  {
    case '>':
      /* For every tuple in the relation, if it satisfies the comperator
       * insert it to the filter_res */
      if (found_flag == 1)
      {
        for (i = 0; i < map[query_relations[relation]].num_tuples; i++)
          if (col[i] > filter_p->value)
            InsertRowIdResult(&res, &i);
      }
      else
      {
        for (i = 0; i < head->data->num_tuples; ++i)
        if ( col[head->data->table[relation][i]] > filter_p->value)
        InsertRowIdResult(&res, &i);
      }
      break;
    case '<':
      if (found_flag == 1)
      {
        for (i = 0; i < map[query_relations[relation]].num_tuples; i++)
          if (col[i] < filter_p->value)
            InsertRowIdResult(&res, &i);
      }
      else
      {
        for (i = 0; i < head->data->num_tuples; ++i)
        if ( col[head->data->table[relation][i]] < filter_p->value)
        InsertRowIdResult(&res, &i);
      }
      break;
    case '=':
      if (found_flag == 1)
      {
        for (i = 0; i < map[query_relations[relation]].num_tuples; i++)
          if (col[i] == filter_p->value)
            InsertRowIdResult(&res, &i);
      }
      else
      {
        for (i = 0; i < head->data->num_tuples; ++i)
        if ( col[head->data->table[relation][i]] == filter_p->value)
        InsertRowIdResult(&res, &i);
      }
      break;
    default:
      printf("Wrong comperator in filter function\n");
      exit(2);
  }
  /* Insert the results to the intermediate_results data structure */
  return res;
}
