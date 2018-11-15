#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "inter_res.h"
#include "filter.h"
#include "results.h"
#include "preprocess.h"

int GetResultRowId(result *res, int num)
{
  int pos,total_load = 0;
  tuple *temp;
  /* Find the right bucket */
  while( (res != NULL) &&  ((total_load + res->current_load) < num))
  {
    total_load += res->current_load;
    res = res->next;
  }
  pos = num - total_load;
  //Find the right tuple
  temp = (tuple *) (res->buff + pos * sizeof(tuple));
  return temp->row_id;
}

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

int FindResultNum(result *res)
{
  int num_of_results = 0;
  while(res != NULL)
  {
    num_of_results += res->current_load;
    res = res->next;
  }
  return num_of_results;
}

/*
 * Need to figure out if i insert the filter results one by one
 * or whether i need to save the results in a list (?)
 */
int InsertFilterToInterResult(inter_res** head, int relation_num, result* res, int num_of_results)
{
	//(*head)->data->table[relation_num];
  if ((*head)->active_relations[relation_num] == -1)
  {
    /* If the relation is inactive insert all results to the inter_res */
    (*head)->active_relations[relation_num] = 1;
    (*head)->data->num_tuples = num_of_results;
    (*head)->data->table[relation_num] = malloc(num_of_results * sizeof(int64_t));
    // Insert results one by one
    for (size_t i = 0; i < num_of_results; i++)
      (*head)->data->table[relation_num][i] = GetResultRowId(res, i);
  }
  else
  {
    // Remove the results that are in head->data->table but not in the res
  }
}


int Filter(inter_res** head, int relation_num, relation* rel, char comperator, int constant)
{
	result *filter_res = NULL;
	int i;
	switch(constant)
	{
		case '>':
      /* For every tuple in the relation, if it satisfies the filter_res
       * insert it to the filter_res */
			for (i = 0; i < rel->num_tuples; ++i)
				if (rel->tuples[i].value > constant)
          InsertFilterRes(&filter_res, &(rel->tuples[i]));
			break;
		case '<':
			for (i = 0; i < rel->num_tuples; ++i)
				if (rel->tuples[i].value < constant)
          InsertFilterRes(&filter_res, &(rel->tuples[i]));
			break;
		default:
			printf("Wrong comperator in filter function\n");
      return -1;
	}
  // Find the number of the results in filter_res
  int num_of_results = FindResultNum(filter_res);
  // Call the InsertFilterToInterResult with the right
  InsertFilterToInterResult(head, relation_num, filter_res, num_of_results);
  FreeResult(filter_res);
}
