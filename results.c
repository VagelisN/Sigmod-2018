#include "results.h"
#include "preprocess.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


int InsertResult(result **head, result_tuple *res_tuple)
{
	//if the list is empty create the first node and insert the first result
	if( (*head) == NULL )
	{
		(*head)=malloc(sizeof(result));
		CheckMalloc((*head), "*head (results.c)");
		(*head)->buff = malloc(RESULT_MAX_BUFFER * sizeof(char));
		CheckMalloc((*head)->buff, "*head->buff (results.c)");
		(*head)->current_load = 1;
		(*head)->next = NULL;

		memcpy((*head)->buff,res_tuple,sizeof(result_tuple));

	}
	//else find the first node with available space
	else
	{
		result *temp = (*head);
		while( ((temp->current_load*sizeof(result_tuple)) + sizeof(result_tuple)) > RESULT_MAX_BUFFER)
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
				memcpy(temp->next->buff,res_tuple,sizeof(result_tuple));
				return 0;
			}
		}
		//found the last, make the insertion
		void* data = temp->buff;
		data += (temp->current_load*sizeof(result_tuple));
		memcpy(data, res_tuple, sizeof(result_tuple));
		temp->current_load ++;
		return 0;
	}
}


uint64_t FindResultRowId(result *res, int num)
{
  int pos,total_load = 0;
  uint64_t *row_id;
  /* Find the right bucket */
  while( (res != NULL) &&  ((total_load + res->current_load) < num))
  {
    total_load += res->current_load;
    res = res->next;
  }
  pos = num - total_load;
  //Find the right tuple
  row_id = (uint64_t *) (res->buff + pos * sizeof(uint64_t));
	return *row_id;
}

int GetResultNum(result *res)
{
  uint64_t num_of_results = 0;
  while(res != NULL)
  {
		//printf("In GetResultNum, current_load: %lu\n", res->current_load);
		//printf("Max number of result_tuples in a node = %lu\n", 1048576/sizeof(result_tuples));
    num_of_results += res->current_load;
    res = res->next;
  }
  return num_of_results;
}

void PrintResult(result* head)
{
	printf("------------------------------\n");
	printf("Printing results:\n");
	int temp_curr_load, num_results = 0;
	void* data;
	result_tuple res_tuple;
	while(head!=NULL)
	{
		data = head->buff;
		temp_curr_load = head->current_load;
		while(temp_curr_load > 0)
		{
			memcpy(&res_tuple,data,sizeof(result_tuple));
			num_results++;
			printf("row_id R %lu value S %lu || " ,res_tuple.row_idR, res_tuple.row_idS);
			data += sizeof(result_tuple);
			temp_curr_load --;
		}
		head = head->next;
	}
	printf("Finished printing results!\n");
	printf("Number of rows in the result: %d\n",num_results );
	printf("-------------------------------\n");
}

void PrintSelfResult(result* head)
{
	printf("------------------------------\n");
	printf("Printing results:\n");
	int temp_curr_load, num_results = 0;
	uint64_t* data;
	uint64_t row_id;
	while(head!=NULL)
	{
		data = (uint64_t*)head->buff;
		temp_curr_load = head->current_load;
		while(temp_curr_load > 0)
		{
			num_results++;
			printf("row_id %2ld",*data);
			data++;
			temp_curr_load --;
		}
		head = head->next;
	}
	printf("Finished printing results!\n");
	printf("Number of rows in the result: %d\n",num_results );
	printf("-------------------------------\n");
}

result_tuple* FindResultTuples(result* head, int num)
{
	int count = 0;
	result_tuple* res_ptr = NULL;
	if(num < 0) return NULL;
	while(head!=NULL)
	{
		//result wanted is in this node's buffer
		if(head->current_load + count > num)
		{
			res_ptr = (result_tuple*)(head->buff + ( (num-count)*sizeof(result_tuple)));
			return res_ptr;
		}
		count += head->current_load;
		head = head->next;
	}
}

void FreeResult(result* head)
{
	result* temp;
	while(head != NULL)
	{
		temp = head;
		head=head->next;
		free(temp->buff);
		free(temp);
	}
}

int InsertRowIdResult(result **head, uint64_t *row_id)
{
	//if the list is empty create the first node and insert the first result
	if( (*head) == NULL )
	{
		(*head)=malloc(sizeof(result));
		CheckMalloc((*head), "*head (results.c)");
		(*head)->buff = malloc(RESULT_MAX_BUFFER * sizeof(char));
		CheckMalloc((*head)->buff, "*head->buff (results.c)");
		(*head)->current_load = 1;
		(*head)->next = NULL;


		memcpy((*head)->buff, row_id, sizeof(uint64_t));
	}
	//else find the first node with available space
	else
	{
		result *temp = (*head);
		while( ((temp->current_load*sizeof(uint64_t)) + sizeof(uint64_t)) > RESULT_MAX_BUFFER)
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
				memcpy(temp->next->buff,row_id,sizeof(uint64_t));
				return 0;
			}
		}
		//found the last, make the insertion
		void* data = temp->buff;
		data += (temp->current_load*sizeof(uint64_t));
		memcpy(data, row_id, sizeof(uint64_t));
		temp->current_load ++;
		return 0;
	}
}

void PrintRelation(relation* rel)
{
	printf("\nprinting relation:\n\n");
	for (int i = 0; i < rel->num_tuples; ++i)
	{
		printf("Row Id: %ld Value %ld\n",rel->tuples[i].row_id, rel->tuples[i].value );
	}
	printf("\nfinished printing relation\n\n");
}