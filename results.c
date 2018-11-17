#include "results.h"
#include "preprocess.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


int InsertResult(result **head, result_tuples *res_tuples)
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

		memcpy((*head)->buff,res_tuples,sizeof(result_tuples));

	}
	//else find the first node with available space
	else
	{
		result *temp = (*head);
		while( ((temp->current_load*sizeof(result_tuples)) + sizeof(result_tuples)) > RESULT_MAX_BUFFER)
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
				memcpy(temp->next->buff,res_tuples,sizeof(result_tuples));
				return 0;
			}
		}
		//found the last, make the insertion
		void* data = temp->buff;
		data += (temp->current_load*sizeof(result_tuples));
		memcpy(data, res_tuples, sizeof(result_tuples));
		temp->current_load ++;
		return 0;
	}
}

int FindResultRowId(result *res, int num)
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

int GetResultNum(result *res)
{
  int num_of_results = 0;
  while(res != NULL)
  {
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
	result_tuples res_tuples;
	while(head!=NULL)
	{
		data = head->buff;
		temp_curr_load = head->current_load;
		while(temp_curr_load > 0)
		{
			memcpy(&res_tuples,data,sizeof(result_tuples));
			num_results++;
			printf("row_id R %2d value R %2d || " ,res_tuples.tuple_R.row_id,res_tuples.tuple_R.value);
			printf("row_id S %2d value S %2d\n" ,res_tuples.tuple_S.row_id,res_tuples.tuple_S.value);
			data += sizeof(result_tuples);
			temp_curr_load --;
		}
		head = head->next;
	}
	printf("Finished printing results!\n");
	printf("Number of rows in the result: %d\n",num_results );
	printf("-------------------------------\n");
}


void CheckResult(result* head)
{
	printf("------------------------------\n");
	int temp_curr_load;
	void* data;
	result_tuples temp_result;
	while(head != NULL)
	{
		data = head->buff;
		temp_curr_load = head->current_load;
		while(temp_curr_load > 0)
		{
			memcpy(&temp_result, data, sizeof(result_tuples));
			//Check the results
			if (temp_result.tuple_S.value != temp_result.tuple_R.value)
			{
				printf("Error! Failed in result check. R.value is different from S.value\n");
				exit(-2);
			}
			temp_curr_load --;
		}
		head = head->next;
	}
	printf("Result check completed succesfully!\n");
	printf("------------------------------\n");
}

result_tuples* FindResultTuples(result* head, int num)
{
	int count = 0;
	result_tuples* res_ptr = NULL;
	if(num < 0) return NULL;
	while(head!=NULL)
	{
		//result wanted is in this node's buffer
		if(head->current_load + count > num)
		{
			res_ptr = (result_tuples*)(head->buff + ( (num-count)*sizeof(result_tuples)));
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
