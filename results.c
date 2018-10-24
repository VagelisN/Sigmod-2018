#include "results.h"
#include "preprocess.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


int InsertResult(struct result **head, result_tuples *res_tuples)
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
		struct result *temp = (*head);
		while( ((temp->current_load*sizeof(result_tuples)) + sizeof(result_tuples)) > RESULT_MAX_BUFFER)
		{
			if ( temp->next != NULL) temp = temp->next;
			//if all nodes are full create a new one
			else 
			{
				temp->next = malloc(sizeof(struct result));
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
			printf("Rowid R %2d Value R %2d || " ,res_tuples.tuple_R.RowId,res_tuples.tuple_R.Value);
			printf("Rowid S %2d Value S %2d\n" ,res_tuples.tuple_S.RowId,res_tuples.tuple_S.Value);
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
			if (temp_result.tuple_S.Value != temp_result.tuple_R.Value)
			{
				printf("Error! Failed in result check. R.Value is different from S.Value\n");
				exit(-2);
			}
			temp_curr_load --;
		}
		head = head->next;
	}
	printf("Result check completed succesfully!\n");
	printf("------------------------------\n");
}

void FreeResult(struct result* head)
{
	struct result* temp;
	while(head != NULL)
	{
		temp = head;
		head=head->next;
		free(temp->buff);
		free(temp);
	}
}