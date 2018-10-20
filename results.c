#include "results.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


int InsertResult(struct result **head, result_tuples *res_tuples)
{
	//if the list is empty create the first node and insert the first result
	if( (*head) == NULL )
	{
		(*head)=malloc(sizeof(result));

		(*head)->buff = malloc(RESULT_MAX_BUFFER * sizeof(char));
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
				temp->next->buff = malloc(RESULT_MAX_BUFFER * sizeof(char));
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

void PrintResult(struct result* head)
{
	int temp_curr_load;
	void* data;
	result_tuples res_tuples;
	while(head!=NULL)
	{
		data = head->buff;
		temp_curr_load = head->current_load;
		while(temp_curr_load > 0)
		{
			memcpy(&res_tuples,data,sizeof(result_tuples));
			printf("Rowid R %d Value R %d || " ,res_tuples.tuple_R.RowId,res_tuples.tuple_R.Value);
			printf("Rowid S %d Value S %d\n" ,res_tuples.tuple_S.RowId,res_tuples.tuple_S.Value);
			data += sizeof(result_tuples);
			temp_curr_load --;
		}
		head = head->next;
	}
}

void FreeResult(struct result* head)
{
	struct result* temp;
	while(head != NULL)
	{
		temp = head;
		head=head->next;
		free(temp->buff);
	}
}