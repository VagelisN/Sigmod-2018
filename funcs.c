#include "funcs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

result* RadixHashJoin(relation *relR, relation* relS)
{
	//Create Histogram,Psum,R',S'

	//create bucket , chain for the smaller array
	

}

uint32_t hash_function_1(int32_t num, int n)
{
	uint32_t mask = 0b11111111111111111111111111111111;
	mask = mask<<32-n;
	mask =mask >> 32-n;
	uint32_t hash_value = num & mask;
	return hash_value;
}

uint32_t hash_function_2(int32_t num)
{
	return num%31;
}

int init_index(index** ind, int num_bucket,int num_chain)
{
	(*ind) = malloc(sizeof(index));
	(*ind)->bucket = malloc(num_bucket*sizeof(int));
	(*ind)->chain = malloc(num_chain*sizeof(int));
	return 0;
}
















int insert_result(struct result_listnode **head, result *res)
{
	//if the list is empty create the first node and insert the first result
	if( (*head) == NULL )
	{
		(*head)=malloc(sizeof(struct result_listnode));
		(*head)->buff = malloc(RESULTLIST_MAX_BUFFER * sizeof(char));
		(*head)->current_load = 1;
		(*head)->next = NULL;
		memcpy((*head)->buff,res,sizeof(result));
	}
	//else find the first node with available space
	else
	{
		struct result_listnode *temp = (*head);
		printf("%ld\n",(temp->current_load*sizeof(result)) + sizeof(result));
		while( ((temp->current_load*sizeof(result)) + sizeof(result)) > RESULTLIST_MAX_BUFFER)
		{
			if ( temp->next != NULL) temp = temp->next;
			//if all nodes are full create a new one
			else 
			{
				temp->next = malloc(sizeof(struct result_listnode));
				temp->next->buff = malloc(RESULTLIST_MAX_BUFFER * sizeof(char));
				(temp)->next->current_load = 1;
				temp->next->next = NULL;
				memcpy(temp->next->buff,res,sizeof(result));
				return 0;
			}
		}
		//found the last, make the insertion
		void* data = temp->buff;
		data += (temp->current_load*sizeof(result));
		memcpy(data, res, sizeof(result));
		temp->current_load ++;
		return 0;
	}
}

void print_result_list(struct result_listnode* head)
{
	int temp_curr_load;
	void* data;
	result res;
	while(head!=NULL)
	{
		data = head->buff;
		temp_curr_load = head->current_load;
		printf("%d\n",temp_curr_load );
		while(temp_curr_load > 0)
		{
			memcpy(&res,data,sizeof(result));
			printf("Rowid R %d Rowid S %d\n" ,res.key_R,res.key_S );
			data += sizeof(result);
			temp_curr_load --;
		}
		head = head->next;
	}
}

void free_result_list(struct result_listnode* head)
{
	struct result_listnode* temp;
	while(head != NULL)
	{
		temp = head;
		head=head->next;
		free(temp->buff);
	}
}