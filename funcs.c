#include "funcs.h"
#include "stdlib.h"
#include <string.h>

result* RadixHashJoin(relation *relR, relation* relS)
{

}

int insert_result(struct result_listnode **head, result *res)
{
	//if the list is empty create the first node and insert the first result
	if( (*head) == NULL )
	{
		(*head)=malloc(sizeof(struct result_listnode));
		(*head)->current_load=sizeof(result);
		(*head)->next = NULL;
		memcpy((*head)->buff,res,sizeof(result));
	}
	//else find the first node with available space
	else
	{
		struct result_listnode *temp = (*head);
		while( (temp->current_load + sizeof(result)) >= 1048576)
		{
			if ( temp->next != NULL) temp = temp->next;
			//if all nodes are full create a new one
			else 
			{
				temp->next = malloc(sizeof(struct result_listnode));
				(temp)->current_load=sizeof(result);
				temp->next->next = NULL;
				memcpy(temp->buff,res,sizeof(result));
				return 0;
			}
		}
		//found the last, make the insertion
		char* buff_ptr = temp->buff;
		buff_ptr += temp->current_load;
		memcpy(buff_ptr, res, sizeof(result));
		temp->current_load += sizeof(result);
		return 0;
	}
}