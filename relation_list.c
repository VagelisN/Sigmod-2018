#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include "relation_list.h"
#include <errno.h>


int RelationListInsert(relation_listnode** head,char* relation_text)
{
	//if the list is empty create the first node and insert the first relation
	if( (*head) == NULL )
	{
		(*head) = malloc(sizeof(relation_listnode));
		//CheckMalloc((*head), "*head (relation_list.c)");

		(*head)->filename = malloc((strlen(relation_text) + 1)*sizeof(char));
		//CheckMalloc((*head)->filename, "*head->filename (relation_list.c)");
		strcpy((*head)->filename,relation_text);

		if((*head)->fd = open(relation_text,O_RDONLY) == -1)
		{

			fprintf(stderr,"open error %s : %s\n\n",relation_text, strerror(errno));
			return 1;
		} 
		(*head)->next = NULL;

		return 0;
	}
	//else find the last node
	else
	{
		relation_listnode *temp = (*head);
		while(temp->next != NULL)temp=temp->next;

		temp->next = malloc(sizeof(relation_listnode));
		//CheckMalloc(temp->next, "temp->next (relation_list.c)");

		temp->next->filename = malloc(strlen(relation_text) + 1);
		//CheckMalloc(temp->next->filename, "temp->next->filename (relation_list.c)");

		strcpy(temp->next->filename,relation_text);
		if(temp->next->fd = open(relation_text,O_RDONLY) == -1)
		{
			fprintf(stderr,"open error %s : %s\n\n",relation_text, strerror(errno));
			return 1;
		}
		temp->next->next = NULL;
		return 0;
	}
}

void FreeRelationList(relation_listnode* head)
{
	relation_listnode *temp;
	while(head != NULL)
	{
		temp=head;
		free(temp->filename);
		close(temp->fd);
		head=head->next;
		free(temp);

	}
}

void PrintRelationList(relation_listnode* head)
{
	while(head != NULL)
	{
		printf("%s\n",head->filename );
		head = head->next;
	}
}
