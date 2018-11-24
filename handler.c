#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "query.h"
#include "structs.h"
#include "inter_res.h"
#include "filter.h"
#include "relation_map.h"
#include "relation_list.h"

int main(void)
{
	// First get all the relations from stdin
	char buff[250] = "start";
	int relations_count = 0;

	// The list that holds the names of the relation files
	relation_listnode *relation_list = NULL;
	printf("If you've completed adding relation, type Done .\n");
	while ( strcmp(buff,"Done") != 0 )
	{
		if (scanf("%s",buff) == EOF)
		{
			freopen("/dev/tty", "r", stdin);
        	scanf("%s",buff);
		}
		else
		{
			if ( !RelationListInsert(&relation_list,buff) ) relations_count ++;
			else fprintf(stderr, "RelationListInsert Error \n");
		}
	}
	PrintRelationList(relation_list);

	// From the relations given create a relation map
	relation_map *rel_map = malloc(relations_count * sizeof(relation_map));
	InitRelationMap(relation_list,rel_map);
	FreeRelationList(relation_list);

	sleep(1);

	// Start getting batches of querries
	printf("Give queries (or type Exit to quit):\n");
	freopen("/dev/tty", "r", stdin);
	batch_listnode *batch = NULL, *batch_temp = NULL;
	while ( fgets(buff,250,stdin) != NULL )
	{
		if (strcmp(buff, "Exit\n") == 0) break;
		// If F is given the end of the current batch is reached
		if (strcmp(buff,"F\n") == 0)
		{
			printf("End of the current batch\n");

			// EXECUTE THE QUERIES
			batch_temp = batch;
			PrintBatch(batch);


			while(batch_temp!=NULL)
			{
				ExecuteQuery(batch_temp,rel_map);
				batch_temp = batch_temp->next;
			}

			FreeBatch(batch);
			batch = NULL;
			printf("Give queries: (or type Exit to quit)\n");
		}
		// Else we are still on the same batch
		else
			InsertToQueryBatch(&batch, buff);
		printf("Give queries (or type Exit to quit):\n");
	}
	FreeRelationMap(rel_map, relations_count);
	printf("EXIT\n");
	return 0;
}
