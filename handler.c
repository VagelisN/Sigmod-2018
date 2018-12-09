#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include "query.h"
#include "structs.h"
#include "rhjoin.h"
#include "results.h"
#include "preprocess.h"
#include "inter_res.h"
#include "filter.h"
#include "relation_map.h"
#include "relation_list.h"

int main(void)
{
	// First get all the relations from stdin
	char buff[250] = "start";
	int relations_count = 0;
	int done_flag = 0;
	// The list that holds the names of the relation files
	relation_listnode *relation_list = NULL;
	fprintf(stderr,"If you've completed adding relation, type Done .\n");
	while (done_flag == 0)
	{
		if (scanf("%s",buff) == EOF)
			freopen("/dev/tty", "r", stdin);
		else
		{
			if (strcmp(buff,"Done") == 0 )
			{
				done_flag = 1;
				fflush(stdin);
				break;
			}
			if ( !RelationListInsert(&relation_list,buff) ) relations_count ++;
			else fprintf(stderr, "RelationListInsert Error %s\n",buff);
		}
	}
	//SPrintRelationList(relation_list);

	// From the relations given create a relation map
	relation_map *rel_map = malloc(relations_count * sizeof(relation_map));
	InitRelationMap(relation_list,rel_map);
	FreeRelationList(relation_list);

	// Start getting batches of querries
	//fprintf(stderr,"Give queries (or type Exit to quit):\n");
	//freopen("/dev/tty", "r", stdin);
	batch_listnode *batch = NULL, *batch_temp = NULL;

	clock_t time_taken = clock();
	while (1)
	{
		fflush(stdout);
		if(fgets(buff,250,stdin) == NULL )
		{
			break;
		}
 		if(strlen(buff) < 2);
 		else
		{
			if (strcmp(buff, "Exit\n") == 0) break;
			// If F is given the end of the current batch is reached
			if (strcmp(buff,"F\n") == 0)
			{
				//Execute the queries 
				batch_temp = batch;
				while(batch_temp!=NULL)
				{
					ExecuteQuery(batch_temp,rel_map);
					batch_temp = batch_temp->next;
				}
				FreeBatch(batch);
				batch = NULL;
			}
			// Else we are still on the same batch
			else
			{
				InsertToQueryBatch(&batch, buff);
			}
			//fprintf(stderr,"Give queries or:\n-type F to finish the current batch\n-type Exit to quit\n");
		}
	}
	time_taken = clock() - time_taken;
	fprintf(stderr, "Total running time is: %.2f seconds.\n", ((double)time_taken)/CLOCKS_PER_SEC);
	FreeRelationMap(rel_map, relations_count);
	return 0;
}
