#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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
				break;
			}
			if ( !RelationListInsert(&relation_list,buff) ) relations_count ++;
			else fprintf(stderr, "RelationListInsert Error %s\n",buff);
		}
	}
	PrintRelationList(relation_list);

	// From the relations given create a relation map
	relation_map *rel_map = malloc(relations_count * sizeof(relation_map));
	InitRelationMap(relation_list,rel_map);
	FreeRelationList(relation_list);

	sleep(1);

	// Start getting batches of querries
	fprintf(stderr,"Give queries (or type Exit to quit):\n");
	freopen("/dev/tty", "r", stdin);
	batch_listnode *batch = NULL, *batch_temp = NULL;


	//---------------------------------------------------------------------------
	/*
	inter_res *intermediate_result = NULL;
	InitInterResults(&intermediate_result, 4);
	printf("Joining 0.0=0.0\n");
	//relation* relR = GetRelation(0, 0 , intermediate_result,rel_map);
	//relation* relS = GetRelation(0, 1, intermediate_result, rel_map);
	SelfJoin(0, 0, 0, &intermediate_result, rel_map);
	//FreeRelation(relR);
	//FreeRelation(relS);
	//FreeResult(curr_res);
	relation *relR = NULL, *relS = NULL;result* curr_res = NULL;
	PrintInterResults(intermediate_result);
	printf("Going to sleep.\n" );
	sleep(10);

	printf("Joining 0.2=3.0\n");
	relR = GetRelation(0, 2, intermediate_result,rel_map);
	relS = GetRelation(3, 0, intermediate_result, rel_map);
	curr_res = RadixHashJoin(relR,relS);
	InsertJoinToInterResults(&intermediate_result, 0, 3, curr_res);
	FreeRelation(relR);
	FreeRelation(relS);
	FreeResult(curr_res);
	relR = NULL; relS = NULL; curr_res = NULL;
	PrintInterResults(intermediate_result);
/*	printf("Joining 1.0=2.0\n");
	relR = GetRelation(1, 0 , intermediate_result,rel_map);
	relS = GetRelation(2, 0, intermediate_result, rel_map);
	curr_res = RadixHashJoin(relR,relS);
	InsertJoinToInterResults(&intermediate_result, 1, 2, curr_res);
	FreeRelation(relR);
	FreeRelation(relS);
	FreeResult(curr_res);

*/
/*
	PrintInterResults(intermediate_result);
	sleep(25);
	printf("\n\n\n\nMerge inter_res\n\n\n\n");
	MergeInterNodes(&intermediate_result);
	PrintInterResults(intermediate_result);
	FreeInterResults(intermediate_result);
	*/

	// --------------------------------------
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
		}
		// Else we are still on the same batch
		else
			InsertToQueryBatch(&batch, buff);
		printf("Give queries or:\n-type F to finish the current batch\n-type Exit to quit\n");
	}
	FreeRelationMap(rel_map, relations_count);
	printf("EXIT\n");
	return 0;
}
