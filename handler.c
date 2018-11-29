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
	//PrintRelationList(relation_list);

	// From the relations given create a relation map
	relation_map *rel_map = malloc(relations_count * sizeof(relation_map));
	InitRelationMap(relation_list,rel_map);
	FreeRelationList(relation_list);

	// Start getting batches of querries
	fprintf(stderr,"Give queries (or type Exit to quit):\n");
	//freopen("/dev/tty", "r", stdin);
	batch_listnode *batch = NULL, *batch_temp = NULL;


	//---------------------------------------------------------------------------
	/*int relations[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
	inter_res *intermediate_result = NULL;
	InitInterResults(&intermediate_result, 10);

	//Filter 3.2 > 3499
	printf("Filter 3.2>3499\n");
	relation* relR = GetRelation(3, 2 , intermediate_result, rel_map, relations);
	Filter(&intermediate_result, 3, relR, '>', 3499);
	FreeRelation(relR);
	relation *relS = NULL;result* curr_res = NULL;
	//PrintInterResults(intermediate_result);
	printf("Going to sleep. Above is the first instance of inter_res after the filter.\n" );

	//Join 0.0=3.2
	relR = GetRelation(3, 2, intermediate_result, rel_map, relations);
	relS = GetRelation(0, 0, intermediate_result, rel_map, relations);
	curr_res = RadixHashJoin(relR, relS);

	//PrintResult(curr_res);
	//exit(1);
	printf("Num_tuples in inter_res: %lu\n", intermediate_result->data->num_tuples);
	InsertJoinToInterResults(&intermediate_result, 3, 0, curr_res);
	printf("Num_tuples in inter_res: %lu\n", intermediate_result->data->num_tuples);
	//PrintInterResults(intermediate_result);
	//exit(1);
	FreeRelation(relR);
	FreeRelation(relS);
	FreeResult(curr_res);

	//Join  3.1=1.0
	relR = GetRelation(3, 1, intermediate_result, rel_map, relations);
	relS = GetRelation(1, 0, intermediate_result, rel_map, relations);
	curr_res = RadixHashJoin(relR, relS);
	InsertJoinToInterResults(&intermediate_result, 3, 1, curr_res);
	FreeRelation(relR);
	FreeRelation(relS);
	FreeResult(curr_res);
	batch_listnode* query = malloc(sizeof(batch_listnode));
	query->num_of_relations = 10;
	query->relations = (int*)&relations;
	query->views = malloc(sizeof(query_string_array));
	query->views->num_of_elements = 2;
	query->views->data = malloc(2 * sizeof(char*));
	query->views->data[0] = malloc(5 * sizeof(char));
	query->views->data[0][0] = '0';
	query->views->data[0][1] = '.';
	query->views->data[0][2] = '2';
	query->views->data[0][3] = '\0';
	query->views->data[1] = malloc(5 * sizeof(char));
	query->views->data[1][0] = '3';
	query->views->data[1][1] = '.';
	query->views->data[1][2] = '1';
	query->views->data[1][3] = '\0';
	CalculateQueryResults(intermediate_result, rel_map, query);*/
	// --------------------------------------
	while (fgets(buff,250,stdin) != NULL )
	{
		fprintf(stderr, "BUFF %s\n",buff );
 		if(strlen(buff) < 2)
 			fprintf(stderr,"Input too small\n");
 		else
		{
			if (strcmp(buff, "Exit\n") == 0) break;
			// If F is given the end of the current batch is reached
			if (strcmp(buff,"F\n") == 0)
			{
				fprintf(stderr,"End of the current batch\n");

				// EXECUTE THE QUERIES
				batch_temp = batch;
				//PrintBatch(batch);


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
			fprintf(stderr,"Give queries or:\n-type F to finish the current batch\n-type Exit to quit\n");
		}
	}
	//FreeInterResults(intermediate_result);
	fprintf(stderr, "Exited main\n" );
	FreeRelationMap(rel_map, relations_count);
	return 0;
}
