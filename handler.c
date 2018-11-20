#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
//#include "handler.h"
#include "structs.h"
#include "inter_res.h"
#include "filter.h"
#include "relation_map.h"
#include "relation_list.h"
#include "query.h"

int main(void)
{
	// First get all the relations from stdin
	char buff[250] = "start";
	int relations_count = 0;

	// The list that holds the names of the relation files
	relation_listnode *relation_list = NULL;

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

	sleep(1);

	// Start getting batches of querries
	printf("Give queries:\n");
	freopen("/dev/tty", "r", stdin);
	while ( fgets(buff,250,stdin) != NULL )
	{
		printf("buff %s\n",buff );
		// If F is given the end of the current batch is reached
		if (strcmp(buff,"F") == 0)
			printf("End of the current batch\n");

		// Else we are still on the same batch
		else
		{
			//buff now has a querry
			query *curr_query = NULL;
			printf("%s\n",buff );
			if (ReadQuery(&curr_query, buff) != 0)
			{
				fprintf(stderr, "Read Querry error\n");
				exit(1);
			}
			else
			{
				query_listnode *query_list = NULL;
				for (int i = 0; i < curr_query->predicates->num_of_elements; ++i)
				{	
					InsertPredicate(&query_list,curr_query->predicates->data[i]);
				}
			}
		}
	}
	return 0;
}