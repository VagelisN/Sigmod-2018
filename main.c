#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>

#include "relation_list.h"
#include "relation_map.h"
#include "structs.h"
#include "rhjoin.h"
#include "results.h"
#include "preprocess.h"
#include "inter_res.h"
#include "filter.h"


int main(void)
{
	char buff[250];
	int relations_count = 0;
	relation_listnode *relation_list = NULL;
	while (scanf("%s",buff) != EOF)
	{
		if ( !RelationListInsert(&relation_list,buff) ) relations_count ++;
		else fprintf(stderr, "RelationListInsert Error \n");
	}
	PrintRelationList(relation_list);

	relation_map *rel_map = malloc(relations_count * sizeof(relation_map));
	InitRelationMap(relation_list,rel_map);
	//PrintRelationMap(rel_map,relations_count);

	inter_res* intermediate = NULL;
	InitInterResults(&intermediate, 2);

	// Filter relation's 0 column 0 with values lower of 500
	relation *rel1 = GetRelation(0, 0, intermediate, rel_map);
	Filter(&intermediate, 0, rel1, '<', 500);
	free(rel1->tuples);
	free(rel1);

	// Join 0.1 = 1.0
	rel1 = GetRelation(0, 1, intermediate, rel_map);
	relation *rel2 = GetRelation(1, 0, intermediate, rel_map);
	result *res = RadixHashJoin(rel1, rel2);
	InsertJoinToInterResults(&intermediate, 0, 1, res);

	FreeInterResults(intermediate);
	FreeRelationMap(rel_map,relations_count);
	FreeRelationList(relation_list);
	return 0;
}
