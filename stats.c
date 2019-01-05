#include "stats.h"
#include <stdlib.h>

void InitQueryStats(column_stats **query_stats,batch_listnode *curr_query, relation_map* rel_map)
{
	for (int i = 0; i < curr_query->num_of_relations; ++i)
	{
		query_stats[i] = malloc(rel_map[curr_query->relations[i]].num_columns * sizeof(column_stats));
		for (int j = 0; j < rel_map[curr_query->relations[i]].num_columns; ++j)
		{
			query_stats[i][j].l = rel_map[curr_query->relations[i]].col_stats[j].l;
			query_stats[i][j].u = rel_map[curr_query->relations[i]].col_stats[j].u;
			query_stats[i][j].f = rel_map[curr_query->relations[i]].col_stats[j].f;
			query_stats[i][j].d = rel_map[curr_query->relations[i]].col_stats[j].d;
		}
	}
}

void FreeQueryStats(column_stats **query_stats,batch_listnode *curr_query)
{
	for (int i = 0; i < curr_query->num_of_relations; ++i)
		free(query_stats[i]);
	free(query_stats);
}