#ifndef STATS_H
#define STATS_H

#include "structs.h"

/**/
void InitQueryStats(column_stats ***query_stats,batch_listnode *curr_query, relation_map* rel_map);

/**/
void FreeQueryStats(column_stats ***query_stats,batch_listnode *curr_query,relation_map* rel_map);

/**/
void ValuePredicate(column_stats ***query_stats,batch_listnode *curr_query,predicates_listnode* pred,relation_map* rel_map);

void PrintQueryStats(column_stats ***query_stats,batch_listnode *curr_query,relation_map*rel_map);

#endif