#ifndef STATS_H
#define STATS_H

#include "structs.h"

/*
 * It initilizes query stats' stats of the columns that take part in the current query.
 * It copies the stats from the relation map.
 */
void InitQueryStats(column_stats ***query_stats,batch_listnode *curr_query, relation_map* rel_map);

/*
 * Takes a predicate and an already initialized column stats array and updates the stats of the
 * columns of the relations that take part in the predicate given with the expected values after
 * the execution of the predicate.
 */
void ValuePredicate(column_stats ***query_stats,batch_listnode *curr_query,predicates_listnode* pred,relation_map* rel_map);

/** Frees the allocated space of a collumn stats array */
void FreeQueryStats(column_stats ***query_stats,batch_listnode *curr_query,relation_map* rel_map);

#endif