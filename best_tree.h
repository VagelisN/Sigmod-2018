#ifndef BEST_TREE_H
#define BEST_TREE_H

#include "structs.h"

void InitBestTree(best_tree_node*** best_tree, int num_of_relations);

void FreeBestTree(best_tree_node **best_tree, int num_of_relations);

predicates_listnode* Connected(best_tree_node **best_tree, int rel_num, int S, int R, predicates_listnode *list);

predicates_listnode* JoinEnum(batch_listnode* curr_query, column_stats*** query_stats,relation_map* rel_map);

int CreateJoinTree(best_tree_node **dest, best_tree_node* source ,batch_listnode* curr_query,relation_map* rel_map,int s_new);

void CostTree(best_tree_node *curr_tree, batch_listnode* curr_query, predicates_listnode* pred,relation_map *rel_map);

#endif
