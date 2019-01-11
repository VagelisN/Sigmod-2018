#ifndef BEST_TREE_H
#define BEST_TREE_H

#include "structs.h"

/** Initializes the hash table BestTree that is used in JoinEnum. */
void InitBestTree(best_tree_node*** best_tree, int num_of_relations);

/*
 * Gets a query that contains only joins in the form of a predicate list, and returns 
 * a predicate list with the best estimated order of the joins based on the number of 
 * tuples of the intermediate results.
 */
predicates_listnode* JoinEnum(batch_listnode* curr_query, column_stats*** query_stats,relation_map* rel_map);

/*
 * It scans BestTree[S] and the predicate list of the query for a predicate that has relation R
 * and a relation contained in BestTree[S] 
 */
predicates_listnode* Connected(best_tree_node **best_tree, int rel_num, int S, int R, predicates_listnode *list);

/** Copies the best tree and the stats from BestTree[S] */
int CreateJoinTree(best_tree_node **dest, best_tree_node* source ,batch_listnode* curr_query,relation_map* rel_map,int s_new);

/*
 * Upends a predicate to the predicate list of a BestTree node and initializes the stats
 * of its columns to the tree_cost of the BestTree node
 */ 
int InserPredAtEnd(best_tree_node* tree, predicates_listnode* pred,column_stats ***query_stats,relation_map* rel_map,batch_listnode* curr_query);

/*
 * The function used to estimate the cost of a tree, adds the number of tuples of the relation used in pred
 * to the cost up until now. (The last join added to BestTree[num_of_relations -1] does not take part in the cost 
 * estimation as we only care to minimize the intermediate results).
 */
void CostTree(best_tree_node *curr_tree, batch_listnode* curr_query, predicates_listnode* pred,relation_map *rel_map);

/** Frees The allocated space of the BestTree hashtable*/
void FreeBestTree(best_tree_node **best_tree,batch_listnode *curr_query,relation_map* rel_map);

#endif
