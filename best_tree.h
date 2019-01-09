#ifndef BEST_TREE_H
#define BEST_TREE_H

typedef struct best_tree_node
{
	predicates_listnode* best_tree;
	int single_relation;
	int active_bits;
	double cost;
	column_stats ***tree_stats;
}best_tree_node;

void InitBestTree(best_tree_node*** best_tree, int num_of_relations);

void FreeBestTree(best_tree_node **best_tree, int num_of_relations);

predicates_listnode* Connected(best_tree_node **best_tree, int rel_num, int S, int R, predicates_listnode *list);

#endif
