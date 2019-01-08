typedef struct best_tree_node
{	
	predicates_listnode* best_tree;
	int single_relation;
	int active_bits;
	column_stats *** tree_stats;
}best_tree_node;