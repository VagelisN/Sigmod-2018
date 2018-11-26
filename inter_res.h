#ifndef INTER_RES_H
#define INTER_RES_H

typedef struct inter_data
{
	int num_tuples;
	uint64_t **table;
}inter_data;


/* active_relations[i] = 1 if the i-th relations is active
 * or -1 if it is inactive */
typedef struct intermediate_result
{
	struct inter_data *data;
	int num_of_relations;
	struct intermediate_result *next;
}inter_res;

int InitInterData(inter_data** head, int num_of_relations, int num_tuples);

void FreeInterData(inter_data *head, int num_of_relations);

/* Initialises an inter_res variable */
int InitInterResults(inter_res** head, int num_of_relations);

/* Prints intermediate results data structure */
void PrintInterResults(inter_res *head);

/* Deallocate all the memory used by inter_res */
void FreeInterResults(inter_res* var);

/*
 * Takes the results of a join and the active relation of the two and
 * updates the inter_res. The ex_rel_num is always tuple_R in the result.
 * Can be used when 1 of the 2 relations is in the inter_res already or
 * when the inter_res is empty.
 */
int InsertJoinToInterResults(inter_res** head, int ex_rel_num, int new_rel_num, result* res);

/*
 * Calls ScanInterResult to check if a relation is in the intermediate result
 * If it is then ScanInterResult will create the struct relation else
 * It creates and returns a struct relation from the relation map
 */
relation* GetRelation(int, int , inter_res*, relation_map*);

/*
 * Given the number of a relations place in the map, it checks
 * if this relation is in the intermediate results and returns
 * a struct relation* that contains only the rowids of the given column
 * that take part in the intermediate result
 */
relation* ScanInterResults(int,int, inter_res*,relation_map* );

/* Handles the case of joining 2 different rows of the same relation. */
int SelfJoin(int , int , int ,inter_res** , relation_map* );

/* Checks whether a relation is active on multiple nodes of the inter_res.
 * If it is , then call merge between the nodes that the relation is active.*/
void MergeInterNodes(inter_res **inter);

/* Merges two inter_res nodes to one. Needs a common active relation. */
void Merge(inter_res **head, inter_res **node, int rel_num);

/* Takes the views defined by the query, the intermediate results data structure
 * and the relation_map and prints the query results. */
void CalculateQueryResults(inter_res *inter, relation_map *map, query_string_array *views);


#endif
