#ifndef INTER_RES_H
#define INTER_RES_H

/* Initialises an inter_data variable.*/
int InitInterData(inter_data** head, int num_of_relations, int num_tuples);

/* Free the allocated memory used by inter_data variable. */
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
 * when the inter_res is empty. This function doesn't check if the result is
 * empty, this check is performed in the ExecuteQuery function.
 */
int InsertJoinToInterResults(inter_res* head, int ex_rel_num, int new_rel_num, result* res);

/*
 * Calls ScanInterResult to check if a relation is in the intermediate result
 * If it is then ScanInterResult will create the struct relation else
 * It creates and returns a struct relation from the relation map
 */
relation* GetRelation(int given_rel, int column, inter_res* inter, relation_map* map,int* query_relations);

/*
 * Given the number of a relations place in the map, it checks
 * if this relation is in the intermediate results and returns
 * a struct relation* that contains only the rowids of the given column
 * that take part in the intermediate result
 */
relation* ScanInterResults(int given_rel,int column, inter_res* inter, relation_map* map,int* query_relations);

/* Handles the case of joining 2 different rows of the same relation. */
result* SelfJoin(int given_rel, int column1, int column2, inter_res** inter, relation_map* map, int* query_relations);

/* Checks whether a relation is active on multiple nodes of the inter_res.
 * If it is , then call merge between the nodes that the relation is active.*/
void MergeInterNodes(inter_res **inter);

/* Merges two inter_res nodes to one. Needs a common active relation. */
void Merge(inter_res **head, inter_res **node, int rel_num);

/* Takes the views defined by the query, the intermediate results data structure
 * and the relation_map and prints the query results. */
void CalculateQueryResults(inter_res *inter, relation_map *map, batch_listnode *query);

/* Takes the views defined by the query and prints NULL for every sum needed.*/
void PrintNullResults(batch_listnode *query);

/* Checks if rel1, rel2 are active in the same node of inter_res */
int AreActiveInInter(inter_res *inter, int rel1, int rel2);

/* Performs the join between two relations that are active in the same inter_res node. */
int JoinInterNode(inter_res **inter, relation_map* rel_map, int relation1, int column1, int relation2, int column2, int* relations);

/* If at the end of the query, the inter_res has multiple nodes, then call the
 * CartesianInterResults to merge them to one node.*/
void CartesianInterResults(inter_res **inter);

#endif
