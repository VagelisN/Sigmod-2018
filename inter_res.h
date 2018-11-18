#ifndef INTER_RES_H
#define INTER_RES_H

typedef struct inter_data
{
	int num_tuples;
	int **table;
}inter_data;


/* active_relations[i] = 1 if the i-th relations is active
 * or -1 if it is inactive */
typedef struct intermediate_result
{
	struct inter_data *data;
	int *active_relations;
	int num_of_relations;
}inter_res;

int InitInterData(inter_data** head, int num_of_relations, int num_tuples);

void FreeInterData(inter_data *head, int num_of_relations);

/* Initialises an inter_res variable */
int InitInterResults(inter_res** head, int num_of_relations);

/* Deallocate all the memory used by inter_res */
void FreeInterResults(inter_res* var);

/* Takes the results of a join and the active relation of the two and
 * updates the inter_res. The ex_rel_num is always tuple_R in the result.
 * Can be used when 1 of the 2 relations is in the inter_res already or
 * when the inter_res is empty.*/
int InsertJoinToInterResults(inter_res** head, int ex_rel_num, int new_rel_num, result* res);

/* Given the number of a relations place in the map, it checks
 * if this relation is in the intermediate results and returns
 * a struct relation* that contains only the rowids of the given column 
 * that take part in the intermediate result 
 */
relation* ScanInterResults(int,int, inter_res*,relation_map* );

#endif
