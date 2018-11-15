#ifndef INTER_RES_H
#define INTER_RES_H

struct inter_data
{
	int num_tuples;
	int **table;
};


/* active_relations[i] = 1 if the i-th relations is active
 * or -1 if it is inactive */
typedef struct intermediate_result
{
	struct inter_data *data;
	int *active_relations;
	int num_of_relations;
}inter_res;

int InitInterResults(inter_res** head, int num_of_relations);

void FreeInterResults(inter_res* var);

#endif
