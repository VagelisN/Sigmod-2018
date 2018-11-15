#include <stdio.h>
#include <stdlib.h>
#include "inter_res.h"

int InitInterResults(inter_res** head, int num_of_relations)
{
	(*head) == malloc(sizeof(inter_res));
	(*head)->num_of_relations = num_of_relations;
	(*head)->active_relations = malloc(num_of_relations * sizeof(int));
	for (size_t i = 0; i < num_of_relations; i++)
		(*head)->active_relations[i] = -1;
	(*head)->data = malloc(sizeof(struct inter_data));
	(*head)->data->num_tuples = 0;
	(*head)->data->table = malloc((*head)->num_of_relations * sizeof(int *));
	for (int i = 0; i < num_of_relations; ++i)
		(*head)->data->table[i] = NULL;
}

void FreeInterResults(inter_res* var)
{
	if (var->data->num_tuples > 0)
		for (int i = 0; i < var->num_of_relations; ++i)
			if(var->data->table[i] != NULL)
				free(var->data->table[i]);
	free(var->data->table);
	free(var->data);
	free(var->active_relations);
	free(var);
}
