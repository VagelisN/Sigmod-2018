#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "results.h"
#include "inter_res.h"

int InitInterData(inter_data** head, int num_of_relations, int num_tuples)
{
	(*head) = malloc(sizeof(struct inter_data));
	(*head)->num_tuples = 0;
	(*head)->table = malloc(num_of_relations * sizeof(int *));
	for (int i = 0; i < num_of_relations; ++i)
		(*head)->table[i] = NULL;
}

void FreeInterData(inter_data *head, int num_of_relations)
{
	for (size_t i = 0; i < num_of_relations; i++) {
		if (head->table[i] != NULL)
			free(head->table[i]);
	}
	free(head->table);
	free(head);
}

int InitInterResults(inter_res** head, int num_of_rel)
{
	(*head) = malloc(sizeof(inter_res));
	(*head)->num_of_relations = num_of_rel;
	(*head)->active_relations = malloc(num_of_rel * sizeof(int));
	for (size_t i = 0; i < num_of_rel; i++)
		(*head)->active_relations[i] = -1;
	InitInterData(&(*head)->data, num_of_rel, 0);
}

int InsertJoinToInterResults(inter_res** head, int ex_rel_num, int new_rel_num, result* res)
{
	//If this is the first instance of the inter_res
	if ((*head)->data->num_tuples == 0)
	{
		//Insert everything from the result to the inter_res
		(*head)->data->num_tuples = GetResultNum(res);
		(*head)->active_relations[ex_rel_num] = 1;
		(*head)->active_relations[new_rel_num] = 1;
		(*head)->data->table[ex_rel_num] = malloc((*head)->data->num_tuples * sizeof(int64_t));
		(*head)->data->table[new_rel_num] = malloc((*head)->data->num_tuples * sizeof(int64_t));
		for (size_t i = 0; i < (*head)->data->num_tuples; i++)
		{
			result_tuples *temp = FindResultTuples(res, i);
			(*head)->data->table[ex_rel_num][i] = temp->tuple_R.row_id;
			(*head)->data->table[ex_rel_num][i] = temp->tuple_S.row_id;
		}
	}
	else
	{
		//Allocate and initialise the new inter_data variable.
		inter_data *temp_array = malloc(sizeof(inter_data));
		temp_array->num_tuples = GetResultNum(res);
		temp_array->table = malloc((*head)->num_of_relations * sizeof(int64_t*));
		for (size_t i = 0; i < (*head)->num_of_relations; i++)
		{
			if((*head)->active_relations[i] == 1)
				temp_array->table[i] = malloc((*head)->data->num_tuples * sizeof(int64_t));
			else temp_array->table[i] = NULL;
		}

		//Insert the results.
		result_tuples *temp;
		int old_pos;
		for (size_t i = 0; i < (*head)->data->num_tuples; i++)
		{
			//Insert the results that are stored in the res variable.
			temp = FindResultTuples(res, i);
			/* Old_pos refers to the current result's row_id in the inter_res data table.*/
			old_pos = temp->tuple_R.row_id;
			temp_array->table[ex_rel_num][i] = (*head)->data->table[ex_rel_num][old_pos];
			temp_array->table[new_rel_num][i] = temp->tuple_S.row_id;
			for (size_t j = 0; j < (*head)->num_of_relations; j++)
			{
				if ((ex_rel_num != j) && (*head)->active_relations[j] == 1)
				{
					temp_array->table[j][i] = (*head)->data->table[j][old_pos];
				}
			}
		}
		/* Set the newly added relation as active. And set the new instance of
		 * the inter_res variable, after deallocating the memory of the
		 * previous instance. */
		(*head)->active_relations[new_rel_num] = 1;
		FreeInterData((*head)->data, (*head)->num_of_relations);
		(*head)->data = temp_array;
	}
	return 0;
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

relation* ScanInterResults(int given_rel,int column, inter_res* inter, relation_map* map)
{
	if (inter->num_of_relations >= given_rel || given_rel < 0 )
	{
		fprintf(stderr, "given_rel out of bounds\n");
		exit(2);
	}
	if (inter->active_relations[given_rel] == -1) return NULL;

	// Allocate a new struct relation
	relation* new_rel = malloc(sizeof(relation));
	new_rel->num_tuples = inter->data->num_tuples;
	new_rel->tuples = malloc(new_rel->num_tuples * sizeof(tuple));

	//Get a pointer to the correct column of the mapped relation
	int64_t* col = map->columns[column];

	int i;
	for (i = 0; i < inter->data->num_tuples; ++i)
	{
		new_rel->tuples[i].row_id = i;
		new_rel->tuples[i].value = *(col + inter->data->table[given_rel][i] * sizeof(int64_t));
	}
	return new_rel;
} 