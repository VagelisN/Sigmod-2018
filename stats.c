#include "stats.h"
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

void InitQueryStats(column_stats ***query_stats,batch_listnode *curr_query, relation_map* rel_map)
{
	for (int i = 0; i < curr_query->num_of_relations; ++i)
	{
		query_stats[i] = calloc(rel_map[curr_query->relations[i]].num_columns , sizeof(column_stats*));
	}

	predicates_listnode* temp = curr_query->predicate_list;

	//copy only the stats of the columns that take part in the query to save space
	while(temp!=NULL)
	{
		if(temp->filter_p!=NULL)
		{
			filter_pred* fil= temp->filter_p;
			if(query_stats[fil->relation][fil->column]== NULL)
			{
				query_stats[fil->relation][fil->column] = malloc(sizeof(column_stats));
				query_stats[fil->relation][fil->column]->l = rel_map[curr_query->relations[fil->relation]].col_stats[fil->column].l;
				query_stats[fil->relation][fil->column]->u = rel_map[curr_query->relations[fil->relation]].col_stats[fil->column].u;
				query_stats[fil->relation][fil->column]->f = rel_map[curr_query->relations[fil->relation]].col_stats[fil->column].f;
				query_stats[fil->relation][fil->column]->d = rel_map[curr_query->relations[fil->relation]].col_stats[fil->column].d;
			}
		}
		else
		{
			join_pred *join = temp->join_p;
			if(query_stats[join->relation1][join->column1]== NULL)
			{
				query_stats[join->relation1][join->column1] = malloc(sizeof(column_stats));
				query_stats[join->relation1][join->column1]->l = rel_map[curr_query->relations[join->relation1]].col_stats[join->column1].l;
				query_stats[join->relation1][join->column1]->u = rel_map[curr_query->relations[join->relation1]].col_stats[join->column1].u;
				query_stats[join->relation1][join->column1]->f = rel_map[curr_query->relations[join->relation1]].col_stats[join->column1].f;
				query_stats[join->relation1][join->column1]->d = rel_map[curr_query->relations[join->relation1]].col_stats[join->column1].d;
			}
			if(query_stats[join->relation2][join->column2]== NULL)
			{
				query_stats[join->relation2][join->column2] = malloc(sizeof(column_stats));
				query_stats[join->relation2][join->column2]->l = rel_map[curr_query->relations[join->relation2]].col_stats[join->column2].l;
				query_stats[join->relation2][join->column2]->u = rel_map[curr_query->relations[join->relation2]].col_stats[join->column2].u;
				query_stats[join->relation2][join->column2]->f = rel_map[curr_query->relations[join->relation2]].col_stats[join->column2].f;
				query_stats[join->relation2][join->column2]->d = rel_map[curr_query->relations[join->relation2]].col_stats[join->column2].d;
			}
		}
		temp=temp->next;
	}
}

void PrintQueryStats(column_stats ***query_stats,batch_listnode *curr_query,relation_map*rel_map)
{
	for (int i = 0; i <curr_query->num_of_relations ; ++i)
	{
		if(query_stats[i] !=NULL)
		{
			for (int j = 0; j < rel_map[curr_query->relations[i]].num_columns; ++j)
			{
					if(query_stats[i][j] != NULL)
						fprintf(stderr, "i %d j %d f %lf\n",i,j,query_stats[i][j]->f );
			}
		}	
	}
}

void FreeQueryStats(column_stats ***query_stats,batch_listnode *curr_query,relation_map* rel_map)
{
	for (int i = 0; i < curr_query->num_of_relations; ++i)
	{
		if(query_stats[i] !=NULL)
		{
			for (int j = 0; j < rel_map[curr_query->relations[i]].num_columns; ++j)
			{
					if(query_stats[i][j] != NULL)
					{
						free(query_stats[i][j]);
					}
			}
		}	
		free(query_stats[i]);	
	}
	free(query_stats);
}

void ValuePredicate(column_stats ***query_stats,batch_listnode *curr_query,predicates_listnode* pred,relation_map* rel_map)
{
	uint64_t prev_f, prev_d;

	// Estimate a filter predicate
	if(pred->filter_p != NULL)
	{
		filter_pred* fil= pred->filter_p;
		column_stats *stats = query_stats[fil->relation][fil->column];

		// Estimate an equal filter predicate
		if (fil->comperator == '=')
		{
			prev_d = stats->d;
			prev_f = stats->f;
			if (fil->value >= stats->l && 
				fil->value <= stats->u)
			{
				stats->d = 1;
				if (prev_d!= 0)
					stats->f = (stats->f/prev_d);
				else
					stats->f = 0;
			}
			else 
			{
				stats->d = 0;
				stats->f = 0;
			}
			stats->l = fil->value;
			stats->u = fil->value;
		}

		//Estimate a < > filter predicate
		else
		{
			uint64_t k1, k2;
			prev_f = stats->f;
			if (fil->comperator == '<')
			{
				if(fil->value > stats->u)
					k2 = stats->u;
				else
					k2 = fil->value;

				k1 = stats->l;
			}
			else
			{
				if(fil->value < stats->l)
					k1 = stats->l;
				else 
					k1 = fil->value;
				k2 = stats->u;
			}
			if(stats->u - stats->l == 0)
			{
				stats->d = 0;
				stats->l = 0;
			}
			else
			{
				stats->d = (((double)(k2-k1)/(stats->u-stats->l)) * (stats->d) );
				stats->f = (((double)(k2-k1)/(stats->u-stats->l)) * (stats->f) );
			}
			stats->l = k1;
			stats->u = k2;
		}

		// Set the stats for the rest of the collumns of the relation that took part
		column_stats *rest_stats = NULL;
		for (int i = 0; i < rel_map[curr_query->relations[fil->relation]].num_columns; ++i)
		{
			if (i != fil->column && query_stats[fil->relation][i]!= NULL)
			{
				rest_stats = query_stats[fil->relation][i];
				if(rest_stats->d !=0)
					rest_stats->d = (rest_stats->d * (1-powl((1-(stats->f/prev_f)),(rest_stats->f/rest_stats->d))));
				rest_stats->f = stats->f;
			}
		}
	}

	// Estimate Self Join predicate
	else if( pred->join_p != NULL && pred->join_p->relation1 == pred->join_p->relation2)
	{
		join_pred* join= pred->join_p;
		column_stats *stats1 = query_stats[join->relation1][join->column1];
		column_stats *stats2 = query_stats[join->relation2][join->column2];

		prev_f =stats1->f;

		stats1->f = stats2->f = (stats1->f)/(stats1->u-stats1->l+1);
		stats1->d = stats2->d = stats1->d * (1-powl( (1-(stats1->f/prev_f)),(prev_f/stats1->d)));

		if (stats1->l >= stats2->l)
			stats2->l = stats1->l;
		else 
			stats1->l = stats2->l;

		// Set the stats for the rest of the collumns of the relations that took part
		column_stats *rest_stats = NULL;
		for (int i = 0; i < rel_map[curr_query->relations[join->relation1]].num_columns; ++i)
		{
			if (i != join->column1 && query_stats[join->relation1][i]!= NULL)
			{
				rest_stats = query_stats[join->relation1][i];
				if(rest_stats->d !=0)
					rest_stats->d = (rest_stats->d * (1-powl((1-(stats1->f/prev_f)),(rest_stats->f/rest_stats->d))));
				rest_stats->f = stats1->f;
			}
		}
		for (int i = 0; i < rel_map[curr_query->relations[join->relation2]].num_columns; ++i)
		{
			if (i != join->column2 && query_stats[join->relation2][i]!= NULL)
			{
				rest_stats = query_stats[join->relation2][i];
				if(rest_stats->d !=0)
					rest_stats->d = (rest_stats->d * (1-powl((1-(stats1->f/prev_f)),(rest_stats->f/rest_stats->d))));
				rest_stats->f = stats1->f;
			}
		}
	}

	//Estimate a Join predicate
	else
	{
				join_pred* join= pred->join_p;


		column_stats *stats1 = query_stats[join->relation1][join->column1];
		column_stats *stats2 = query_stats[join->relation2][join->column2];

		double prev_d1 = stats1->d;
		double prev_d2 = stats2->d;
		if (stats1->l > stats2-> l)
			stats2->l = stats1->l;
		else 
			stats1->l = stats2->l;

		if (stats1->u < stats2->u)
			stats2->u = stats1->u;
		else 
			stats1->u = stats2->u;

		stats1->f = stats2->f = ((stats1->f * stats2->f)/(double)((stats1->u - stats1->l) +1));
		stats1->d = stats2->d = (stats1->d * stats2->d)/(double)((stats1->u - stats1->l) +1);

		// Set the stats for the rest of the collumns of the relations that took part
		column_stats *rest_stats = NULL;
		for (int i = 0; i < rel_map[curr_query->relations[join->relation1]].num_columns; ++i)
		{
			if (i != join->column1 && query_stats[join->relation1][i]!= NULL)
			{
				rest_stats = query_stats[join->relation1][i];
				if(rest_stats->d !=0)
					rest_stats->d = (rest_stats->d * (1-powl((1-(stats1->d/prev_d1)),(rest_stats->f/rest_stats->d))));
				rest_stats->f = stats1->f;
			}
		}
		for (int i = 0; i < rel_map[curr_query->relations[join->relation2]].num_columns; ++i)
		{
			if (i != join->column2 && query_stats[join->relation2][i]!= NULL)
			{
				rest_stats = query_stats[join->relation2][i];
				if(rest_stats->d !=0)
					rest_stats->d = (rest_stats->d * (1-powl((1-(stats2->d/prev_d2)),(rest_stats->f/rest_stats->d))));
				rest_stats->f = stats1->f;
			}
		}
	}
}