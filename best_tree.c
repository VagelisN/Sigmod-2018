#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "structs.h"
#include "best_tree.h"
#include "query.h"
#include "stats.h"


void InitBestTree(best_tree_node*** best_tree, int num_of_relations)
{
  int best_tree_size = (int)pow(2, num_of_relations);
  (*best_tree) = malloc( best_tree_size * sizeof(best_tree_node*));
  for (size_t i = 1; i < best_tree_size; i++) 
  {
    (*best_tree)[i] = malloc(sizeof(best_tree_node));
    (*best_tree)[i]->best_tree = NULL;
    (*best_tree)[i]->tree_stats = NULL;
    (*best_tree)[i]->num_predicates=0;
    (*best_tree)[i]->cost = 0;
    //Find the number of bits in the i
    int num = i, bit_num = 0;
    while(num != 0)
    {
      if (num % 2 == 1)
        bit_num++;
      num = num >> 1;
    }
    //If its a single relation set the variable accordingly
    if( bit_num == 1)(*best_tree)[i]->single_relation = i;
    else (*best_tree)[i]->single_relation = -1;
    (*best_tree)[i]->active_bits = bit_num;

    //Set the column stats here
    if(bit_num != 1)
    	(*best_tree)[i]->tree_stats = calloc(num_of_relations,sizeof(column_stats**));
  }
  return;
}

void FreeBestTree(best_tree_node **best_tree,batch_listnode *curr_query,relation_map* rel_map)
{
  int size = pow(2, curr_query->num_of_relations);
  for (size_t i = 1; i < size; i++) 
  {
    if (best_tree[i]->best_tree != NULL) 
    	FreePredicateList(best_tree[i]->best_tree);
    if(best_tree[i]->tree_stats != NULL)
      FreeQueryStats(best_tree[i]->tree_stats,curr_query,rel_map);
    free(best_tree[i]);
  }
  free(best_tree);
}


predicates_listnode* Connected(best_tree_node **best_tree, int num_of_relations, int S, int R, predicates_listnode *list )
{
  //All relations that are active in S will be stored in active_rel array
	//fprintf(stderr, "connected %d %d \n",S,R );
  //Active rel can be malloced at the JoinEnum function so we dont have to malloc it every time
  short int *active_rel = calloc(num_of_relations, sizeof(short int));
  predicates_listnode *head = best_tree[S]->best_tree;

    int num = S , bit_num = 0, i =0;
    while(num != 0)
    {
      if (num % 2 == 1)
        active_rel[i]=1;
      num = num >> 1;
      i++;
    }
    /*for (int i = 0; i < num_of_relations; ++i)
    {
    	fprintf(stderr, "active_rel[%d] %d\n",i,active_rel[i] );
    }*/
  /*while(head != NULL)
  {
    active_rel[ head->join_p->relation1 ] = 1;
    active_rel[ head->join_p->relation2 ] = 1;
    head = head->next;
  }*/

  //Traverse through the query predicates
  while(list != NULL)
  {
    if(list->join_p->relation1 == R ) {
      if(active_rel[list->join_p->relation2] == 1){
        free(active_rel);
        return list;
      }
    }
    else if (list->join_p->relation2 == R)
    {
      if(active_rel[list->join_p->relation1] == 1){
        free(active_rel);
        //fprintf(stderr, "join %d %d\n",list->join_p->relation1,list->join_p->relation2 );
        return list;
      }
    }
    list = list->next;
  }
  free(active_rel);
  return NULL;
}



predicates_listnode* JoinEnum(batch_listnode* curr_query, column_stats*** query_stats,relation_map* rel_map)
{
	predicates_listnode *temp_pred = NULL;
	best_tree_node **best_tree = NULL, *curr_tree = NULL;
	InitBestTree(&best_tree, curr_query->num_of_relations);
	int s_new;

  //the size of the tree is equal to 2^relations
	int best_tree_size = (int)pow(2, curr_query->num_of_relations);

  //
	for (int i = 1; i < curr_query->num_of_relations; ++i)
	{
		//fprintf(stderr, "i %d\n",i );
    // For all subsets S of the relations of size i
		for (int s = 1; s < best_tree_size; ++s)
		{
			//fprintf(stderr, "\ts %d\n",s );
			if(best_tree[s]->active_bits == i)
			{
        // For all the relations that are not in S
				for (int j = 1; j <= curr_query->num_of_relations; ++j)
				{
					if ( (((int)pow(2,j-1)) & s) != 0)continue;
					//fprintf(stderr, "\t \tj %d\n",j );
          // If relation Rj is connected to S
					temp_pred = Connected(best_tree,curr_query->num_of_relations, s, j-1,curr_query->predicate_list);
					if (temp_pred == NULL)continue;

					s_new = ( (s | (int)pow(2,j-1)));

          // Create left deep tree that contains this relation
					CreateJoinTree(&curr_tree,best_tree[s],curr_query,rel_map,s_new);
					InserPredAtEnd(curr_tree,temp_pred,query_stats,rel_map,curr_query);

          // Find the cost od the current tree
          if(s_new != best_tree_size-1) 
					 CostTree(curr_tree,curr_query,temp_pred,rel_map);

				  //fprintf(stderr, "join _pred %d %d \n",temp_pred->join_p->relation1,temp_pred->join_p->relation2 );
					//fprintf(stderr, "s_new %d\n",s_new );

          //Update Best Tree
					if(best_tree[s_new]->best_tree == NULL || best_tree[s_new]->cost > curr_tree->cost)
					{
						if(best_tree[s_new]->best_tree == NULL || best_tree[s_new]->num_predicates == curr_tree->num_predicates)
						{
                FreeQueryStats(best_tree[s_new]->tree_stats,curr_query,rel_map);
  							FreePredicateList(best_tree[s_new]->best_tree);
  							free(best_tree[s_new]);
							  best_tree[s_new] = curr_tree;
						}
            else
            {
              FreeQueryStats(curr_tree->tree_stats,curr_query,rel_map);
              FreePredicateList(curr_tree->best_tree);
              free(curr_tree);
              curr_tree = NULL;
            }

					}
					else 
					{
						FreeQueryStats(curr_tree->tree_stats,curr_query,rel_map);
						FreePredicateList(curr_tree->best_tree);
						free(curr_tree);
						curr_tree = NULL;
					}
				}
			}
		}
		/*for (int m = 1; m < best_tree_size; ++m)
		{
				fprintf(stderr, "Printing predicate list of best_tree[%d] cost %lf \n",m,best_tree[m]->cost );
				PrintPredList(best_tree[m]->best_tree);
		}*/
	}

	//fprintf(stderr, "tree size%d\n",best_tree_size-1 );
	//PrintPredList(best_tree[best_tree_size-1]->best_tree);


	predicates_listnode *return_list =best_tree[best_tree_size-1]->best_tree;
	best_tree[best_tree_size-1]->best_tree = NULL;

  /* Some queries have two predicates that contain the same relations
   * JoinEnum returns a list that contain only one of the two and so
   * if the number of predicates of the best tree is less than the 
   * number of predicates of the current query, find these predicates
   * and insert them at end */
  predicates_listnode* temp_old = curr_query->predicate_list;

  int found, temp_num_pred = 0;
  while(temp_old!=NULL)
  {
    temp_num_pred++;
    temp_old= temp_old->next;
  }
  temp_old = curr_query->predicate_list;
  if(temp_num_pred != best_tree[best_tree_size-1]->num_predicates )
  {
    //check if there were any predicates left out from JoinEnum
    predicates_listnode* temp_new;
    while(temp_old!=NULL)
    {
      found = 0;
      temp_new = return_list;
      while(temp_new!=NULL)
      {
        if(temp_old->join_p->relation1 == temp_new->join_p->relation1&&
           temp_old->join_p->relation2 == temp_new->join_p->relation2&&
           temp_old->join_p->column1 == temp_new->join_p->column1&&
           temp_old->join_p->column2 == temp_new->join_p->column2)
        {
          found =1;
          break;
        }
        if(temp_new->next == NULL)break;
        temp_new = temp_new->next;
      }
      if(found == 0)
      {
        temp_new->next = malloc (sizeof(predicates_listnode));
        temp_new->next->next = NULL;
        temp_new->next->filter_p = NULL;
        temp_new ->next->join_p = malloc(sizeof(join_pred));
        temp_new->next->join_p->relation1=temp_old->join_p->relation1;
        temp_new->next->join_p->relation2=temp_old->join_p->relation2;
        temp_new->next->join_p->column1=temp_old->join_p->column1;
        temp_new->next->join_p->column2=temp_old->join_p->column2;
      }
      temp_old = temp_old->next;
    }
    FreePredicateList(curr_query->predicate_list);
  }
	FreeBestTree(best_tree, curr_query,rel_map);
	return return_list;
}

int CreateJoinTree(best_tree_node **dest, best_tree_node* source ,batch_listnode* curr_query,relation_map* rel_map,int s_new)
{
	(*dest) = malloc(sizeof(best_tree_node));
    (*dest)->best_tree = NULL;
    //Find the number of bits in the i
    int num = s_new, bit_num = 0;
    while(num != 0)
    {
      if (num % 2 == 1)
        bit_num++;
      num = num >> 1;
    }
    //If its a single relation set the variable accordingly
    if( bit_num == 1)(*dest)->single_relation = s_new;
    else (*dest)->single_relation = -1;
    (*dest)->active_bits = bit_num;
    (*dest)->num_predicates = 0;
    //Set the column stats here

    (*dest)->tree_stats = calloc(curr_query->num_of_relations,sizeof(column_stats**));
    predicates_listnode* source_tree = source->best_tree;
    while(source_tree!=NULL)
    {
    	InserPredAtEnd((*dest) ,source_tree, source->tree_stats,rel_map,curr_query);
    	source_tree=source_tree->next;
    }
    (*dest)->cost = source->cost;
}

void CostTree(best_tree_node *curr_tree, batch_listnode* curr_query, predicates_listnode* pred,relation_map *rel_map)
{
	ValuePredicate(curr_tree->tree_stats,curr_query,pred,rel_map);
	//fprintf(stderr, "prev cost =%lf + %lf \n",curr_tree->cost ,curr_tree->tree_stats[pred->join_p->relation1][pred->join_p->column1]->f );
	curr_tree->cost = (curr_tree->cost + curr_tree->tree_stats[pred->join_p->relation1][pred->join_p->column1]->f);
}

