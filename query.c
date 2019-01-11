#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "structs.h"
#include "results.h"
#include "preprocess.h"
#include "query.h"
#include "inter_res.h"
#include "filter.h"
#include "rhjoin.h"
#include "stats.h"
#include "best_tree.h"

int InitialiseQueryString(query_string_array** my_var, int elements, char* str, char* delimeter)
{
  char *temp;

  (*my_var) = malloc(sizeof(query_string_array));
  (*my_var)->num_of_elements = elements;
  if ((*my_var) == NULL) return -1;

  (*my_var)->data = malloc(elements * sizeof(char*));
  if ((*my_var)->data == NULL) return -1;

  int i = 0;
  while( (temp = strtok_r(str, delimeter, &str)) != NULL )
  {
    (*my_var)->data[i] = malloc((strlen(temp) + 1) * sizeof(char));
    if ((*my_var)->data[i] == NULL) return -1;
    strcpy((*my_var)->data[i], temp);
    i++;
  }
  return 0;
}

void FreeQueryString(query_string_array* my_var)
{
  for (size_t i = 0; i < my_var->num_of_elements; i++)free(my_var->data[i]);
  free(my_var->data);
  free(my_var);
}

int ReadQuery(batch_listnode** curr_query, char* buffer)
{
  if ((*curr_query) == NULL) (*curr_query) = malloc(sizeof(batch_listnode));
    (*curr_query)->predicate_list = NULL;

  char *rel, *pred, *views_temp, *temp;
  query_string_array *temp_array = NULL;
  int i;

  //Seperate relations predicates and views (delimeter = | )
  rel = strtok_r(buffer, "|", &temp);
  pred = strtok_r(NULL, "|", &temp);
  views_temp = strtok_r(NULL, "|", &temp);
  if (rel == NULL || pred == NULL || views_temp == NULL)
  {
    perror("strtok() failed\n");
    printf("%s\n",rel );
    return -1;
  }

  // find the number of relations
  int elements = 1;
  (*curr_query)->relations = NULL;
  for (i = 0; i < strlen(rel); i++)
    if (rel[i] == ' ')elements++;

  // allocate the space needed and set the relations given
  (*curr_query)->relations = malloc(elements * sizeof(int));
  i = 0;
  while( (temp = strtok_r(rel, " ", &rel)) != NULL )
  {
    (*curr_query)->relations[i] = atoi(temp);
    i++;
  }

  (*curr_query)->num_of_relations = elements;

  // find the number of predicates
  elements = 1;
  for (i = 0; i < strlen(pred); i++)
    if (pred[i] == '&')elements++;

  // save all the predicates in a query string array for easier access.
  InitialiseQueryString(&temp_array, elements, pred, "&");

  // insert all the predicates found to the predicate list of the current batch_listnode
  for (i = 0; i < temp_array->num_of_elements; ++i)
      InsertPredicate(&(*curr_query)->predicate_list, temp_array->data[i]);

  FreeQueryString(temp_array);
  temp_array = NULL;

  // find the number ofviews
  elements = 1;
  for (i = 0; i < strlen(views_temp); i++)
    if (views_temp[i] == ' ')elements++;

  // save all the views in a query string array for easier access.
  InitialiseQueryString(&temp_array, elements, views_temp, " ");

  // set the current batch_listnode's view to point to the query string array
  (*curr_query)->views = temp_array;
  (*curr_query)->next = NULL;
  return 0;
}

int InserPredAtEnd(best_tree_node* tree, predicates_listnode* pred,column_stats ***query_stats,relation_map* rel_map,batch_listnode* curr_query)
{
  tree->num_predicates++;
  if (tree->tree_stats[pred->join_p->relation1] == NULL)
  {
      tree->tree_stats[pred->join_p->relation1] =
    calloc(rel_map[curr_query->relations[pred->join_p->relation1]].num_columns , sizeof(column_stats*));

    for (int i = 0; i < rel_map[curr_query->relations[pred->join_p->relation1]].num_columns ; ++i)
    {
      if(query_stats[pred->join_p->relation1][i] == NULL)continue;

      tree->tree_stats[pred->join_p->relation1][i] =  malloc(sizeof(column_stats));
      column_stats* stats = tree->tree_stats[pred->join_p->relation1][i];
      stats->l = query_stats[pred->join_p->relation1][i]->l;
      stats->u = query_stats[pred->join_p->relation1][i]->u;
      stats->f = query_stats[pred->join_p->relation1][i]->f;
      stats->d = query_stats[pred->join_p->relation1][i]->d;
    }
  }
  if (tree->tree_stats[pred->join_p->relation2] == NULL)
  {
      tree->tree_stats[pred->join_p->relation2] =
    calloc(rel_map[curr_query->relations[pred->join_p->relation2]].num_columns , sizeof(column_stats*));

    for (int i = 0; i < rel_map[curr_query->relations[pred->join_p->relation2]].num_columns ; ++i)
    {
      if(query_stats[pred->join_p->relation2][i] == NULL)continue;

      tree->tree_stats[pred->join_p->relation2][i] =  malloc(sizeof(column_stats));
      column_stats* stats = tree->tree_stats[pred->join_p->relation2][i];
      stats->l = query_stats[pred->join_p->relation2][i]->l;
      stats->u = query_stats[pred->join_p->relation2][i]->u;
      stats->f = query_stats[pred->join_p->relation2][i]->f;
      stats->d = query_stats[pred->join_p->relation2][i]->d;
    }
  }
  if(tree->best_tree== NULL )
  {
    tree->best_tree = malloc(sizeof(predicates_listnode));
    tree->best_tree->next = NULL;
    tree->best_tree->filter_p = NULL;
    tree->best_tree->join_p = malloc(sizeof(predicates_listnode));
    tree->best_tree->join_p->relation1 = pred->join_p->relation1;
    tree->best_tree->join_p->relation2 = pred->join_p->relation2;
    tree->best_tree->join_p->column1 = pred->join_p->column1;
    tree->best_tree->join_p->column2 = pred->join_p->column2;
  }
  else
  {
    predicates_listnode *temp = tree->best_tree;
    while(temp->next != NULL)
       temp = temp->next;

    temp->next = malloc(sizeof(predicates_listnode));
    temp->next->filter_p = NULL;
    temp->next->next = NULL;
    temp->next->join_p = malloc(sizeof(predicates_listnode));
    temp->next->join_p->relation1 = pred->join_p->relation1;
    temp->next->join_p->relation2 = pred->join_p->relation2;
    temp->next->join_p->column1 = pred->join_p->column1;
    temp->next->join_p->column2 = pred->join_p->column2;
  }
}

int InsertPredicate(predicates_listnode **head,char* predicate)
{

  char *c = predicate;
  int fullstop_count = 0;

  // the predicate given can either be a join or a filter
  join_pred *join_p;
  filter_pred *filter_p;

  // if two fullstops are found the predicate is a join, or else it is a filter
  while( *c != '\0')
  {
    if (*c =='.') fullstop_count++;
    c++;
  }

  if (fullstop_count == 2)
    TokenizeJoinPredicate(predicate,&join_p);
  else
    TokenizeFilterPredicate(predicate,&filter_p);

  if((*head) == NULL )
  {
    (*head) = malloc(sizeof(predicates_listnode));
    if (fullstop_count == 2)
    {
      (*head)->join_p =join_p;
      (*head)->filter_p = NULL;
    }
    else
    {
      (*head)->filter_p = filter_p;
      (*head)->join_p = NULL;
    }
    (*head)->next = NULL;
  }
  else
  {
    // if filter or self join insert at beginning
    if (fullstop_count == 1 || (fullstop_count == 2 && join_p->relation1 == join_p->relation2))
    {
      predicates_listnode *new_head = malloc(sizeof(predicates_listnode));
      new_head->filter_p = filter_p;
      new_head->join_p = NULL;
      new_head->next = (*head);
      (*head) = new_head;
    }
    // if join insert at end
    else
    {
      predicates_listnode *temp = (*head);
      while(temp->next != NULL)
        temp = temp->next;

      temp->next = malloc(sizeof(predicates_listnode));
      temp->next->filter_p = NULL;
      temp->next->join_p = join_p;
      temp->next->next = NULL;
    }
  }
  return 0;
}

void FreePredicateList(predicates_listnode* head)
{
	predicates_listnode *temp = head;
	while(head!=NULL)
	{
		temp = head;
		head = head->next;
    if (temp->filter_p != NULL)
      free(temp->filter_p);
    else
      free(temp->join_p);
		free(temp);
	}
}

void TokenizeJoinPredicate(char* predicate, join_pred **join_p)
{
  (*join_p) = malloc(sizeof(join_pred));
  char *buffer,*temp;

  buffer = strtok_r(predicate, ".", &temp);
  (*join_p)->relation1 = atoi(buffer);

  buffer = strtok_r(NULL, "=",&temp);
  (*join_p)->column1 = atoi(buffer);

  buffer = strtok_r(NULL, ".", &temp);
  (*join_p)->relation2 = atoi(buffer);

  buffer = strtok_r(NULL, "",&temp);
  (*join_p)->column2 = atoi(buffer);
}

void TokenizeFilterPredicate(char* predicate, filter_pred **filter_p)
{
  (*filter_p) = malloc(sizeof(filter_pred));
  char *buffer, *left_operand,*right_operand,*temp;
  char *c = predicate;

  while( (*c != '<' && *c != '>' && *c != '='))
    c++;
  (*filter_p)->comperator = *c;

  left_operand = strtok_r(predicate, "<>=", &temp);
  right_operand = strtok_r(NULL, " ", &temp);

  char comperator;
  c =left_operand;
  int found_fullstop = 0;
  while( *c != '\0')
  {
    if (*c =='.')
    {
      found_fullstop =1;
      break;
    }
    c++;
  }

  if(found_fullstop == 1)
  {
    buffer = strtok_r(left_operand, ".", &temp);
    (*filter_p)->relation = atoi(buffer);
    buffer = strtok_r(NULL, " ", &temp);
    (*filter_p)->column = atoi(buffer);
    (*filter_p)->value = atoi(right_operand);
  }
  else
  {
    buffer = strtok_r(right_operand, ".", &temp);
    (*filter_p)->relation = atoi(buffer);
    buffer = strtok_r(NULL, " ", &temp);
    (*filter_p)->column = atoi(buffer);
    (*filter_p)->value = atoi(left_operand);
  }
}

int InsertToQueryBatch(batch_listnode** batch, char* query_str)
{
	if( (*batch) == NULL )
    ReadQuery(batch,query_str);
	else
	{
		batch_listnode *temp = (*batch);
		while(temp->next != NULL)
      temp = temp->next;

    ReadQuery(&temp->next,query_str);
	}
	return 0;
}

void FreeBatch(batch_listnode* batch)
{
	while(batch != NULL)
	{
		batch_listnode *temp = batch;
		batch = batch->next;
		//FreePredicateList(temp->predicate_list);
    FreeQueryString(temp->views);
    free(temp->relations);
		free(temp);
	}
}

void PrintPredList(predicates_listnode* head)
{
  while(head!= NULL)
  {
    if(head->filter_p != NULL)
    {
      printf("    filter:\n");
      printf("     %d %d %c %d\n",head->filter_p->relation ,head->filter_p->column,head->filter_p->comperator ,head->filter_p->value );
    }
    else
    {
      printf("    join:\n");
      printf("     %d %d %d %d \n",head->join_p->relation1, head->join_p->relation2, head->join_p->column1, head->join_p->column2 );
    }
    head = head->next;
  }
}

void PrintBatch(batch_listnode* batch)
{
  int i = 0;
  while(batch!=NULL)
  {
    printf("\nPrinting query :%d\n",i);
    printf("  num_of_relations: %d\n   ",batch->num_of_relations);
    for (int j = 0; j < batch->num_of_relations; ++j)
      printf("%d ",batch->relations[j]);
    printf("\n\n");
    printf("  predicates:\n");
    PrintPredList(batch->predicate_list);
    i++;
    batch = batch->next;
  }
}

void FreePredListNode(predicates_listnode *current)
{
  // This node is the head of the list
    if (current->filter_p != NULL)
      free(current->filter_p);
    else
      free(current->join_p);
    free(current);
}

void ExecuteQuery(batch_listnode* curr_query, relation_map* rel_map, scheduler* sched)
{
  // Initialize an intermediate result
  inter_res* intermediate_result = NULL;
  column_stats ***query_stats = calloc(curr_query->num_of_relations,sizeof(column_stats**));
  InitQueryStats(query_stats,curr_query,rel_map);
  predicates_listnode* current =NULL;


  InitInterResults(&intermediate_result, curr_query->num_of_relations);

  // Execute the predicates
  while(curr_query->predicate_list != NULL)
  {
    // First execute all filters and self joins
    // All filters are int the beginning of the list

    current = curr_query->predicate_list;
    if(current->filter_p != NULL ||
      (current->join_p != NULL && current->join_p->relation1 == current->join_p->relation2))
    {
      current = curr_query->predicate_list;
      ValuePredicate(query_stats,curr_query,current,rel_map);
      curr_query->predicate_list=current->next;

      result *res =NULL;

      // filter
      if(current->filter_p != NULL)
      {
        relation* rel = NULL;
        res = Filter(intermediate_result, current->filter_p,
                                  rel_map,curr_query->relations);

        /*If a result is NULL then all the query results are NULL */
        if (res == NULL)
        {
          PrintNullResults(curr_query);
          FreeRelation(rel);
          FreeQueryStats(query_stats,curr_query,rel_map);
          FreePredListNode(current);
          FreePredicateList(curr_query->predicate_list);
          FreeInterResults(intermediate_result);
          return;
          //Free all memory used for this query
          //Exit query
        }
        InsertSingleRowIdsToInterResult(&intermediate_result, current->filter_p->relation, res);
        FreeRelation(rel);
      }

      // self join
      else
      {
        int relation1 = current->join_p->relation1;
        int relation2 = current->join_p->relation2;
        res = SelfJoin(relation1, current->join_p->column1, current->join_p->column2,
                            &intermediate_result,rel_map,curr_query->relations);
        if (res == NULL)
        {
          PrintNullResults(curr_query);
          FreeInterResults(intermediate_result);
          FreePredListNode(current);
          FreeQueryStats(query_stats,curr_query,rel_map);
          FreePredicateList(curr_query->predicate_list);
          return;
        }
        InsertSingleRowIdsToInterResult(&intermediate_result, relation1, res);
      }
      FreeResult(res);
      FreePredListNode(current);
    }
    else
      break;
  }

  //all filters have been executed so we need to optimize the order of the joins
  if( curr_query->predicate_list->next!=NULL)
    curr_query->predicate_list = JoinEnum(curr_query,query_stats,rel_map);

  while(curr_query->predicate_list != NULL)
  {
    current = curr_query->predicate_list;
    curr_query->predicate_list=current->next;
    //Execute Join
    //if either of the relations is in the intermediate result or we reached the end
    int relation1 = current->join_p->relation1;
    int relation2 = current->join_p->relation2;
    result* curr_res = NULL;

    //Check whether or not the relations are both active in the same node
    if (AreActiveInInter(intermediate_result, current->join_p->relation1, current->join_p->relation2) == 1)
    {
      if(JoinInterNode(&intermediate_result, rel_map, current->join_p->relation1, current->join_p->column1,
                       current->join_p->relation2, current->join_p->column2,curr_query->relations) == 0)
      {
        fprintf(stderr, "Error in JoinInterNode()\n");
        exit(2);
      }
    }
    else
    {
      relation* relR = GetRelation(current->join_p->relation1,
                                   current->join_p->column1 ,
                                   intermediate_result,rel_map,
                                   curr_query->relations);
      relation* relS = GetRelation(current->join_p->relation2,
                                  current->join_p->column2,
                                  intermediate_result, rel_map,
                                  curr_query->relations);
      result* curr_res = RadixHashJoin(relR, relS, sched);
      if (curr_res == NULL)
      {
          PrintNullResults(curr_query);
          FreeQueryStats(query_stats,curr_query,rel_map);
          FreePredListNode(current);
          FreePredicateList(curr_query->predicate_list);
          FreeRelation(relR);
          FreeRelation(relS);
          FreeInterResults(intermediate_result);
          return;
      }
      InsertJoinToInterResults(intermediate_result,
                               relation1, relation2, curr_res);

      if(intermediate_result->next != NULL) MergeInterNodes(&intermediate_result);
      FreeRelation(relR);
      FreeRelation(relS);
      FreeResult(curr_res);
    }

    FreePredListNode(current);
    FreeResult(curr_res);
  }
  if(intermediate_result->next != NULL)CartesianInterResults(&intermediate_result);
  CalculateQueryResults(intermediate_result, rel_map, curr_query);
  FreeInterResults(intermediate_result);
  FreeQueryStats(query_stats,curr_query,rel_map);
  query_stats = NULL;
}
