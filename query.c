#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "query.h"
#include "inter_res.h"
#include "filter.h"


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
  int i;

  //Seperate 1. 2. 3. (delimeter = | )
  rel = strtok_r(buffer, "|", &temp);
  pred = strtok_r(NULL, "|", &temp);
  views_temp = strtok_r(NULL, "|", &temp);
  if (rel == NULL || pred == NULL || views_temp == NULL)
  {
    perror("strtok() failed\n");
    printf("%s\n",rel );
    return -1;
  }

  /*Save all the different relations needed in the query to an array
   *for easier access. Find the number of relations first, before
   * allocating space.*/
  int elements = 1;
  (*curr_query)->relations = NULL;
  for (i = 0; i < strlen(rel); i++)
    if (rel[i] == ' ')elements++;

  (*curr_query)->relations = malloc(elements * sizeof(int));
  i = 0;
  while( (temp = strtok_r(rel, " ", &rel)) != NULL )
  {
    (*curr_query)->relations[i] = atoi(temp);
    i++;
  }

  (*curr_query)->num_of_relations = elements;

  /*Save all the predicates in a ch_listnode for easier access.*/
  query_string_array *temp_array = NULL;
  elements = 1;
  for (i = 0; i < strlen(pred); i++)
    if (pred[i] == '&')elements++;
  InitialiseQueryString(&temp_array, elements, pred, "&");

  for (i = 0; i < temp_array->num_of_elements; ++i)
  { 
      InsertPredicate(&(*curr_query)->predicate_list, temp_array->data[i]);
  }

  FreeQueryString(temp_array);
  temp_array = NULL;

  /*Save all the views in a ch_listnode. */
  elements = 1;
  for (i = 0; i < strlen(views_temp); i++)
    if (views_temp[i] == ' ')elements++;
  InitialiseQueryString(&temp_array, elements, views_temp, " ");

  (*curr_query)->views = temp_array;

  (*curr_query)->next = NULL;
  return 0;
}

int InsertPredicate(predicates_listnode **head,char* predicate)
{
  char *c = predicate;
  char tempc;
  join_pred *join_p;
  filter_pred *filter_p;
  while( *c != '=' && *c != '<' && *c != '>')
    c++;
  tempc = (*c);

  if (tempc == '=')
    TokenizeJoinPredicate(predicate,&join_p);
  else
    TokenizeFilterPredicate(predicate,&filter_p,tempc);

  if((*head) == NULL )
  {
    (*head) = malloc(sizeof(predicates_listnode));
    if (tempc == '=')
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
    // if filter -> insert at beginning
    if (tempc != '=')
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

  //printf("%d %d %d %d \n",(*join_p)->relation1, (*join_p)->relation2, (*join_p)->column1, (*join_p)->column2 );
}

void TokenizeFilterPredicate(char* predicate, filter_pred **filter_p,char c)
{
  (*filter_p) = malloc(sizeof(filter_pred));
  char *buffer,*temp;

  buffer = strtok_r(predicate, ".", &temp);
  (*filter_p)->relation = atoi(buffer);

  buffer = strtok_r(NULL, "<>",&temp);
  (*filter_p)->column = atoi(buffer);

  (*filter_p)->comperator = c;

  buffer = strtok_r(NULL, "", &temp);
  (*filter_p)->value = atoi(buffer);

  //printf(%d %d %c %d\n",(*filter_p)->relation, (*filter_p)->column, (*filter_p)->comperator, (*filter_p)->value );
}

int InsertToQueryBatch(batch_listnode** batch, char* query_str)
{
	if( (*batch) == NULL )
    ReadQuery(&(*batch),query_str);
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
	batch_listnode* temp;
	while(batch != NULL)
	{
		temp = batch;
		batch = batch->next;
		FreePredicateList(temp->predicate_list);
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

predicates_listnode* FreePredListNode(predicates_listnode *current, predicates_listnode* prev)
{
  // This node is the head of the list 
  if (prev == current)
  {
    current=current->next;
    if (prev->filter_p != NULL)
      free(prev->filter_p);
    else 
      free(prev->join_p);
    free(prev);
    return current;
  }
  else
  {
    prev->next = current->next;
    if (current->filter_p != NULL)
      free(current->filter_p);
    else 
      free(current->join_p);
    free(current);
    return NULL;
  }
}

void ExecuteQuery(batch_listnode* curr_query,relation_map* rel_map)
{

  // Initialize an intermediate result
  inter_res* intermediate_result = NULL;
  InitInterResults(&intermediate_result,curr_query->num_of_relations);

  // Execute the predicates
  while(curr_query->predicate_list != NULL)
  {
    printf("peos\n");
    // First execute all filters 
    // All filters are int the beginning of the list

    predicates_listnode* current = curr_query->predicate_list;
    predicates_listnode* prev = curr_query->predicate_list;
    // Found a filter
    if(current->filter_p != NULL)
    {
      relation* rel = NULL;
      rel = GetRelation(current->filter_p->relation,current->filter_p->column ,intermediate_result,rel_map);
      Filter(&intermediate_result, curr_query->num_of_relations,rel,current->filter_p->comperator,current->filter_p->value);

      // Filters are always the head of the list
      curr_query->predicate_list = FreePredListNode(current,prev);
    }
    else
    {
      //Execute Join
    }
  }
}
