#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "query.h"

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

int ReadQuery(query **my_query, char* buffer)
{
  if ((*my_query) == NULL)(*my_query) = malloc(sizeof(query));
  char *rel, *pred, *views_temp, *temp;
  int i;
  //Seperate 1. 2. 3. (delimeter = | )
  rel = strtok_r(buffer, "|", &temp);
  pred = strtok_r(NULL, "|", &temp);
  views_temp = strtok_r(NULL, "|", &temp);
  if (rel == NULL || pred == NULL || views_temp == NULL)
  {
    perror("strtok() failed\n");
    return -1;
  }
  /*Save all the different relations needed in the query to an array
   *for easier access. Find the number of relations first, before
   * allocating space.*/
  int elements = 1;
  (*my_query)->relations = NULL;
  for (i = 0; i < strlen(rel); i++)
    if (rel[i] == ' ')elements++;
  (*my_query)->relations = malloc(elements * sizeof(int));
  i = 0;
  while( (temp = strtok_r(rel, " ", &rel)) != NULL )
  {
    (*my_query)->relations[i] = atoi(temp);
    i++;
  }

  /*Save all the predicates in a ch_listnode for easier access.*/
  (*my_query)->predicates = NULL;
  elements = 1;
  for (i = 0; i < strlen(pred); i++)
    if (pred[i] == '&')elements++;
  InitialiseQueryString(&(*my_query)->predicates, elements, pred, "&");
  /*Save all the views in a ch_listnode. */
  (*my_query)->views = NULL;
  elements = 1;
  for (i = 0; i < strlen(views_temp); i++)
    if (views_temp[i] == ' ')elements++;
  InitialiseQueryString(&(*my_query)->views, elements, views_temp, " ");
}

void FreeQuery(query *my_query)
{
  free(my_query->relations);
  FreeQueryString(my_query->predicates);
  FreeQueryString(my_query->views);
  free(my_query);
}
