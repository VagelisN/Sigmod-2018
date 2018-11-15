#ifndef QUERY_H
#define QUERY_H

typedef struct query_string_array
{
  char **data;
  int num_of_elements;
}query_string_array;


/*
 * We save the relations as an int array without a variable to define
 * its size, because we take as granted that the queries that we read
 * will be correct.
 */
typedef struct query
{
  int *relations;
  query_string_array *predicates, *views;
}query;

/* Function that takes a string and constructs the query struct needed.
 */
int ReadQuery(query **my_query, char* buffer);

void FreeQuery(query *my_query);

int InitialiseQueryString(query_string_array** my_var, int elements, char* str, char* delimeter);

void FreeQueryString(query_string_array* my_var);

#endif
