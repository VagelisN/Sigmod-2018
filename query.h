#ifndef QUERY_H
#define QUERY_H

typedef struct query_string_array
{
  char **data;
  int num_of_elements;
}query_string_array;


/*
 * We save the relations as an int array without a variable to define
 * its size, because we take for granted that the queries that we read
 * will be correct.
 */
typedef struct query
{
  int *relations;
  query_string_array *predicates, *views;
}query;

typedef struct filter_pred
{
	int relation;
	int column;
	int value;
	char comperator;
}filter_pred;

typedef struct join_pred
{
	int relation1;
	int relation2;
	int column1;
	int column2;
}join_pred;

typedef struct query_listnode
{
	filter_pred *filter_p;
	join_pred *join_p;
	struct query_listnode *next;
}query_listnode;

/* Function that takes a string and constructs the query struct needed.
 */
int ReadQuery(query **my_query, char* buffer);

void FreeQuery(query *my_query);

int InitialiseQueryString(query_string_array** my_var, int elements, char* str, char* delimeter);

void FreeQueryString(query_string_array* my_var);

int InsertPredicate(query_listnode**,char*);

void TokenizeFilterPredicate(char* predicate, filter_pred **filter_p,char c);
void TokenizeJoinPredicate(char* predicate, join_pred **join_p);

#endif
