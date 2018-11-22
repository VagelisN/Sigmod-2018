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

typedef struct predicates_listnode
{
	filter_pred *filter_p;
	join_pred *join_p;
	struct predicates_listnode *next;
}predicates_listnode;

typedef struct query_batch_listnode
{
	predicates_listnode * predicate_list;
	query_string_array *views;
	struct query_batch_listnode *next;
}batch_listnode;

/* Function that takes a string and constructs the query struct needed.
 */
int ReadQuery(query **my_query, char* buffer);

void FreeQuery(query *my_query);

int InitialiseQueryString(query_string_array** my_var, int elements, char* str, char* delimeter);

void FreeQueryString(query_string_array* my_var);

int InsertPredicate(predicates_listnode**,char*);

void TokenizeFilterPredicate(char* predicate, filter_pred **filter_p,char c);

void TokenizeJoinPredicate(char* predicate, join_pred **join_p);

int InsertToQueryBatch(batch_listnode** batch, char* query);

void FreeBatch(batch_listnode* batch);

void ExecuteQuery(batch_listnode *batch_temp);

#endif
