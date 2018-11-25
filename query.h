#ifndef QUERY_H
#define QUERY_H

#include "structs.h"
#include "inter_res.h"


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
	int num_of_relations;
	int *relations;
	predicates_listnode * predicate_list;
	query_string_array *views;
	struct query_batch_listnode *next;
}batch_listnode;

/* Function that takes a string and constructs the query struct needed.*/
int ReadQuery(batch_listnode **, char* );

int InitialiseQueryString(query_string_array** my_var, int elements, char* str, char* delimeter);

void FreeQueryString(query_string_array* my_var);

int InsertPredicate(predicates_listnode**,char*);

void TokenizeFilterPredicate(char* predicate, filter_pred **filter_p);

void TokenizeJoinPredicate(char* predicate, join_pred **join_p);

int InsertToQueryBatch(batch_listnode** batch, char* query);

void FreeBatch(batch_listnode* batch);

void PrintBatch(batch_listnode* batch);

predicates_listnode* FreePredListNode(predicates_listnode *current);

predicates_listnode* ReturnExecPred(batch_listnode* curr_query,inter_res* intermediate_result);

void ExecuteQuery(batch_listnode* curr_query,relation_map* rel_map);



#endif
