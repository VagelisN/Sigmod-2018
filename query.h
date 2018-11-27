#ifndef QUERY_H
#define QUERY_H

#include "structs.h"
#include "inter_res.h"

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
