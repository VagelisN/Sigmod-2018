#ifndef QUERY_H
#define QUERY_H

#include "structs.h"
#include "inter_res.h"

/* 
 * This is the main function called when getting a query apart from the string "F".
 * Calls ReadQuery to break the string into predicates and view that will be 
 * inserted in the current batch node.
 */ 
int InsertToQueryBatch(batch_listnode** batch, char* query);

/* 
 * Takes a query that was given from the input and an allocated batch_listnode, 
 * breaks the query into relations, predicates and views, initializes a predicate_list
 * that holds the filter and join predicates and sets them to the current batch_listnode
 */
int ReadQuery(batch_listnode **, char* );

int InitialiseQueryString(query_string_array** my_var, int elements, char* str, char* delimeter);

void FreeQueryString(query_string_array* my_var);

int InsertPredicate(predicates_listnode**,char*);

void TokenizeFilterPredicate(char* predicate, filter_pred **filter_p);

void TokenizeJoinPredicate(char* predicate, join_pred **join_p);


void FreeBatch(batch_listnode* batch);

void PrintBatch(batch_listnode* batch);

predicates_listnode* FreePredListNode(predicates_listnode *current);

predicates_listnode* ReturnExecPred(batch_listnode* curr_query,inter_res* intermediate_result);

void ExecuteQuery(batch_listnode* curr_query,relation_map* rel_map);

#endif
