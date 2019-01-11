#ifndef QUERY_H
#define QUERY_H

#include "structs.h"
#include "inter_res.h"

/*
 * This is the main function called when getting a query from stdin apart from
 * the string "F". Calls ReadQuery to break the string into predicates and views
 * that will be inserted in the current batch node.
 */
int InsertToQueryBatch(batch_listnode** batch, char* query);

/*
 * Takes a query that was given from the input and an allocated batch_listnode,
 * breaks the query into relations, predicates and views, initializes a predicate_list
 * that holds the filter and join predicates and sets them to the current batch_listnode.
 */
int ReadQuery(batch_listnode **, char* );

/*
 * Every batch listnode contains a query given from the stdin. This query
 * consists of one or more predicates that are stored in a list. The predicate
 * is given as a string, it gets tokenized and if it is a filter predcate it is
 * stored at the beggining of the list, else if it is a join predicate it is stored
 * at the end of the list.
 */
int InsertPredicate(predicates_listnode**,char*);

/*
 * Gets a filter predicate as a string, tokenizes and sets a struct
 * filter_pred appropriately
 */
void TokenizeFilterPredicate(char* predicate, filter_pred **filter_p);

/*
 * Gets a join predicate as a string, tokenizes and sets a struct
 * join_pred appropriately
 */
void TokenizeJoinPredicate(char* predicate, join_pred **join_p);

/*
 * Gets a batch_listnode that contains a single query and executes all the
 * predicates it contains. By calling the ReturnExecPred function, it first
 * executes all the filter predicates, then it executes the joins that contain
 * one or more relations that are already in the intermediate result and finally
 * executes the joins whose both relations are not in the intermediate result.
 */
void ExecuteQuery(batch_listnode* curr_query,relation_map* rel_map, scheduler*);

/*
 * Gets a batch listnode, iterates through the predicates list and returns the predicate
 * to be executed. First returns filters, then joins that that contain one or more relations
 * that are already in the intermediate result and finally the joins whose both relations
 * are not in the intermediate result.
 */
predicates_listnode* ReturnExecPred(batch_listnode* curr_query,inter_res* intermediate_result);

/*
 * Initializes a query string array struct that is used to store predicates before their insertion
 * to a predicate list and views.
 */
int InitialiseQueryString(query_string_array** my_var, int elements, char* str, char* delimeter);

/* Frees the allocated space of a query_string_array */
void FreeQueryString(query_string_array* my_var);

/* Frees the allocated space of a batch list */
void FreeBatch(batch_listnode* batch);

/*
 * In the case that NULL is returned the other predicates are not executed
 * and so this function is called to free the rest of the predicates list
 */
void FreePredicateList(predicates_listnode* head);

/* Frees a single predicates_listnode (the one that was just executed) */
void FreePredListNode(predicates_listnode *current);

/* Helper function for printing the queries in a batch */
void PrintBatch(batch_listnode* batch);

/* Helper function for printing a predicates_list */
void PrintPredList(predicates_listnode* head);

#endif
