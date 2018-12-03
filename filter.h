#ifndef FILTER_H
#define FILTER_H

/* Takes the filter arguments and the inter_res and creates a result list with
 * all the values of the relation that pass the filter. Then inserts the results
 * to the inter_res variable. */
result* Filter(inter_res* head,filter_pred* filter_p, relation_map* map,int *query_relations);

/* Used to insert result with single row_id values to inter_res. This function
 * is being used by Filter and by SelfJoin.*/
int InsertSingleRowIdsToInterResult(inter_res** head, int relation_num, result* res);

#endif
