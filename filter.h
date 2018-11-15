#ifndef FILTER_H
#define FILTER_H

int GetResultRowId(result *res, int num);

int InsertFilterRes(result **, tuple*);

int FindResultNum(result *);

int Filter(inter_res** head, int relation_num, relation* rel, char comperator, int constant);

int InsertFilterToInterResult(inter_res** head, int relation_num, result* res, int num_of_results);

#endif
