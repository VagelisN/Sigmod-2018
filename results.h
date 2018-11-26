#ifndef RESULTS_H
#define RESULTS_H

#include "structs.h"

/** Inserts a result_tuples in the result list */
int InsertResult(result **, result_tuples*);

/* Returns the number of the results in the result list. */
int GetResultNum(result *);

/* Returns the num-th RowID in the list. */
uint64_t FindResultRowId(result *res, int num);

/** Prints the result list */
void PrintResult(result*);

/** Frees the result list */
void FreeResult(result*);

/** Finds a specific result_tuples in the result */
result_tuples* FindResultTuples(result*, int);

void CheckResult(result* );

int InsertRowIdResult(result **, uint64_t *);

#endif
