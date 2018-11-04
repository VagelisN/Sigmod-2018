#ifndef RESULTS_H
#define RESULTS_H

#include "structs.h"

/** Inserts a result_tuples in the result list */
int InsertResult(result **, result_tuples*);

/** Prints the result list */
void PrintResult(result*);

/** Frees the result list */
void FreeResult(result*);

/** Finds a specific result_tuples in the result */
result_tuples* FindResultTuples(result*, int);

void CheckResult(result* );

#endif