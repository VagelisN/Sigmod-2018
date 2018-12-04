#ifndef RESULTS_H
#define RESULTS_H

#include "structs.h"

/** Inserts a result_tuples in the result list */
int InsertResult(result **, result_tuple*);

/* Returns the number of the results in the result list. */
int GetResultNum(result *);

/* Returns the num-th RowID in the list. */
uint64_t FindResultRowId(result *res, int num);

/** Prints the result list */
void PrintResult(result*);

/** Frees the result list */
void FreeResult(result*);

/** Finds a specific result_tuples in the result */
result_tuple* FindResultTuples(result*, int);

/* Inserts single rowids to the buffer of a result list in the fors of uint64_t */
int InsertRowIdResult(result **, uint64_t *);

/* Prints the tuples in a relation */
void PrintRelation(relation* rel);

#endif
