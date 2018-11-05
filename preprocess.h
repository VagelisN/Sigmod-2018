#ifndef PREPROCESS_H
#define PREPROCESS_H

#include "structs.h"


// Takes a table and converts it to an array of tuples for faster join access 
relation* ToRow(int** OriginalArray, int RowToJoin, relation* NewRel);


/*
 * Reorders an row stored array so it is sorted based on the values that belong to the 
 * same bucket. Stores the ordered array , the histogram and the Psum array in a 
 * ReorderedRelation variable.
 */
void ReorderArray(relation* RelArray, int n_lsb, ReorderedRelation** NewRel);

/* Free the allocated memory used for ReorderedRelation variables */
void FreeReorderRelation(ReorderedRelation *rel);

/* Checks if the return value of a malloc is NULL */
int CheckMalloc(void* ptr, char* txt);

/* Checks whether a bucket size exceeds L1 cache (32 KB).
 * Prints a warning if a bucket can't fit in L1 cache.
 */
void CheckBucketSizes(int** Hist, int hist_size);

#endif