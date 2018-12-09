#ifndef PREPROCESS_H
#define PREPROCESS_H

#include "structs.h"

/*
 * Reorders an row stored array so it is sorted based on the values that belong to the
 * same bucket. Stores the ordered array , the histogram and the Psum array in a
 * ReorderedRelation variable.
 */
void ReorderArray(relation* RelArray, int n_lsb, reordered_relation** NewRel);

/* Free the allocated memory used for ReorderedRelation variables */
void FreeReorderRelation(reordered_relation *rel);

/* Free the allocated memory used for relation variables. */
void FreeRelation(relation *rel);

/* Checks if the return value of a malloc is NULL */
int CheckMalloc(void* ptr, char* txt);

/* Checks whether a bucket size exceeds L1 cache (32 KB).
 * Prints a warning if a bucket can't fit in L1 cache.
 */
void CheckBucketSizes(int* Hist, int hist_size);

#endif
