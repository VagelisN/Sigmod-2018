#ifndef PREPROCESS_H
#define PREPROCESS_H

#include "structs.h"


/** Takes a table and converts it to an array of tuples for faster join access */
relation* ToRow(int**, int, relation*);


/*
 * Reorders an row stored array so it is sorted based on the values that belong to the 
 * same bucket. Stores the ordered array , the histogram and the Psum array in a 
 * reordered_relation variable.
 */
void ReorderArray(relation*, int , reordered_relation**);

/* Free the allocated memory used for reordered_relation variables */
void FreeReorderRelation(reordered_relation*);

/* Checks if the return value of a malloc is NULL */
int CheckMalloc(void*, char*);

/* Checks whether a bucket size exceeds L1 cache (32 KB).
 * Prints a warning if a bucket can't fit in L1 cache.
 */
void CheckBucketSizes(int**, int);

#endif