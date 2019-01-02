#ifndef RHJOIN_H
#define RHJOIN_H

#include "structs.h"



/** Radix Hash Join */
result* RadixHashJoin(relation*, relation*);

/** Hash function that returns the n last bits of a given number */
uint64_t HashFunction1(uint64_t, uint64_t);

/** Hash function that returns the modulo of a number and a given prime number */
uint64_t HashFunction2(uint64_t, uint64_t);

/*
 * Given a number, returns the closest prime number
 * that is greater or equal than the number given
 */
uint64_t FindNextPrime(uint64_t);

/*
 * Initializes a bc_index struct
 * Allocates space for the bucket and chain arrays
 */
int InitIndex(bc_index**, int, int);

/**Frees the H2 index */
int DeleteIndex(bc_index**);


/**
 * Creates the second layer index for a bucket of a relation
 * that already has a first layer index
 */
int CreateIndex(reordered_relation*, bc_index**, int);

/** Prints the second layer H2 index of a bucket*/
void PrintIndex(bc_index* ind);

/**
 * Gets two relations. The first one only has a first layer index and the second one
 * also has a second layer index (ind) for a bucket (curr_bucket). For every element
 * of the first relation's bucket it checks for equalityin the second layer index
 * of the second relation and returns the results
*/
int GetResults(reordered_relation*, reordered_relation* , bc_index *, struct result**, int, int);

/* The function that will perform a join between two buckets. */
void JoinJob(void *arguments);

/* Takes an array of result lists and merges them to one list. */
void MergeResults(result **res, result **res_array, int size);


#endif
