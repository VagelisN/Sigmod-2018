#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>

#define CACHE_SIZE 32768 //L1 cache is 32 KB
#define RESULT_MAX_BUFFER 1048576 //number of bits in a result node buffer
#define N_LSB 5 //number of least significant bits used in H1

/** Type definition for a tuple */
typedef struct tuple
{
	int32_t value;
	int32_t row_id;
}tuple;

/*
 * Type definition for a relation
 * It consists of an array of tuples and the size of a relation
 */
typedef struct relation
{
	tuple* tuples;
	uint32_t num_tuples;
}relation;

/*
 * Type definition for a result node
 * It consists of a buffer that will contain result_tuples
 * A pointer to the next node
 * and the number of records in the node
 */
typedef struct result
{
	char *buff;
	struct result* next;
	int current_load;

}result;

/** Struct that contains two tuples as a result from the join */
typedef struct result_tuples
{
	tuple tuple_R;
	tuple tuple_S;
}result_tuples;

/*
 * reordered relation is a relation that has been hashed with 
 * HashFunction1 (rel_array). It also contains the histogram
 * (number of records in each bucket) and the psum 
 * (where is the start of each bucket) 
 */

typedef struct reordered_relation 
{
	int hist_size;
	int **psum;
	int **hist;
	relation* rel_array;
} reordered_relation;

/*
 * THe index that uses H2 as the hash function
 * Contains:
 * -the size of the index in buckets 
 * -the start and end are pointers to the full array's start and end 
 * of the bucket on which the index is created
 * -the index and chain arrays
 */
typedef struct bc_index
{
	int index_size;
	int start;
	int end;
	int* bucket;
	int* chain;
}bc_index;


#endif