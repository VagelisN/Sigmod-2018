#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>

#define RESULT_MAX_BUFFER 1024
#define N_LSB 5
#define N_HASH_2 8

/** Type definition for a tuple */
typedef struct tuple
{
	int32_t Value;
	int32_t RowId;
}tuple;

/**
* Type definition for a relation
* It consists of an array of tuples and the size of a relation
*/
typedef struct relation
{
	tuple* tuples;
	uint32_t num_tuples;
}relation;

 typedef struct result
 {
 	char *buff;
 	struct result* next;
 	int current_load;

 }result;

typedef struct result_tuples
{
	tuple tuple_R;
	tuple tuple_S;
}result_tuples;


typedef struct ReorderRelation 
{
	int Hist_size;
	int **Psum;
	int **Hist;
	relation* RelArray;
} ReorderedRelation;

typedef struct bc_index
{
	int index_size;
	int start;
	int end;
	int* bucket;
	int* chain;
}bc_index;


#endif