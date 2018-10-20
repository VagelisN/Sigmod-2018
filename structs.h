#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>

#define RESULTLIST_MAX_BUFFER 1024

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
 	int32_t key_R;
 	int32_t key_S;

 }result;

struct result_listnode
{
	char *buff;
 	struct result_listnode* next;
 	int current_load;
};


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
	int* bucket;
	int* chain;
}bc_index;


#endif