#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>

#define CACHE_SIZE 32768 //L1 cache is 32 KB
#define RESULT_MAX_BUFFER 1048576 //number of bits in a result node buffer
#define N_LSB 5 //number of least significant bits used in H1

/** Type definition for a tuple */
typedef struct tuple
{
	uint64_t value;
	uint64_t row_id;
}tuple;

/*
 * Type definition for a relation
 * It consists of an array of tuples and the size of a relation
 */
typedef struct relation
{
	tuple* tuples;
	uint64_t num_tuples;
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
	uint64_t current_load;

}result;

/** Struct that contains two tuples as a result from the join */
typedef struct result_tuples
{
	tuple tuple_R;
	tuple tuple_S;
}result_tuples;

typedef struct reorder_relation
{
	int hist_size;
	int *psum;
	int *hist;
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

/* list that holds the names of the files containing the relations
 * and their respective file descriptors
 */

typedef struct relation_listnode
{
	char* filename;
	int fd;
	struct relation_listnode *next;
}relation_listnode;


/* struct that holds the relations after being mapped
 * from the relation files given. it contains
 * the number of rows, the number of comuns
 * and an array that has pointers to the start of each column
 */
typedef struct relation_map
{
	uint64_t num_tuples;
	uint64_t num_columns;
	uint64_t **columns;
}relation_map;

typedef struct query_string_array
{
  char **data;
  int num_of_elements;
}query_string_array;

#endif
