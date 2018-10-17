#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>

/** Type definition for a tuple */
typedef struct tuple
{
	int32_t key;
	int32_t payload;
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

/**
 * Type definition for a relation.
 * It consists of an array of tuples and a size of the relation.
 */

 typedef struct result
 {

 }result;

#endif