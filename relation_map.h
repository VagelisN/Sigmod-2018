#ifndef RELATION_MAP 
#define RELATION_MAP

#include "structs.h"

/* traverses the relation list and for every filename, opens the file and 
 * sets the relation map struct with the number of columns number of tuples 
 * and the pointers to the columns 
 */
int InitRelationMap(relation_listnode*,relation_map*);

/* Prints all the relations from a given array of maps */
void PrintRelationMap(relation_map *, int);

/* Frees the relation map array */
void FreeRelationMap(relation_map *rel_map, int map_size);
#endif