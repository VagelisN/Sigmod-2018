#ifndef RELATION_LIST_H
#define RELATION_LIST_H

#include "structs.h"

/* Inserts relation file in the relation list*/
int RelationListInsert(relation_listnode** , char*);

/* Frees the relation list */
void FreeRelationList(relation_listnode*);

/* Prints the relation list */
void PrintRelationList(relation_listnode*);

#endif