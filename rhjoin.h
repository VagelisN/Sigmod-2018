#ifndef RHJOIN_H
#define RHJOIN_H

#include "structs.h"

/** Radix Hash Join */
result* RadixHashJoin(relation* relR, relation* relS);

uint32_t HashFunction1(int32_t, int);

uint32_t HashFunction2(int32_t, uint32_t);

int InitIndex(bc_index**, int);

int CreateIndex(ReorderedRelation*, bc_index**, int);

void PrintIndex(bc_index* ind);

int GetResults(ReorderedRelation*, ReorderedRelation* , bc_index *, struct result**, int, int);

#endif