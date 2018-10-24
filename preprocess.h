#ifndef PREPROCESS_H
#define PREPROCESS_H

#include "structs.h"

relation* ToRow(int** OriginalArray, int RowToJoin, relation* NewRel);

void ReorderArray(relation* RelArray, int n_lsb, ReorderedRelation** NewRel);

int CheckMalloc(void* ptr, char* txt);

void CheckBucketSizes(int** Hist, int hist_size);

#endif