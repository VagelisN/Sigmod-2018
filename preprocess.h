#ifndef PREPROCESS_H
#define PREPROCESS_H

#include "structs.h"

relation* ToRow(int** OriginalArray, int RowToJoin, relation* NewRel);

void ReorderArray(relation* RelArray, int n_lsb, ReorderedRelation** NewRel);

#endif