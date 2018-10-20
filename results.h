#ifndef RESULTS_H
#define RESULTS_H

#include "structs.h"

int InsertResult(struct result_listnode **, result*);

void PrintResultList(struct result_listnode*);

void FreeResultList(struct result_listnode*);

#endif