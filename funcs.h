#ifndef FUNCS_H
#define FUNCS_H

#include "structs.h"

/** Radix Hash Join */
result* RadixHashJoin(relation *relR, relation* relS);

int insert_result(struct result_listnode **, result*);

void print_result_list(struct result_listnode*);

void free_result_list(struct result_listnode*);


#endif