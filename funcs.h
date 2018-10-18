#ifndef FUNCS_H
#define FUNCS_H

#include "structs.h"

/** Radix Hash Join */
result* RadixHashJoin(relation *relR, relation* relS);

int insert_result(struct result_listnode **, result*);

void print_result_list(struct result_listnode*);

void free_result_list(struct result_listnode*);

uint32_t hash_function_1(int32_t, int);

int init_index(index **, int, int);


#endif