#ifndef FUNCS_H
#define FUNCS_H

#include "structs.h"

/** Radix Hash Join */
result* RadixHashJoin(relation *relR, relation* relS);

int insert_result(struct result_listnode **, result*);

void print_result_list(struct result_listnode*);

void free_result_list(struct result_listnode*);

uint32_t hash_function_1(int32_t, int);

uint32_t FindNextPrime(uint32_t);

uint32_t hash_function_2(int32_t,uint32_t);

void ReorderArray(relation*, int, ReorderedRelation** );

int init_index(bc_index **, int);

int CreateIndex(ReorderedRelation*, bc_index**,int);

void PrintIndex(bc_index* ind);

int GetResults(ReorderedRelation*,ReorderedRelation* ,bc_index *,struct result_listnode **, int, int);

#endif