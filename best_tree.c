#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "structs.h"
#include "best_tree.h"
#include "query.h"


void InitBestTree(best_tree_node*** best_tree, int num_of_relations)
{
  int best_tree_size = (int)pow(2, num_of_relations);
  (*best_tree) = malloc( best_tree_size * sizeof(best_tree_node*));
  for (size_t i = 0; i < best_tree_size; i++) {
    (*best_tree)[i] = malloc(sizeof(best_tree_node));
    (*best_tree)[i]->best_tree = NULL;
    //Find the number of bits in the i
    int num = i, bit_num = 0;
    while(num != 0)
    {
      if (num % 2 == 1)
        bit_num++;
      num = num >> 1;
    }
    //If its a single relation set the variable accordingly
    if( bit_num == 1)(*best_tree)[i]->single_relation = i;
    else (*best_tree)[i]->single_relation = -1;
    (*best_tree)[i]->active_bits = bit_num;

    //Set the column stats here


  }
  return;
}


predicates_listnode* Connected(best_tree_node **best_tree, int num_of_relations, int S, int R, predicates_listnode *list )
{
  //All relations that are active in S will be stored in active_rel array

  //Active rel can be malloced at the JoinEnum function so we dont have to malloc it every time
  short int *active_rel = calloc(num_of_relations, sizeof(short int));
  predicates_listnode *head = best_tree[S]->best_tree;
  while(head != NULL)
  {
    active_rel[ head->join_p->relation1 ] = 1;
    active_rel[ head->join_p->relation2 ] = 1;
    head = head->next;
  }

  //Traverse through the query predicates
  while(list != NULL)
  {
    if(list->join_p->relation1 == R ) {
      if(active_rel[list->join_p->relation2] == 1){
        free(active_rel);
        return list;
      }
    }
    else if (list->join_p->relation2 == R)
    {
      if(active_rel[list->join_p->relation1] == 1){
        free(active_rel);
        return list;
      }
    }
    list = list->next;
  }
  free(active_rel);
  return NULL;
}

void FreeBestTree(best_tree_node **best_tree, int num_of_relations)
{
  int size = pow(2, num_of_relations);
  for (size_t i = 0; i < size; i++) {
    if (best_tree[i]->best_tree != NULL) {
      //FreePredicateList(best_tree[i]->best_tree);
    }
    free(best_tree[i]);
  }
  free(best_tree);
}



int main(int argc, char const *argv[]) {
  best_tree_node **best_tree = NULL;
  InitBestTree(&best_tree, 3);
  for (size_t i = 0; i < pow(2, 3); i++) {
    printf("active_bits: %d | single_relation: %d\n", best_tree[i]->active_bits, best_tree[i]->single_relation);
  }
  FreeBestTree(best_tree, 3);
  return 0;
}
