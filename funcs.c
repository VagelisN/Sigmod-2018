#include "funcs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define N_LSB 3
#define N_HASH_2 5

result* RadixHashJoin(relation *relR, relation* relS)
{

	//Create Histogram,Psum,R',S'
	ReorderedRelation* NewR = NULL;
	ReorderedRelation* NewS = NULL; 
	ReorderArray(relR, N_LSB, &NewR);
	ReorderArray(relS, N_LSB, &NewS);

	for (int i = 0; i < relR->num_tuples; ++i)
	{
		printf("%2d %2d || %2d %2d\n", relR->tuples[i].Value, relR->tuples[i].RowId, NewR->RelArray->tuples[i].Value, NewR->RelArray->tuples[i].RowId);
	}
	printf("\n");
	for (int i = 0; i < relS->num_tuples; ++i)
	{
		printf("%2d %2d || %2d %2d\n", relS->tuples[i].Value, relS->tuples[i].RowId, NewS->RelArray->tuples[i].Value, NewS->RelArray->tuples[i].RowId);
	}


		int i;
	printf("Hist:\n");
	for (i = 0; i < NewR->Hist_size; ++i)
	{
		printf("%d %d\n", NewR->Hist[i][0], NewR->Hist[i][1]);
	}
	printf("--------------------------------------\n");
	printf("Psum:\n");
	for (i = 0; i < NewR->Hist_size; ++i)
	{
		printf("%d %d\n", NewR->Psum[i][0], NewR->Psum[i][1]);
	}


	//for every bucket
	struct result_listnode* results= NULL;
	for (i = 0; i < NewR->Hist_size; ++i)
	{
		if (NewR->Hist[i][1] != 0 && NewS->Hist[i][1] != 0)
		{
			
			if( NewR->Hist[i][1] >= NewS->Hist[i][1])
			{
				bc_index* indS;
				CreateIndex(NewS,&indS,i);
				//Get results
				GetResults(NewR,NewS,indS,&results,i,0);
				

			}
			else
			{
				//Create index for R
				bc_index* indR;
				CreateIndex(NewR,&indR,i);
				//GetResults
				GetResults(NewS,NewR,indR,&results,i,1);
			}
		}

	}
	print_result_list(results);


}

int GetResults(ReorderedRelation* full_relation,ReorderedRelation* indexed_relation,bc_index * ind,struct result_listnode ** res,int curr_bucket,int r_s)
{
	int i ,j, start , end, hash_value, sp;
	if(full_relation->Hist[curr_bucket][1]!=0)
	{
		start = full_relation->Psum[curr_bucket][1];
		end = start + full_relation->Hist[curr_bucket][1];
		printf("%d %d\n",start, end );
		for (i = start; i < end; ++i)
		{
			hash_value = hash_function_2(full_relation->RelArray->tuples[i].Value);
			if( ind->bucket[hash_value] != -1)
			{
				//printf("hash_value%d \n",hash_value );
				//find values
				if(indexed_relation->RelArray->tuples[(ind->bucket[hash_value]-1)].Value == full_relation->RelArray->tuples[i].Value)
				{
					result *curr_res = malloc(sizeof(result));
					//index is on relation S
					if (r_s ==0)
					{
						curr_res->key_R = full_relation->RelArray->tuples[i].RowId;
						curr_res->key_S = indexed_relation->RelArray->tuples[(ind->bucket[hash_value]-1)].RowId;
					}
					else 
					{
						curr_res->key_S = full_relation->RelArray->tuples[i].RowId;
						curr_res->key_R = indexed_relation->RelArray->tuples[(ind->bucket[hash_value]-1)].RowId;
					}

					insert_result(res, curr_res);
				}
				sp = (ind->bucket[i]-1);
				while( ind->chain[sp] != 0)
				{
					if(indexed_relation->RelArray->tuples[(ind->chain[hash_value]-1)].Value == full_relation->RelArray->tuples[i].Value)
					{
						result *curr_res = malloc(sizeof(result));
						if (r_s ==0)
						{
							curr_res->key_R = full_relation->RelArray->tuples[i].RowId;
							curr_res->key_S = indexed_relation->RelArray->tuples[(ind->bucket[hash_value]-1)].RowId;
						}
						else 
						{
							curr_res->key_S = full_relation->RelArray->tuples[i].RowId;
							curr_res->key_R = indexed_relation->RelArray->tuples[(ind->bucket[hash_value]-1)].RowId;
						}
						insert_result(res, curr_res);
					}	
					sp = ind->chain[sp]-1;
				}
			}
		}
	}
}


int CreateIndex(ReorderedRelation *rel, bc_index** ind,int curr_bucket)
{
	int start,end,i,hash_value;

	start = rel->Psum[curr_bucket][1];
	end = start + rel->Hist[curr_bucket][1];
	printf("%d ,%d\n",start, end );

	init_index(ind , N_HASH_2, rel->Hist[curr_bucket][1]);

	//Hash every value of the bucket with H2 and set up bucket and chain
	for (i = end-1; i >= start; --i)
	{
		hash_value = hash_function_2(rel->RelArray->tuples[i].Value);
		//first encounter
		if ((*ind)->bucket[hash_value] == -1 )
		{
			(*ind)->bucket[hash_value] = i+1;
			(*ind)->chain[i] = 0;
		}

		//second or later encounter ->also need to adjust chain
		else
		{
			//while chain is not -1 go to the next
			int sp = (*ind)->bucket[hash_value] - 1;
			while( (*ind)->chain[sp] != 0)
			{
				sp = (*ind)->chain[sp]-1;
			} 
			(*ind)->chain[sp]=i+1;
			(*ind)->chain[i] = 0;
		}
	}
	PrintIndex((*ind));
	return 0;
}

void PrintIndex(bc_index* ind)
{
	int i;
	int sp;
	for (i = 0; i < N_HASH_2; ++i)
	{
		if (ind->bucket[i] != -1)
		{
			printf("bucket[%d] = %d\n",i , ind->bucket[i] );
			sp = (ind->bucket[i]-1);
			while( ind->chain[sp] != 0)
			{
				printf("chain[%d] = %d\n",ind->bucket[i]-1, ind->chain[ind->bucket[i]-1]);
				sp = ind->chain[sp]-1;
			}
		}
	}
}

uint32_t hash_function_1(int32_t num, int n)
{
	uint32_t mask = 0b11111111111111111111111111111111;
	mask = mask<<32-n;
	mask =mask >> 32-n;
	uint32_t hash_value = num & mask;
	return hash_value;
}

uint32_t hash_function_2(int32_t num)
{
	return num%5;
}

int init_index(bc_index** ind, int num_bucket,int num_chain)
{
	(*ind) = malloc(sizeof(index));
	(*ind)->bucket = malloc(num_bucket*sizeof(int));
	(*ind)->chain = malloc(num_chain*sizeof(int));
	int i;
	for (i = 0; i < num_bucket; ++i)
	{
		(*ind)->bucket[i] = -1;
	}
	return 0;
}


int insert_result(struct result_listnode **head, result *res)
{
	//if the list is empty create the first node and insert the first result
	if( (*head) == NULL )
	{
		(*head)=malloc(sizeof(struct result_listnode));

		(*head)->buff = malloc(RESULTLIST_MAX_BUFFER * sizeof(char));
		(*head)->current_load = 1;
		(*head)->next = NULL;

		memcpy((*head)->buff,res,sizeof(result));

	}
	//else find the first node with available space
	else
	{
		struct result_listnode *temp = (*head);
		//printf("%ld\n",(temp->current_load*sizeof(result)) + sizeof(result));
		while( ((temp->current_load*sizeof(result)) + sizeof(result)) > RESULTLIST_MAX_BUFFER)
		{
			if ( temp->next != NULL) temp = temp->next;
			//if all nodes are full create a new one
			else 
			{
				temp->next = malloc(sizeof(struct result_listnode));
				temp->next->buff = malloc(RESULTLIST_MAX_BUFFER * sizeof(char));
				(temp)->next->current_load = 1;
				temp->next->next = NULL;
				memcpy(temp->next->buff,res,sizeof(result));
				return 0;
			}
		}
		//found the last, make the insertion
		void* data = temp->buff;
		data += (temp->current_load*sizeof(result));
		memcpy(data, res, sizeof(result));
		temp->current_load ++;
		return 0;
	}
}

void print_result_list(struct result_listnode* head)
{
	int temp_curr_load;
	void* data;
	result res;
	while(head!=NULL)
	{
		data = head->buff;
		temp_curr_load = head->current_load;
		printf("%d\n",temp_curr_load );
		while(temp_curr_load > 0)
		{
			memcpy(&res,data,sizeof(result));
			printf("Rowid R %d Rowid S %d\n" ,res.key_R,res.key_S );
			data += sizeof(result);
			temp_curr_load --;
		}
		head = head->next;
	}
}

void free_result_list(struct result_listnode* head)
{
	struct result_listnode* temp;
	while(head != NULL)
	{
		temp = head;
		head=head->next;
		free(temp->buff);
	}
}