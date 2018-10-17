#include <stdio.h>
#include <stdlib.h>
#include "funcs.h"

int main(void)
{
	relation relR;
	relR.tuples=malloc(3*sizeof(tuple));
	int i;
	for (i = 0; i < 3; ++i)
	{
		relR.tuples[i].key = i;
		relR.tuples[i].payload = i*3; 
	}
	for (i = 0; i < 3; ++i)
	{
		printf("%d %d\n",relR.tuples[i].key, relR.tuples[i].payload);
	}

	result res1,res2,res3;
	res1.key_R =1;
	res1.key_S =1;

	res2.key_R =2;
	res2.key_S =2;

	res3.key_R =3;
	res3.key_S =3;

	struct result_listnode *head = NULL;

	insert_result(&head, &res1);

	free(relR.tuples);
	return 0;
}