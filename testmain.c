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
		relR.tuples[i].RowId = i;
		relR.tuples[i].Value = i*3; 
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
	insert_result(&head, &res2);
	insert_result(&head, &res3);
	insert_result(&head, &res1);
	insert_result(&head, &res2);
	insert_result(&head, &res3);
	insert_result(&head, &res1);
	insert_result(&head, &res2);
	insert_result(&head, &res3);
		insert_result(&head, &res1);
	insert_result(&head, &res2);
	insert_result(&head, &res3);
		insert_result(&head, &res1);
	insert_result(&head, &res2);
	insert_result(&head, &res3);
		insert_result(&head, &res1);
	insert_result(&head, &res2);
	insert_result(&head, &res3);
		insert_result(&head, &res1);
	insert_result(&head, &res2);
	insert_result(&head, &res3);
		insert_result(&head, &res1);
	insert_result(&head, &res2);
	insert_result(&head, &res3);
		insert_result(&head, &res1);
	insert_result(&head, &res2);
	insert_result(&head, &res3);


	printf("printing result list\n");
	print_result_list(head);
	printf("%d\n",hash_function_1(70,7));



	free_result_list(head);
	free(relR.tuples);
	return 0;
}