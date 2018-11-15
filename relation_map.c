#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "structs.h"
#include "relation_map.h"


int InitRelationMap(relation_listnode *head,relation_map *rel_map)
{
	int i =0, length;
	struct stat sb;
	void *map;

	//for every relation text given
	while(head != NULL)
	{
		//open the file
		if ( (head->fd = open(head->filename, O_RDONLY)) == -1 )return 1;

		//find its length (needed for mmap)
  		if (fstat(head->fd , &sb) == -1) return 1;
		length = sb.st_size;

		//map the file 
		map = mmap(NULL,length,PROT_READ,MAP_PRIVATE,head->fd,0);
		if (map==MAP_FAILED) 
		{
			printf("map failed \n");
			return 1;
		}

		//the first two ints are the number of tuples and the number of columns
		rel_map[i].num_tuples = *(uint64_t*)map;
		rel_map[i].num_columns = *(uint64_t*)(map+sizeof(uint64_t));

		//alocate num_columns pointers
		rel_map[i].columns = malloc (rel_map[i].num_columns * sizeof(uint64_t*));

		//make every column pointer point to the right column
		map = (map + 2*sizeof(uint64_t));
		for (int j = 0; j < rel_map[i].num_columns; ++j)
		{
			rel_map[i].columns[j] = map;
			map += (rel_map[i].num_tuples * sizeof(uint64_t));
		}

		head = head->next;
		i++;
	}
}

void FreeRelationMap(relation_map *rel_map, int map_size)
{
	for (int i = 0; i < map_size; ++i)
	{
			free(rel_map[i].columns);
	}
	free(rel_map);
}

void PrintRelationMap(relation_map *rel_map , int map_size)
{
	for (int i = 0; i < map_size; ++i)
	{
		printf("Printing Relation: %d\n",i);
		printf("%ld %ld\n",rel_map[i].num_tuples,rel_map[i].num_columns);
		for (int j = 0; j < rel_map[i].num_columns; ++j)
		{
			printf("Printing Column: %d\n",j );
			for (int k = 0; k < rel_map[i].num_tuples; ++k)
			{
				printf(" %ld\n",*(uint64_t*)(rel_map[i].columns[j]+k*sizeof(uint64_t)));
			}
		}
	}
}	