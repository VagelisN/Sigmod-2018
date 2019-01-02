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
	int i =0;
	struct stat sb;
	uint64_t *map;
	unsigned short int *dist_array = NULL;

	//for every relation text given
	while(head != NULL)
	{
		//open the file
		if ( (head->fd = open(head->filename, O_RDONLY)) == -1 )return 1;

		//find its length (needed for mmap)
  	if (fstat(head->fd , &sb) == -1) return 1;
		int length = sb.st_size;

		//map the file
		map = mmap(NULL,length,PROT_READ,MAP_PRIVATE,head->fd,0);
		if (map==MAP_FAILED)
		{
			printf("map failed \n");
			return 1;
		}

		//the first two ints are the number of tuples and the number of columns
		rel_map[i].num_tuples = map[0];
		rel_map[i].num_columns = map[1];

		rel_map[i].col_stats = malloc((rel_map[i].num_columns * sizeof(column_stats)));
		//alocate num_columns pointers
		rel_map[i].columns = malloc (rel_map[i].num_columns * sizeof(uint64_t*));
		//make every column pointer point to the right column and keep the stats
		map = map + 2;
		for (int j = 0; j < rel_map[i].num_columns; ++j)
		{
			rel_map[i].columns[j] = map;
			map += rel_map[i].num_tuples;

			// compute the max and the min value of each column
			rel_map[i].col_stats[j].f = rel_map[i].num_tuples;
			rel_map[i].col_stats[j].l = rel_map[i].col_stats[j].u = rel_map[i].columns[j][0];
			for (int k = 1; k < rel_map[i].num_tuples ; ++k)
			{
				if (rel_map[i].columns[j][k] >  rel_map[i].col_stats[j].u) 
					rel_map[i].col_stats[j].u = rel_map[i].columns[j][k];
				if(rel_map[i].columns[j][k] < rel_map[i].col_stats[j].l) 
					rel_map[i].col_stats[j].l = rel_map[i].columns[j][k];
			}

			//create a boolean array to check for distinct values
			uint64_t temp_size = rel_map[i].col_stats[j].u - rel_map[i].col_stats[j].l +1;
			if (temp_size > 50000000) temp_size = 50000000;
				
			dist_array = malloc (temp_size *sizeof(unsigned short int));

			for (int k = 0; k < temp_size; ++k)
				dist_array[k] = 1;

			for (int k = 0; k < rel_map[i].num_tuples ; ++k)
			{
				if(temp_size < 50000000)
					dist_array[(rel_map[i].columns[j][k] - rel_map[i].col_stats[j].l)] = 0;
				else 
					dist_array[(rel_map[i].columns[j][k] - rel_map[i].col_stats[j].l)%5000000] = 0;
			}
			rel_map[i].col_stats[j].d = 0;

			for (int k = 0; k < temp_size; ++k)
			{
				if (dist_array[k] == 0)
					rel_map[i].col_stats[j].d++;
			}
			free(dist_array);
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
		printf("%lu %lu\n",rel_map[i].num_tuples,rel_map[i].num_columns);
		for (int j = 0; j < rel_map[i].num_columns; ++j)
		{
			printf("Printing Column: %d\n",j );
			for (int k = 0; k < rel_map[i].num_tuples; ++k)
			{
				printf(" %lu\n",rel_map[i].columns[j][k]);
			}
		}
	}
}
