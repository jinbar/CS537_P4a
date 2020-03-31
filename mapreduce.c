#include "mapreduce.h"
#include <stdio.h> 
#include <stdlib.h> 
#include <pthread.h>
#include <string.h> 
#include <sys/stat.h>
#include <semaphore.h>

typedef struct Node { 
    char *key;
	int val;
    struct Node *next; 
} Node;

char* global_map[5381];
Node* unique[5381];
int map_index = 0;
int combine_index = 0;

void push(struct Node** head_ref, char* new_key) {
    struct Node* new_node = (struct Node*) malloc(sizeof(struct Node)); 
    new_node->key  = new_key; 
    new_node->next = (*head_ref); 
    (*head_ref) = new_node; 
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_EmitToCombiner(char *key, char *value) {
	global_map[map_index] = key;
	map_index++;
	printf("%s %s\n", key, value);
}

void MR_EmitToReducer(char *key, char *value) {
	printf("%s %s\n", key, value);
}

char* get_next_cg(char *key) {
	char * temp = global_map[combine_index];
	combine_index++;
	return(temp);
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition) {
			// at this point all we have is an array with all the individual words.
			// also they're all 1 so theres no point in adding
			map(argv[1]);

		}
