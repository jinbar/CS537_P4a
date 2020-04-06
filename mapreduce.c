#include "mapreduce.h"
#include <stdio.h> 
#include <stdlib.h> 
#include <pthread.h>
#include <string.h> 
#include <sys/stat.h>
#include <semaphore.h>

// TODO: expand the arrays if it overflows in size
// TODO: figure out when we free all of the things we've allocated so far

struct Node { 
    char *key;
	char *value;
    struct Node *next; 
};

struct Node** reduce_map;

void push_node(struct Node ** head, char* new_key, char* new_value) {
    struct Node* new_node = (struct Node*) malloc(sizeof(struct Node));
    new_node->key = (char*) malloc(30 * sizeof(char));
    new_node->value = (char*) malloc(30 * sizeof(char));
    strcpy(new_node->key, new_key);
    strcpy(new_node->value, new_value);
    new_node->next = *head;
    *head = new_node;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0') {
        hash = hash * 33 + c;
    }
    return hash % num_partitions;
}

void MR_EmitToCombiner(char *key, char *value) {
    struct Node** combine_map;
    combine_map = (struct Node**) malloc(1000 * sizeof(struct Node*));
    MR_EmitToCombiner_Helper(key, value, *combine_map);
}

void MR_EmitToCombiner_Helper(char* key, char* value, struct Node*** combine_map) {
    
}

void MR_EmitToReducer(char *key, char *value) {

}

char* get_next_combine(char* key) {

}

char* get_next_reduce(char* key, int partition_number) {

}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition) {
            map(argv[1]);
		}

