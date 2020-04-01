#include "mapreduce.h"
#include <stdio.h> 
#include <stdlib.h> 
#include <pthread.h>
#include <string.h> 
#include <sys/stat.h>
#include <semaphore.h>

struct Node { 
    char *key;
	char* value;
    struct Node *next; 
};

char** global_map;
int global_index = 0;
struct Node* unique[5381];
int get_next_combine_index = 0;
int get_next_reduce_index = 0;

void push(struct Node** head_ref, char* new_key, char* new_value) {
    struct Node* new_node = (struct Node*) malloc(sizeof(struct Node)); 
    new_node->key = new_key;
    new_node->value = new_value;
    new_node->next = (*head_ref);
    (*head_ref) = new_node;
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
    // global_map[global_index] = key;
    // memcpy(key, global_map[global_index], sizeof(key) + 1);
    strcpy(global_map[global_index], key);
    global_index++;
}

void MR_EmitToReducer(char *key, char *value) {
	unsigned long index = MR_DefaultHashPartition(key, 5381);
    if (unique[index] == NULL) {
        struct Node* new_node = (struct Node*) malloc(sizeof(struct Node));
        new_node->key = key;
        new_node->value = value;
        unique[index] = new_node;
    } else {
        push(&(unique[index]), key, value);
    }
}

char* get_next_combine(char* key) {
    for (int i = get_next_combine_index; i < global_index; i++) {
        if (strcmp(global_map[i], key) == 0) {
            get_next_combine_index = i + 1;
            return key;
        }
    }
    return NULL;
}

char* get_next_reduce(char* key, int partition_number) {
    unsigned long index = MR_DefaultHashPartition(key, 5381);
    struct Node * runner = unique[index];
    while (runner != NULL) {
        if (strcmp(runner->key, key) == 0) {
            // printf("%s %s", runner->key, runner->value);
            return runner->value;
        }
        runner = runner->next;
    }
    return NULL;
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition) {
            global_map = (char **)malloc(5831 * sizeof(char *)); 
            for (int i = 0; i < 5831; i++) {
                global_map[i] = (char * ) malloc(30 * sizeof(char));
            }  

			map(argv[1]);

            for (int i = 0; i < global_index; i++) {
                printf("%s\n", global_map[i]);
            }
		}
