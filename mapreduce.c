#include "mapreduce.h"
#include <stdio.h> 
#include <stdlib.h> 
#include <pthread.h>
#include <string.h> 
#include <sys/stat.h>
#include <semaphore.h>

struct Node { 
    char *key;
	char *value;
    struct Node *next; 
};

char** global_map;
int global_index = 0;

char** unique_tracker;
int unique_tracker_index = 1;

struct Node** unique;
int get_next_combine_index = 0;
int get_next_reduce_index = 0;

void push(unsigned long index, char* new_key, char* new_value) {
    struct Node* new_node = (struct Node*) malloc(sizeof(struct Node));
    new_node->key = (char*) malloc(sizeof(new_key));
    new_node->value = (char*) malloc(sizeof(new_value));
    for (int i = 0; i < sizeof(new_key); i++) {
        *(char *)(new_node->key + i) = *(char *)(new_key + i);
    }
    for (int i = 0; i < sizeof(new_value); i++) {
        *(char *)(new_node->value + i) = *(char *)(new_value + i);
    }
    new_node->next = unique[index];
    unique[index] = new_node;
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
    strcpy(global_map[global_index], key);
    global_index++;
    for (int i = 0 ; i < unique_tracker_index; i++) {
        if (strcmp(key, unique_tracker[i]) == 0) {
            return;
        }
    }
    strcpy(unique_tracker[unique_tracker_index - 1], key);
    unique_tracker_index++;
}

void MR_EmitToReducer(char *key, char *value) {
	unsigned long index = MR_DefaultHashPartition(key, 5381);
    push(index, key, value);
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
int count = 0;
char* get_next_reduce(char* key, int partition_number) {
    unsigned long index = MR_DefaultHashPartition(key, 5381);
    struct Node * runner = unique[index];
    if (count == 1) {
        count = 0;
        return NULL;
    }
    while (runner != NULL) {
        if (strcmp(runner->key, key) == 0) {
            count = 1;
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
            // Allocate memory for everything
            global_map = (char **) malloc(5381 * sizeof(char *));
            unique_tracker = (char **) malloc(5381 * sizeof(char *));
            unique = (struct Node**) malloc(5381 * sizeof(struct Node *));
            for (int i = 0; i < 5381; i++) {
                global_map[i] = (char * ) malloc(30 * sizeof(char));
                unique_tracker[i] = (char *) malloc(30 * sizeof(char));
            }

            // Now map is all the of words. might not be unique
			map(argv[1]);

            // Combining all of the unique words. Now unique has all the unique word counts
            for (int i = 0; i < unique_tracker_index; i++) {
                combine(unique_tracker[i], get_next_combine);
                get_next_combine_index = 0;
            }

            // We reduce
            for (int i = 0; i < unique_tracker_index; i++) {
                reduce(unique_tracker[i], NULL, get_next_reduce, 1);
            }
		}
