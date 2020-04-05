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

struct Reducer_Node {
    struct Node** combined_map;
    struct Reducer_Node *next;
};
struct Reducer_Node * reducer_head;
struct Reducer_Node * reducer_head_runner;

char** global_map;
int global_index = 0;

char** unique_tracker;
int unique_tracker_index = 0;

char** all_unique_tracker;
int all_unique_tracker_index = 0;

struct Node** unique;
int get_next_combine_index = 0;

void push_word(struct Node ** head, char* new_key, char* new_value) {
    struct Node* new_node = (struct Node*) malloc(sizeof(struct Node));
    new_node->key = (char*) malloc(30 * sizeof(char));
    new_node->value = (char*) malloc(30 * sizeof(char));
    strcpy(new_node->key, new_key);
    strcpy(new_node->value, new_value);
    new_node->next = *head;
    *head = new_node;
}

void push_unique(struct Reducer_Node** head, struct Node** combined_map) {
    struct Reducer_Node* new_node = (struct Reducer_Node*) malloc(sizeof(struct Reducer_Node));
    new_node->combined_map = (struct Node**) malloc(10000 * sizeof(struct Node *));
    for (int i = 0; i < unique_tracker_index; i++) {
        unsigned long index = MR_DefaultHashPartition(unique_tracker[i], 10000);
        push_word(&new_node->combined_map[index], combined_map[index]->key, combined_map[index]->value);
    }
    new_node->next = *head;
    *head = new_node;
    reducer_head_runner = *head;
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

    int i;
    for (i = 0 ; i < unique_tracker_index; i++) {
        if (strcmp(key, unique_tracker[i]) == 0) {
            break;
        }
    }

    if (i == unique_tracker_index) {
        strcpy(unique_tracker[unique_tracker_index], key);
        unique_tracker_index++;        
    }

    for (i = 0 ; i < all_unique_tracker_index; i++) {
        if (strcmp(key, all_unique_tracker[i]) == 0) {
            break;
        }
    }

    if (i == all_unique_tracker_index) {
        strcpy(all_unique_tracker[all_unique_tracker_index], key);
        all_unique_tracker_index++;           
    }
}

void MR_EmitToReducer(char *key, char *value) {
	unsigned long index = MR_DefaultHashPartition(key, 10000);
    push_word(&unique[index], key, value);
    // printf("%s:%s\n", unique[index]->key, unique[index]->value);
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
    unsigned long index = MR_DefaultHashPartition(key, 10000);
    if (reducer_head_runner == NULL) {
        reducer_head_runner = reducer_head;
        return NULL;
    }
    if (reducer_head_runner->combined_map[index] == NULL) {
        reducer_head_runner = reducer_head_runner->next;
        return "0";
    }
    char * temp = reducer_head_runner->combined_map[index]->value;
    reducer_head_runner = reducer_head_runner->next;
    return temp;
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition) {
            // Allocate memory for everything
            global_map = (char **) malloc(10000 * sizeof(char *));
            unique_tracker = (char **) malloc(10000 * sizeof(char *));
            all_unique_tracker = (char **) malloc(10000 * sizeof(char *));
            for (int i = 0; i < 10000; i++) {
                global_map[i] = (char * ) malloc(30 * sizeof(char));
                unique_tracker[i] = (char *) malloc(30 * sizeof(char));
                all_unique_tracker[i] = (char *) malloc(30 * sizeof(char));
            }
            unique = (struct Node**) malloc(10000 * sizeof(struct Node *));

            // Put everything in hashmap linked list to be processed
            for (int a = 1; a < argc; a++) {
                // Now map is all the of words. might not be unique
                map(argv[a]);

                // Combining all of the unique words. Now unique has all the unique word counts
                for (int i = 0; i < unique_tracker_index; i++) {
                    combine(unique_tracker[i], get_next_combine);
                    get_next_combine_index = 0;
                    // printf("%s\n", unique_tracker[i]);
                }

                // Add all of the combined unique maps to our reduced linked list
                push_unique(&reducer_head, unique);
                global_index = 0;
                unique_tracker_index = 0;
            }

            // Reduce one by one
            for (int i = 0; i < all_unique_tracker_index; i++) {
                reduce(all_unique_tracker[i], NULL, get_next_reduce, 1);
                reducer_head_runner = reducer_head;
            }
		}

