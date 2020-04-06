#include "mapreduce.h"
#include <stdio.h> 
#include <stdlib.h> 
#include <pthread.h>
#include <string.h> 
#include <sys/stat.h>
#include <semaphore.h>

// Defining variables
struct Node { 
    char *key;
	char *value;
    struct Node *next; 
};
struct Node** reduce_map;
struct Node** combine_map;

char** unique_tracker;
int unique_tracker_index = 0;

char** all_unique_tracker;
int all_unique_tracker_index = 0;

int n_mappers;
int n_reducers;

// Helper functions
void MR_EmitToCombiner_Helper(char* key, char* value, struct Node** head);
void add_node(struct Node** head, char* key, char* value);
char* delete_node(struct Node **head_ref, char* key);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0') {
        hash = hash * 33 + c;
    }
    return hash % num_partitions;
}

void MR_EmitToCombiner(char *key, char *value) {
    unsigned long index = MR_DefaultHashPartition(key, 1000);
    add_node(&combine_map[index], key, value);
    
    for (int i = 0 ; i < unique_tracker_index; i++) {
        if (strcmp(key, unique_tracker[i]) == 0) {
            return;
        }
    }
    strcpy(unique_tracker[unique_tracker_index], key);
    unique_tracker_index++;

    for (int i = 0 ; i < all_unique_tracker_index; i++) {
        if (strcmp(key, all_unique_tracker[i]) == 0) {
            return;
        }
    }
    strcpy(all_unique_tracker[all_unique_tracker_index], key);
    all_unique_tracker_index++;   
}

void add_node(struct Node** head, char* key, char* value) {
    struct Node* new_node = (struct Node*) malloc(sizeof(struct Node));
    new_node->key = (char*) malloc(30 * sizeof(char));
    new_node->value = (char*) malloc(30 * sizeof(char));
    strcpy(new_node->key, key);
    strcpy(new_node->value, value);
    new_node->next = *head;
    *head = new_node;
}

char* delete_node(struct Node **head_ref, char* key) {
    struct Node* temp = *head_ref;
    struct Node* prev = temp; 
    if (temp != NULL && strcmp(temp->key, key) == 0) {
        char * value = temp->value;
        *head_ref = temp->next;
        free(temp);
        return value; 
    }
    while (temp != NULL && strcmp(temp->key, key) != 0) {
        prev = temp; 
        temp = temp->next; 
    } 
    if (temp == NULL) {
        return NULL;
    }
    char * value = temp->value;
    prev->next = temp->next;
    free(temp);
    return value;
} 

char* get_next_combine(char* key) {
    unsigned long index = MR_DefaultHashPartition(key, 1000);
    return delete_node(&combine_map[index], key);
}

char* get_next_reduce(char* key, int partition) {
    return delete_node(&reduce_map[partition], key);
}

void MR_EmitToReducer(char *key, char *value) {
    unsigned long index = MR_DefaultHashPartition(key, n_reducers);
    add_node(&reduce_map[index], key, value);
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition) {
            // Allocate things
            n_mappers = num_mappers;
            n_reducers = num_reducers;
            reduce_map = (struct Node **) malloc(num_reducers * sizeof(struct Node*));
            unique_tracker = (char**) malloc(1000 * sizeof(char*));
            all_unique_tracker = (char**) malloc(1000 * sizeof(char*));
            for (int i = 0; i < 1000; i++) {
                unique_tracker[i] = (char*) malloc(30 * sizeof(char));
                all_unique_tracker[i] = (char*) malloc(30 * sizeof(char));
            }
            combine_map = (struct Node**) malloc(1000 * sizeof(struct Node*));

            for (int a = 1; a < argc; a++) {
                // Map
                map(argv[a]);
                
                // Combine
                for (int i = 0; i < unique_tracker_index; i++) {
                    combine(unique_tracker[i], get_next_combine);
                }
                unique_tracker_index = 0;
            }

            // Reduce
            for (int i = 0; i < all_unique_tracker_index; i++) {
                unsigned long index = MR_DefaultHashPartition(all_unique_tracker[i], num_reducers);
                reduce(all_unique_tracker[i], NULL, get_next_reduce, index);
            }
		}

