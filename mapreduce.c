#include "mapreduce.h"
#include <stdio.h> 
#include <stdlib.h> 
#include <pthread.h>
#include <string.h> 
#include <sys/stat.h>
#include <semaphore.h>

// Defining variables
typedef struct map_thread_args { 
    char* filename;
    Mapper map;
    int num_mappers;
    Reducer reduce;
    int num_reducers;
    Combiner combine;
} map_thread_args;

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

int ut_size = 1000;
int aut_size = 1000;

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

    if (unique_tracker_index == ut_size) {
        char** temp = (char**) malloc(2 * unique_tracker_index * sizeof(char*));
        for (int i = 0; i < unique_tracker_index; i++) {
            temp[i] = (char*) malloc(30 * sizeof(char));
            strcpy(temp[i], unique_tracker[i]);
        }
        for (int i = unique_tracker_index; i < unique_tracker_index * 2; i++) {
            temp[i] = (char*) malloc(30 * sizeof(char));
        }
        unique_tracker = temp;
        ut_size = 2 * ut_size;
    }

    for (int i = 0 ; i < all_unique_tracker_index; i++) {
        if (strcmp(key, all_unique_tracker[i]) == 0) {
            return;
        }
    }
    strcpy(all_unique_tracker[all_unique_tracker_index], key);
    all_unique_tracker_index++;

    if (all_unique_tracker_index == aut_size) {
        char** temp = (char**) malloc(2 * all_unique_tracker_index * sizeof(char*));
        for (int i = 0; i < all_unique_tracker_index; i++) {
            temp[i] = (char*) malloc(30 * sizeof(char));
            strcpy(temp[i], all_unique_tracker[i]);
        }
        for (int i = all_unique_tracker_index; i < all_unique_tracker_index * 2; i++) {
            temp[i] = (char*) malloc(30 * sizeof(char));
        }
        all_unique_tracker = temp;
        aut_size = 2 * aut_size;
    }
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

void* thread(void* args) {
    char* filename = ((map_thread_args *) args)-> filename;
    Mapper map = ((map_thread_args *) args)-> map;
    int num_mappers = ((map_thread_args *) args)-> num_mappers;
    int num_reducers = ((map_thread_args *) args)-> num_reducers;
    Combiner combine = ((map_thread_args *) args)-> combine;

    // Allocate all of the variables required for the thread
    n_mappers = num_mappers;
    n_reducers = num_reducers;
    unique_tracker = (char**) malloc(ut_size * sizeof(char*));
    for (int i = 0; i < ut_size; i++) {
        unique_tracker[i] = (char*) malloc(30 * sizeof(char));
    }
    combine_map = (struct Node**) malloc(aut_size * sizeof(struct Node*));

    // Map
    map(filename);
    
    // Combine
    for (int i = 0; i < unique_tracker_index; i++) {
        combine(unique_tracker[i], get_next_combine);
    }
    unique_tracker_index = 0;

    return NULL;
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition) {
            reduce_map = (struct Node **) malloc(num_reducers * sizeof(struct Node*));
            all_unique_tracker = (char**) malloc(aut_size * sizeof(char*));
            for (int i = 0; i < aut_size; i++) {
                all_unique_tracker[i] = (char*) malloc(30 * sizeof(char));
            }
            for (int a = 1; a < argc; a++) {
                map_thread_args* map_args = (map_thread_args *) malloc(sizeof(map_thread_args));
                map_args->filename = argv[a];
                map_args->map = map;
                map_args->num_mappers = num_mappers;
                map_args->reduce = reduce;
                map_args->num_reducers = num_reducers;
                map_args->combine = combine;
                pthread_t tid[argc - 1];
                pthread_create(&tid[a], NULL, thread, (void*) map_args);
                pthread_join(tid[a], NULL);
            }


            // Reduce
            for (int i = 0; i < all_unique_tracker_index; i++) {
                unsigned long index = MR_DefaultHashPartition(all_unique_tracker[i], num_reducers);
                reduce(all_unique_tracker[i], NULL, get_next_reduce, index);
            }

		}

