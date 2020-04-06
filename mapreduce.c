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

// Global variables
struct Node** reduce_map;
char** all_unique_tracker;
int all_unique_tracker_index = 0;
int aut_size = 1000;
int n_mappers;
int n_reducers;

// Variables specific to threads
struct thread_variables { 
    struct Node** combine_map;
    char** unique_tracker;
    int unique_tracker_index;
    int ut_size;
};
struct thread_variables** thread_variable_holder;
long* thread_id_holder;
int thread_id_holder_index = 0;
int thread_id_holder_size;

// Helper functions
void MR_EmitToCombiner_Helper(char* key, char* value, struct Node** head);
void add_node(struct Node** head, char* key, char* value);
char* delete_node(struct Node **head_ref, char* key);
int index_finder();

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0') {
        hash = hash * 33 + c;
    }
    return hash % num_partitions;
}

void MR_EmitToCombiner(char *key, char *value) {
    int thread_index = index_finder();
    struct thread_variables* tv = thread_variable_holder[thread_index];

    unsigned long index = MR_DefaultHashPartition(key, 1000);
    add_node(&tv->combine_map[index], key, value);

    for (int i = 0 ; i < tv->unique_tracker_index; i++) {
        if (strcmp(key, tv->unique_tracker[i]) == 0) {
            return;
        }
    }
    strcpy(tv->unique_tracker[tv->unique_tracker_index], key);
    tv->unique_tracker_index++;

    if (tv->unique_tracker_index == tv->ut_size) {
        char** temp = (char**) malloc(2 * tv->unique_tracker_index * sizeof(char*));
        for (int i = 0; i < tv->unique_tracker_index; i++) {
            temp[i] = (char*) malloc(30 * sizeof(char));
            strcpy(temp[i], tv->unique_tracker[i]);
        }
        for (int i = tv->unique_tracker_index; i < tv->unique_tracker_index * 2; i++) {
            temp[i] = (char*) malloc(30 * sizeof(char));
        }
        tv->unique_tracker = temp;
        tv->ut_size = 2 * tv->ut_size;
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
    int thread_index = index_finder();
    struct thread_variables* tv = thread_variable_holder[thread_index];
    unsigned long index = MR_DefaultHashPartition(key, 1000);
    return delete_node(&tv->combine_map[index], key);
}

char* get_next_reduce(char* key, int partition) {
    return delete_node(&reduce_map[partition], key);
}

void MR_EmitToReducer(char *key, char *value) {
    unsigned long index = MR_DefaultHashPartition(key, n_reducers);
    add_node(&reduce_map[index], key, value);
}

void* thread(void* args) {
    char* filename = ((map_thread_args *) args) -> filename;
    Mapper map = ((map_thread_args *) args) -> map;
    Combiner combine = ((map_thread_args *) args) -> combine;

    // Allocate all of the variables required for the thread
    int index = index_finder();
    thread_variable_holder[index] = (struct thread_variables*) malloc(sizeof(struct thread_variables));
    thread_variable_holder[index]->ut_size = 1000;
    thread_variable_holder[index]->unique_tracker = (char**) malloc(thread_variable_holder[index]->ut_size * sizeof(char*));
    for (int i = 0; i < thread_variable_holder[index]->ut_size; i++) {
        thread_variable_holder[index]->unique_tracker[i] = (char*) malloc(30 * sizeof(char));
    }
    thread_variable_holder[index]->combine_map = (struct Node**) malloc(aut_size * sizeof(struct Node*));

    // Map
    map(filename);
    
    // Combine
    for (int i = 0; i < thread_variable_holder[index]->unique_tracker_index; i++) {
        combine(thread_variable_holder[index]->unique_tracker[i], get_next_combine);
    }
    thread_variable_holder[index]->unique_tracker_index = 0;

    return NULL;
}

int index_finder() {
    for (int i = 0; i < thread_id_holder_size; i++) {
        if (thread_id_holder[i] == pthread_self()) {
            return i;
        }
    }
    return 0;
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition) {
            // Allocate global variables
            reduce_map = (struct Node **) malloc(num_reducers * sizeof(struct Node*));
            all_unique_tracker = (char**) malloc(aut_size * sizeof(char*));
            for (int i = 0; i < aut_size; i++) {
                all_unique_tracker[i] = (char*) malloc(30 * sizeof(char));
            }
            thread_id_holder = (long*) malloc(argc * sizeof(long));
            thread_id_holder_size = argc;
            thread_variable_holder = (struct thread_variables**) malloc(argc * sizeof(struct thread_variables*));
            n_mappers = num_mappers;
            n_reducers = num_reducers;

            // Create the threads
            pthread_t tid[argc - 1];
            for (int a = 1; a < argc; a++) {
                map_thread_args* map_args = (map_thread_args *) malloc(sizeof(map_thread_args));
                map_args->filename = argv[a];
                map_args->map = map;
                map_args->reduce = reduce;
                map_args->combine = combine;

                thread_id_holder[thread_id_holder_index] = pthread_self();
                thread_id_holder_index++;
                pthread_create(&tid[a], NULL, thread, (void*) map_args);
            }

            // Wait for them to finish
            for (int a = 1; a < argc; a++) {
                pthread_join(tid[a], NULL);
            }

            // Reduce
            for (int i = 0; i < all_unique_tracker_index; i++) {
                unsigned long index = MR_DefaultHashPartition(all_unique_tracker[i], num_reducers);
                reduce(all_unique_tracker[i], NULL, get_next_reduce, index);
            }
		}

