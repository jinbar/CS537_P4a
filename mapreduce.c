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

char* global_map[5381];
struct Node* unique[5381];

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
    unsigned long index = MR_DefaultHashPartition(key, 5381);
    if (unique[index] == NULL) {
        struct Node* new_node = (struct Node*) malloc(sizeof(struct Node));
        new_node->key = key;
        new_node->value = value;
    } else {
        // Iterate to see if we find an existing node with the key, and then increment the value
        struct Node* runner = unique[index];
        int found = 0;
        while (runner != NULL) {
            if (strcmp(runner->key, key) == 0) {
                int temp_int = atoi(runner->value);
                temp_int++;
                char* temp_str = (char*)malloc(10 * sizeof(char));
                sprintf(temp_str, "%d", temp_int);
                runner->value = temp_str;
                found = 1;
                break;
            }
            runner = runner->next;
        }
        if (found == 0) {
            push(&(unique[index]), key, value);
        }
    }
	printf("%s %s\n", key, value);
}

void MR_EmitToReducer(char *key, char *value) {
	printf("%s %s\n", key, value);
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
