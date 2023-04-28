#include "mr.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hash.h"
#include "kvlist.h"
#include<stdbool.h>
#include<unistd.h>
#include <limits.h>

#define UNUSED(x) (void)(x)
#define HASH_TABLE_SIZE 1000

//Structs for map and reduce phase
typedef struct 
{
    mapper_t mapper;
    kvlist_t* input;
    kvlist_t* output;
} map_thread_args;

typedef struct 
{
    reducer_t reducer;
    kvlist_t* input;
    kvlist_t* output;
} reducer_thread_args;

//Thread Function for reduce phase
void* reducer_thread(void* arg) 
{
    reducer_thread_args* list1 = (reducer_thread_args*) arg;
    reducer_t reducer = list1->reducer;
    kvlist_t* input = list1->input;
    kvlist_t* output = list1->output;
    kvlist_iterator_t* itor = kvlist_iterator_new(input);
    kvpair_t* pair;
    kvlist_t** hash_table = calloc(HASH_TABLE_SIZE, sizeof(kvlist_t*));
    int i;
    for (i = 0; i < HASH_TABLE_SIZE; i++) 
    {
        hash_table[i] = kvlist_new();
    }
    while ((pair = kvlist_iterator_next(itor)) != NULL) 
    {
        unsigned long hash_val = hash(pair->key);
        kvlist_t* list = hash_table[hash_val % HASH_TABLE_SIZE];
        kvlist_append(list, kvpair_clone(pair));
    }
    for (i = 0; i < HASH_TABLE_SIZE; i++) 
    {
        kvlist_t* list = hash_table[i];
        kvlist_sort(list);
        char* current_key = NULL;
        kvlist_t* current_list = NULL;
        kvlist_iterator_t* itor = kvlist_iterator_new(list);
        while ((pair = kvlist_iterator_next(itor)) != NULL) 
        {
            if (current_key == NULL || strcmp(pair->key, current_key) != 0) 
            {
                // Start a new sublist for the new key.
                if (current_list != NULL) 
                {
                    // Call reducer function for the previous key.
                    reducer(current_key, current_list, output);
                    kvlist_free(&current_list);
                }
                current_key = malloc(strlen(pair->key) + 1);
                if (current_key == NULL) 
                {
                    // handle error
                }
                strcpy(current_key, pair->key);

                current_list = kvlist_new();
            }
            kvlist_append(current_list, kvpair_clone(pair));
        }
        if (current_list != NULL) 
        {
            // Call reducer function for the last key.
            reducer(current_key, current_list, output);
            kvlist_free(&current_list);
        }
        kvlist_iterator_free(&itor);
        free(current_key);
        
    }
    for (i = 0; i < HASH_TABLE_SIZE; i++) 
    {
        kvlist_free(&hash_table[i]);
    }
    free(hash_table);
    kvlist_iterator_free(&itor);
    pthread_exit(NULL);
}

//Thread function for map phase
void* map_thread(void* arg) 
{
    map_thread_args* args = (map_thread_args*) arg;
    mapper_t mapper = args->mapper;
    kvlist_t* input = args->input;
    kvlist_t* output = args->output;
    kvlist_iterator_t* itor = kvlist_iterator_new(input);
    kvpair_t* pair;
    while ((pair = kvlist_iterator_next(itor)) != NULL) 
    {
        mapper(pair,output);
    }
    kvlist_iterator_free(&itor);
    pthread_exit(NULL);
}
void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
        //First we want to iterate over input and split the list accordingly
        //malloc mapper input lists
        //split phase
        kvlist_iterator_t* itor = kvlist_iterator_new(input);
        int length = 0;

        while(true)
        {
            kvpair_t* pair = kvlist_iterator_next(itor);
            if(pair == NULL)
            {
            break;
            }
            length++;
        }

        kvlist_t **smaller_lists = malloc(num_mapper * sizeof(kvlist_t *));
        for (unsigned long i = 0; i < num_mapper; i++) 
        {
            smaller_lists[i] = kvlist_new();
        }

        kvlist_iterator_t* itor1 = kvlist_iterator_new(input);
        unsigned long list_number = 0;
        while (true)
        {
            kvpair_t* pair = kvlist_iterator_next(itor1);
            if (pair == NULL)
            {
                break;
            }
            if (list_number == num_mapper) 
            {
                list_number = 0;
            }
            
            kvlist_append(smaller_lists[list_number], kvpair_clone(pair));
            list_number++;
        }
        /*
        for (unsigned long i = 0; i < num_mapper; i++) 
        {
            printf("List %lu:\n", i);
            kvlist_print(STDOUT_FILENO, smaller_lists[i]);
            printf("\n");
        }
        */
        kvlist_iterator_free(&itor);
        kvlist_iterator_free(&itor1);

        //MAP PHASE
        // Create threads
        pthread_t threads[num_mapper];
        map_thread_args* map_phase = malloc(num_mapper * sizeof(map_thread_args));

        for (unsigned long i = 0; i < num_mapper; i++) 
        {
            map_phase[i].mapper = mapper;
            map_phase[i].input = smaller_lists[i];
            map_phase[i].output = kvlist_new();
            pthread_create(&threads[i], NULL, map_thread, &map_phase[i]);
        }

        // Wait for threads to finish
        for (unsigned long i = 0; i < num_mapper; i++) 
        {
            pthread_join(threads[i], NULL);
        }

        
        /*
        for (unsigned long i = 0; i < num_mapper; i++) 
        {
            printf("Output list for thread %lu:\n", i);
            kvlist_print(STDOUT_FILENO, args[i].output);
            printf("\n");
        }
        */

        //SHUFFLE PHASE
        //num_reducer lists
        kvlist_t **reducer_lists = malloc(num_reducer * sizeof(kvlist_t *));
        for (unsigned long i = 0; i < num_reducer; i++) 
        {
            reducer_lists[i] = kvlist_new();
        }

        // Create a hash table to map each key to a reducer index
        unsigned long *key_to_reducer = calloc(HASH_TABLE_SIZE, sizeof(unsigned long));
        for (unsigned long i = 0; i < num_mapper; i++) 
        {
            map_thread_args *tmp = &map_phase[i];
            map_thread_args *args = tmp;
            kvlist_iterator_t *itor = kvlist_iterator_new(args->output);
            kvpair_t *pair;
            while ((pair = kvlist_iterator_next(itor)) != NULL) 
            {
                unsigned long hash_val = hash(pair->key) % HASH_TABLE_SIZE;
                if (key_to_reducer[hash_val] == 0) 
                {
                    // If the key has not been mapped to a reducer yet, map it to the current reducer
                    key_to_reducer[hash_val] = hash_val % num_reducer;
                }
                // Append the pair to the reducer assigned to the key
                kvlist_append(reducer_lists[key_to_reducer[hash_val]], kvpair_clone(pair));
            }
            kvlist_iterator_free(&itor);
        }

        // Free the key to reducer hash table
        free(key_to_reducer);


        for (size_t i = 0; i < num_reducer; i++) 
        {
            kvlist_sort(reducer_lists[i]);
        }

        /*
        for (size_t i = 0; i < num_reducer; i++) 
        {
            printf("Reducer List %zu:\n", i);
            kvlist_print(STDOUT_FILENO, reducer_lists[i]);
            printf("\n");
        }
        */

        //REDUCE PHASE
        pthread_t reduce_threads[num_reducer];
        reducer_thread_args* reduce_phase = malloc(num_reducer * sizeof(reducer_thread_args));

        for (unsigned long i = 0; i < num_reducer; i++) 
        {
            reduce_phase[i].reducer = reducer;
            reduce_phase[i].input = reducer_lists[i];
            reduce_phase[i].output = kvlist_new();
            pthread_create(&reduce_threads[i], NULL, reducer_thread, &reduce_phase[i]);
        }


        // Wait for threads to finish
        for (unsigned long i = 0; i < num_reducer; i++) 
        {
            pthread_join(reduce_threads[i], NULL);
        }

       

        //free(reduce_phase);

        /*

        // Print the simplified list
        for (unsigned long i = 0; i < num_reducer; i++) 
        {
            kvlist_t* output_list = reduce_phase[i].output;
            kvlist_iterator_t* itor = kvlist_iterator_new(output_list);
            kvpair_t* pair;
            printf("Thread %lu", i);
            printf("\n");
            while ((pair = kvlist_iterator_next(itor)) != NULL) 
            {
                printf("key=%s, value=%s\n", pair->key, pair->value);
            }
            kvlist_iterator_free(&itor);
        }
        */

       // printf("\n");

        //OUTPUT PHASE
        output = kvlist_new();

        // Append all output lists to `output`
        for (unsigned long i = 0; i < num_reducer; i++) 
        {
            kvlist_t* output_list = reduce_phase[i].output;
            kvlist_extend(output, output_list);
        }

        

        // Print the final list
        kvlist_sort(output); // Optional: sort the list by keys
        kvlist_print(STDOUT_FILENO, output); // Print the list to stdout
        kvlist_free(&output); // Free the memory used by the list and its pairs


        /*
        for (unsigned long i = 0; i < num_mapper; i++) {
            kvlist_free(&map_phase[i].output);
        }
        */
        free(map_phase);
        
        
        for (unsigned long i = 0; i < num_reducer; i++) {
            free(reducer_lists[i]);
        }
        free(reducer_lists);

        
        for (unsigned long i = 0; i < num_mapper; i++) {
            free(smaller_lists[i]);
        }
        free(smaller_lists);


        for (unsigned long i = 0; i < num_reducer; i++) {
            kvlist_free(&reduce_phase[i].output);
        }

        free(reduce_phase);
        
        

    
}
