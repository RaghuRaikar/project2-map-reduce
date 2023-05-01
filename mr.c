#include "mr.h"

#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "hash.h"
#include "kvlist.h"
#define UNUSED(x) (void)(x)
#define HASH_SIZE 1200
#define KEY_LENGTH 275

// Structs for map and reduce phase
typedef struct {
  mapper_t mapper;

  kvlist_t* input;

  kvlist_t* output;

} map_struct;

typedef struct {
  reducer_t reducer;

  kvlist_t* input;

  kvlist_t* output;

} reducer_struct;

// Thread Function for reduce phase

void* reducer_thread(void* arg) {
  // Making sure to make arg the correct type
  reducer_struct* temp_reducer_struct = (reducer_struct*)arg;

  // getting values from arg and assigning them
  reducer_t reducer = temp_reducer_struct->reducer;

  kvlist_t* input = temp_reducer_struct->input;

  kvlist_t* output = temp_reducer_struct->output;

  // Setting up iterator
  kvlist_iterator_t* itor = kvlist_iterator_new(input);

  kvpair_t* pair;

  // hash table so we can identify pairs by key
  kvlist_t** hash_table = calloc(HASH_SIZE, sizeof(kvlist_t*));

  // here we initialize each entry with a new empty list
  for (unsigned long i = 0; i < HASH_SIZE; i++) {
    hash_table[i] = kvlist_new();
  }

  // looping through all pairs in the input list
  while ((pair = kvlist_iterator_next(itor)) != NULL) {
    // Use hash function to get hash value
    unsigned long hash_value = hash(pair->key);

    // get hash table entry for the key we are currenly on
    kvlist_t* list = hash_table[hash_value % HASH_SIZE];

    // append copy of pair to hash table entry
    kvlist_append(list, kvpair_clone(pair));
  }
  // loop through all entries in hash table
  for (unsigned long i = 0; i < HASH_SIZE; i++) {
    // get and sort each hash table entry
    kvlist_t* list = hash_table[i];

    kvlist_sort(list);

    // setting up variables for our current key and current list to pass to
    // reducer

    char current_key[KEY_LENGTH];

    kvlist_t* current_list = NULL;

    kvlist_iterator_t* itor = kvlist_iterator_new(list);
    // looping through pairs in the sorted list
    while ((pair = kvlist_iterator_next(itor)) != NULL) {
      // see if current key is different from the key we are on
      if (current_list == NULL || strcmp(pair->key, current_key) != 0) {
        // new sublist for the new key.

        if (current_list != NULL) {
          // Call reducer function for the previous key.

          reducer(current_key, current_list, output);
          // free memory
          kvlist_free(&current_list);
        }
        // reset for new key and value list
        strncpy(current_key, pair->key, KEY_LENGTH);
        current_key[KEY_LENGTH - 1] = '\0';

        current_list = kvlist_new();
      }
      // append pair to current_list
      kvlist_append(current_list, kvpair_clone(pair));
    }

    if (current_list != NULL) {
      // Call reducer function for the last key.

      reducer(current_key, current_list, output);

      kvlist_free(&current_list);
    }

    kvlist_iterator_free(&itor);
  }
  // free memory for the lists in hash table
  for (unsigned long i = 0; i < HASH_SIZE; i++) {
    kvlist_free(&hash_table[i]);
  }

  free(hash_table);

  kvlist_iterator_free(&itor);
  // exiting the thread
  pthread_exit(NULL);
}

// Thread function for map phase

void* map_thread(void* arg) {
  // Making sure to make arg the correct type
  map_struct* temp_mapper_struct = (map_struct*)arg;

  // getting values from arg and assigning them

  mapper_t mapper = temp_mapper_struct->mapper;

  kvlist_t* input = temp_mapper_struct->input;

  kvlist_t* output = temp_mapper_struct->output;
  // creating iterator to iterate over input

  kvlist_iterator_t* itor = kvlist_iterator_new(input);

  kvpair_t* pair;
  // loop through the whole list and call mapper on each pair in the list
  while ((pair = kvlist_iterator_next(itor)) != NULL) {
    mapper(pair, output);
  }
  // free memory
  kvlist_iterator_free(&itor);
  // exit the thread
  pthread_exit(NULL);
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,

                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  // First we want to iterate over input and split the list accordingly

  // malloc mapper input lists

  // SPLIT PHASE

  kvlist_iterator_t* itor = kvlist_iterator_new(input);

  int length = 0;
  // looping over to calculate the length of the list
  while (true) {
    kvpair_t* pair = kvlist_iterator_next(itor);

    if (pair == NULL) {
      break;
    }

    length++;
  }

  kvlist_t** smaller_lists = malloc(num_mapper * sizeof(kvlist_t*));

  for (unsigned long i = 0; i < num_mapper; i++) {
    smaller_lists[i] = kvlist_new();
  }

  kvlist_iterator_t* itor1 = kvlist_iterator_new(input);

  unsigned long list_number = 0;
  // round robin approach to split into smaller lists
  while (true) {
    kvpair_t* pair = kvlist_iterator_next(itor1);

    if (pair == NULL) {
      break;
    }

    if (list_number == num_mapper) {
      list_number = 0;
    }

    kvlist_append(smaller_lists[list_number], kvpair_clone(pair));

    list_number++;
  }

  kvlist_iterator_free(&itor);

  kvlist_iterator_free(&itor1);

  // MAP PHASE

  //  Create map_threads
  // here we make array of threads
  pthread_t map_threads[num_mapper];
  // here we make array of structs so we can access info later
  map_struct* map_phase = malloc(num_mapper * sizeof(map_struct));
  // here we initialize each struct
  for (unsigned long i = 0; i < num_mapper; i++) {
    map_phase[i].mapper = mapper;

    map_phase[i].input = smaller_lists[i];

    map_phase[i].output = kvlist_new();
    // here we create a new thread and pass in the struct as the argument
    pthread_create(&map_threads[i], NULL, map_thread, &map_phase[i]);
  }

  // Here we wait for map_threads to finish

  for (unsigned long i = 0; i < num_mapper; i++) {
    pthread_join(map_threads[i], NULL);
  }

  // SHUFFLE PHASE

  // num_reducer lists
  // create array of kvlist pointers
  kvlist_t** reducer_lists = malloc(num_reducer * sizeof(kvlist_t*));
  // make sure to create new kvlist for each reducer
  for (unsigned long i = 0; i < num_reducer; i++) {
    reducer_lists[i] = kvlist_new();
  }

  // hash table to map each key to a reducer index

  unsigned long* key_hash_table = calloc(HASH_SIZE, sizeof(unsigned long));
  // iterating over each map output
  for (unsigned long i = 0; i < num_mapper; i++) {
    map_struct* tmp = &map_phase[i];

    map_struct* temp_mapper_struct = tmp;

    kvlist_iterator_t* itor = kvlist_iterator_new(temp_mapper_struct->output);

    kvpair_t* pair;
    // loop through each pair in current output list
    while ((pair = kvlist_iterator_next(itor)) != NULL) {
      // compute a hash value and map it to index
      unsigned long hash_value = hash(pair->key) % HASH_SIZE;

      if (key_hash_table[hash_value] == 0) {
        // If the key not been mapped to a reducer yet, it is mapped to the

        // current reducer

        key_hash_table[hash_value] = hash_value % num_reducer;
      }

      // here we append the pair to the reducer assigned to the key

      kvlist_append(reducer_lists[key_hash_table[hash_value]],

                    kvpair_clone(pair));
    }

    kvlist_iterator_free(&itor);
  }

  // Free memory

  free(key_hash_table);
  // sort reducer lists
  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_sort(reducer_lists[i]);
  }

  // REDUCE PHASE
  // create array of threads for reduce phase
  pthread_t reduce_threads[num_reducer];
  // here we make another array of structs
  reducer_struct* reduce_phase =

      malloc(num_reducer * sizeof(reducer_struct));
  // here we initialize each struct
  for (unsigned long i = 0; i < num_reducer; i++) {
    reduce_phase[i].reducer = reducer;

    reduce_phase[i].input = reducer_lists[i];

    reduce_phase[i].output = kvlist_new();
    // create new thread and pass in struct as the argument
    pthread_create(&reduce_threads[i], NULL, reducer_thread, &reduce_phase[i]);
  }

  // here we wait for map_threads to finish

  for (unsigned long i = 0; i < num_reducer; i++) {
    pthread_join(reduce_threads[i], NULL);
  }

  // OUTPUT PHASE

  // Appending all the lists to our final output list

  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_t* output_list = reduce_phase[i].output;

    kvlist_extend(output, output_list);
  }

  // kvlist_sort(output);

  // FREEING MEMORY HERE

  for (unsigned long i = 0; i < num_mapper; i++) {
    kvlist_free(&map_phase[i].output);
  }

  free(map_phase);

  for (unsigned long i = 0; i < num_reducer; i++) {
    kvlist_free(&reducer_lists[i]);
  }

  free(reducer_lists);

  for (unsigned long i = 0; i < num_mapper; i++) {
    kvlist_free(&smaller_lists[i]);
  }

  free(smaller_lists);

  for (unsigned long i = 0; i < num_reducer; i++) {
    kvlist_free(&reduce_phase[i].output);
  }

  free(reduce_phase);
}
