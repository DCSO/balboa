/*
   balboa
   Copyright (c) 2018, DCSO GmbH
*/

#ifndef OBS_ROCKSDB_H
#define OBS_ROCKSDB_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

typedef struct Error Error;

Error*      error_new();
const char* error_get(Error*);
bool        error_is_set(Error*);
void        error_delete(Error*);

typedef struct {
  char *key,
       *inv_key;
  uint32_t count,
           last_seen,
           first_seen;
} Observation;

typedef struct ObsSet ObsSet;

unsigned long      obs_set_size(ObsSet*);
const Observation* obs_set_get(ObsSet*, unsigned long);
void               obs_set_delete(ObsSet*);

typedef struct ObsDB ObsDB;

ObsDB*        obsdb_open(const char *path, uint64_t membudget, Error*);
int           obsdb_put(ObsDB *db, Observation *obs, Error*);
ObsSet*       obsdb_search(ObsDB *db, const char *qrdata, const char *qrrname, 
                         const char *qrrtype, const char *qsensorID);
unsigned long obsdb_num_keys(ObsDB*);
void          obsdb_close(ObsDB*);

#endif