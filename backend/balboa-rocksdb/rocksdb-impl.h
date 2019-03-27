// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#ifndef __ROCKSDB_H
#define __ROCKSDB_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include <engine.h>

typedef struct blb_rocksdb_t blb_rocksdb_t;
typedef struct blb_rocksdb_config_t blb_rocksdb_config_t;
struct blb_rocksdb_config_t{
    size_t membudget;
    int parallelism;
    size_t max_log_file_size;
    int max_open_files;
    int keep_log_file_num;
    const char* path;
};

static inline blb_rocksdb_config_t blb_rocksdb_config_init( ){
    return(
        (blb_rocksdb_config_t){
            .membudget=128*1024*1024
           ,.parallelism=8
           ,.max_log_file_size=10*1024*1024
           ,.max_open_files=300
           ,.keep_log_file_num=2
           ,.path="/tmp/balboa-rocksdb"
        }
    );
}

db_t* blb_rocksdb_open( blb_rocksdb_config_t* config );

#endif
