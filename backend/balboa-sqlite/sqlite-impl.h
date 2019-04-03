// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#ifndef __sqlite_H
#define __sqlite_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include <engine.h>

typedef struct blb_sqlite_config_t blb_sqlite_config_t;
struct blb_sqlite_config_t{
    const char* path;
    const char* name;
    const char* compression;
    const char* journal_mode;
};

static inline blb_sqlite_config_t blb_sqlite_config_init( void ){
    return(
        (blb_sqlite_config_t){
            .path="/tmp/balboa-sqlite.db"
           ,.name="pdns"
           ,.compression="none"
           ,.journal_mode="wal"
        }
    );
}

db_t* blb_sqlite_open( const blb_sqlite_config_t* config );

#endif
