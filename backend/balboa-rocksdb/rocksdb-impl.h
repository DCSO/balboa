// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#ifndef __ROCKSDB_H
#define __ROCKSDB_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include <rocksdb/c.h>

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
rocksdb_t* blb_rocksdb_handle( db_t* db );
rocksdb_readoptions_t* blb_rocksdb_readoptions( db_t* db);

typedef struct value_t value_t;
struct value_t {
    uint32_t count;
    uint32_t first_seen;
    uint32_t last_seen;
};

static inline value_t blb_rocksdb_val_init( ){
    return(
        (value_t){
            .count=0
           ,.first_seen=UINT32_MAX
           ,.last_seen=0
        }
    );
}

#define blb_rocksdb_max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define blb_rocksdb_min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

static inline void _write_u32_le( unsigned char* p, uint32_t v ){
    p[0]=v>>0;
    p[1]=v>>8;
    p[2]=v>>16;
    p[3]=v>>24;
}

static inline uint32_t _read_u32_le( const unsigned char* p ){
    return(
        (((uint32_t)p[0])<<0)
       |(((uint32_t)p[1])<<8)
       |(((uint32_t)p[2])<<16)
       |(((uint32_t)p[3])<<24)
    );
}

static inline int blb_rocksdb_val_encode( const struct value_t* o,char* buf,size_t buflen ){
    size_t minlen=sizeof(uint32_t)*3;
    if( buflen<minlen ){
        return(-1);
    }

    unsigned char* p=(unsigned char*)buf;
    _write_u32_le(p+0,o->count);
    _write_u32_le(p+4,o->last_seen);
    _write_u32_le(p+8,o->first_seen);

    return(0);
}

static inline int blb_rocksdb_val_decode( value_t* o,const char* buf,size_t buflen ){
    size_t minlen = sizeof(uint32_t)*3;
    if( buflen<minlen ){
        return(-1);
    };

    const unsigned char* p=(const unsigned char*)buf;
    o->count = _read_u32_le(p+0);
    o->last_seen = _read_u32_le(p+4);
    o->first_seen = _read_u32_le(p+8);

    return(0);
}

static inline void blb_rocksdb_val_merge( value_t* lhs,const value_t* rhs ){
    lhs->count+=rhs->count;
    lhs->last_seen=blb_rocksdb_max(lhs->last_seen,rhs->last_seen);
    lhs->first_seen=blb_rocksdb_min(lhs->first_seen,rhs->first_seen);
}

static char* blb_rocksdb_merge_fully(
        void *state
       ,const char* key,size_t key_length
       ,value_t* obs
       ,const char* const* operands_list,const size_t* operands_list_length
       ,int num_operands
       ,unsigned char* success
       ,size_t* new_value_length
){
    (void)state;
    if( key_length<1 ){
        L(prnl("impossible: key too short"));
        *success=(unsigned char)0;
        return(NULL);
    }
    if( key[0]=='i' ){
        L(prnl("impossible: got an inverted key during merge"));
        //this is an inverted index key with no meaningful value
        char* res=malloc(sizeof(char)*1);
        if( res==NULL ){
            return(NULL);
        }
        *res='\0';
        *new_value_length=1;
        *success=1;
        return(res);
    }else if( key[0]=='o' ){
        /* this is an observation value */
        size_t buf_length = sizeof(uint32_t)*3;
        char *buf = malloc(buf_length);
        if( buf==NULL ){ return(NULL); }
        for( int i=0;i<num_operands;i++ ){
            value_t nobs={0,0,0};
            blb_rocksdb_val_decode(&nobs,operands_list[i],operands_list_length[i]);
            blb_rocksdb_val_merge(obs,&nobs);
        }
        blb_rocksdb_val_encode(obs,buf,buf_length);
        *new_value_length=buf_length;
        *success=(unsigned char) 1;
        return(buf);
    }else{
        L(prnl("impossbile: unknown key format encountered"));
        *success=(unsigned char)0;
        return(NULL);
    }
}

static char* blb_rocksdb_mergeop_full_merge(
        void* state
       ,const char* key,size_t key_length
       ,const char* existing_value,size_t existing_value_length
       ,const char* const* operands_list,const size_t* operands_list_length
       ,int num_operands
       ,unsigned char* success
       ,size_t* new_value_length
){
    (void)state;
    if( key_length<1 ){
        L(prnl("impossible: key to short"));
        *success=(unsigned char)0;
        return(NULL);
    }
    value_t obs=blb_rocksdb_val_init();
    if( key[0]=='o' && existing_value!=NULL ){
        blb_rocksdb_val_decode(&obs,existing_value,existing_value_length);
    }
    char* result=blb_rocksdb_merge_fully(
        state
       ,key,key_length
       ,&obs
       ,operands_list,operands_list_length
       ,num_operands
       ,success
       ,new_value_length
    );
    return(result);
}

static char* blb_rocksdb_mergeop_partial_merge(
        void* state
       ,const char* key,size_t key_length
       ,const char* const* operands_list,const size_t* operands_list_length
       ,int num_operands
       ,unsigned char* success
       ,size_t* new_value_length
){
    if( key_length<1 ){
        V(prnl("impossible: key too short"));
        *success=(unsigned char)0;
        return(NULL);
    }
    value_t obs=blb_rocksdb_val_init();
    char* result=blb_rocksdb_merge_fully(
        state
       ,key,key_length
       ,&obs
       ,operands_list,operands_list_length
       ,num_operands
       ,success
       ,new_value_length
    );
    return(result);
}

static void blb_rocksdb_mergeop_destructor( void* state ){
    (void)state;
}

static const char* blb_rocksdb_mergeop_name( void* state ){
    (void)state;
    return("observation-mergeop");
}

static inline rocksdb_mergeoperator_t* blb_rocksdb_mergeoperator_create( ){
    return(
        rocksdb_mergeoperator_create(
            NULL
           ,blb_rocksdb_mergeop_destructor
           ,blb_rocksdb_mergeop_full_merge
           ,blb_rocksdb_mergeop_partial_merge
           ,NULL
           ,blb_rocksdb_mergeop_name
        )
    );
}

#endif
