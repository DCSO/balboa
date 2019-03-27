// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#ifndef __OBS_ROCKSDB_H
#define __OBS_ROCKSDB_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include <rocksdb/c.h>

typedef struct Error Error;

Error* error_new();
const char* error_get( Error* );
bool error_is_set( Error* );
void error_delete( Error* );

typedef struct{
  char *key,*inv_key;
  uint32_t count,last_seen,first_seen;
}Observation;

static inline Observation obsdb_obs_init( ){
    return(
        (Observation){
            .key=NULL
           ,.inv_key=NULL
           ,.count=0
           ,.first_seen=UINT32_MAX
           ,.last_seen=0
        }
    );
}

typedef struct ObsSet ObsSet;

unsigned long obs_set_size( ObsSet* );
const Observation* obs_set_get( ObsSet*,unsigned long );
void obs_set_delete( ObsSet* );

typedef struct ObsDB ObsDB;

ObsDB* obsdb_open( const char* path,size_t membudget,Error* );
ObsDB* obsdb_open_readonly( const char *path,Error* );
int obsdb_put( ObsDB *db,Observation *obs,Error* );
ObsSet* obsdb_search( ObsDB* db,const char* qrdata,const char* qrrname,const char* qrrtype,const char* qsensorID,int limit );
int obsdb_dump( ObsDB* db,Error* e );

unsigned long obsdb_num_keys( ObsDB* );
void obsdb_close( ObsDB* );

#define obsdb_max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define obsdb_min(a,b) \
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

static inline int obsdb_encode_obs( const Observation* o,char* buf,size_t buflen ){
    size_t minlen=sizeof(uint32_t)*3;
    if( buflen<minlen ){
        //cgoLogInfo("serialize observation: buffer too short");
        return(-1);
    }

    unsigned char* p=(unsigned char*)buf;
    _write_u32_le(p+0,o->count);
    _write_u32_le(p+4,o->last_seen);
    _write_u32_le(p+8,o->first_seen);

    return(0);
}

static inline int obsdb_decode_obs( Observation* o,const char* buf,size_t buflen ){
    size_t minlen = sizeof(uint32_t)*3;
    if( buflen<minlen ){
        //cgoLogInfo("deserialize observation: buffer too short");
        return(-1);
    };

    const unsigned char* p=(const unsigned char*)buf;
    o->count = _read_u32_le(p+0);
    o->last_seen = _read_u32_le(p+4);
    o->first_seen = _read_u32_le(p+8);

    return(0);
}

static inline void obsdb_merge_obs( Observation* lhs,const Observation* rhs ){
    lhs->count+=rhs->count;
    lhs->last_seen=obsdb_max(lhs->last_seen,rhs->last_seen);
    lhs->first_seen=obsdb_min(lhs->first_seen,rhs->first_seen);
}

static char* obsdb_merge_fully(
        void *state
       ,const char* key,size_t key_length
       ,Observation* obs
       ,const char* const* operands_list,const size_t* operands_list_length
       ,int num_operands
       ,unsigned char* success
       ,size_t* new_value_length
){
    (void)state;
    if( key_length<1 ){
        //cgoLogInfo("obsdb merge: key too short");
        *success=(unsigned char)0;
        return(NULL);
    }
    if( key[0]=='i' ){
        /* this is an inverted index key with no meaningful value */
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
            Observation nobs={NULL,NULL,0,0,0};
            obsdb_decode_obs(&nobs,operands_list[i],operands_list_length[i]);
            obsdb_merge_obs(obs,&nobs);
        }
        obsdb_encode_obs(obs,buf,buf_length);
        *new_value_length=buf_length;
        *success=(unsigned char) 1;
        return(buf);
    }else{
        /* weird key format! */
        //cgoLogInfo("obsdb merge: unknown key format");
        *success=(unsigned char)0;
        return(NULL);
    }
}

static char* obsdb_mergeop_full_merge(
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
        //cgoLogInfo("obsdb full merge callback: key too short");
        *success=(unsigned char)0;
        return(NULL);
    }
    Observation obs=obsdb_obs_init();
    if( key[0]=='o' && existing_value!=NULL ){
        obsdb_decode_obs(&obs,existing_value,existing_value_length);
    }
    char* result=obsdb_merge_fully(
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

static char* obsdb_mergeop_partial_merge(
        void* state
       ,const char* key,size_t key_length
       ,const char* const* operands_list,const size_t* operands_list_length
       ,int num_operands
       ,unsigned char* success
       ,size_t* new_value_length
){
    if( key_length<1 ){
        //cgoLogInfo("obsdb partial merge callback: key too short");
        *success=(unsigned char)0;
        return(NULL);
    }
    Observation obs=obsdb_obs_init();
    char* result=obsdb_merge_fully(
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

static void obsdb_mergeop_destructor( void* state ){
    (void)state;
}

static const char* obsdb_mergeop_name( void* state ){
    (void)state;
    return("observation mergeop");
}

static inline rocksdb_mergeoperator_t* obsdb_mergeoperator_create( ){
    return(
        rocksdb_mergeoperator_create(
            NULL
           ,obsdb_mergeop_destructor
           ,obsdb_mergeop_full_merge
           ,obsdb_mergeop_partial_merge
           ,NULL
           ,obsdb_mergeop_name
        )
    );
}

#endif