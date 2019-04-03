// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <mock.h>

typedef struct blb_mock_t blb_mock_t;

static void blb_mock_teardown( db_t* _db );
static db_t* blb_mock_thread_init( thread_t* th,db_t* db );
static void blb_mock_thread_deinit( thread_t* th,db_t* db );
static int blb_mock_query( thread_t* th,const protocol_query_request_t* q );
static int blb_mock_input( thread_t* th,const protocol_input_request_t* i );
static void blb_mock_dump( thread_t* th,const protocol_dump_request_t* d );
static void blb_mock_backup( thread_t* th,const protocol_backup_request_t* b );

static const dbi_t blb_mock_dbi={
    .thread_init=blb_mock_thread_init
   ,.thread_deinit=blb_mock_thread_deinit
   ,.teardown=blb_mock_teardown
   ,.query=blb_mock_query
   ,.input=blb_mock_input
   ,.backup=blb_mock_backup
   ,.dump=blb_mock_dump
};

struct blb_mock_t{
    const dbi_t* dbi;
};

db_t* blb_mock_thread_init( thread_t* th,db_t* db ){
    (void)th;
    return(db);
}

void blb_mock_thread_deinit( thread_t* th,db_t* db ){
    (void)th;
    (void)db;
}

void blb_mock_teardown( db_t* _db ){
    ASSERT( _db->dbi==&blb_mock_dbi );
    blb_free(_db);
}

static int blb_mock_query( thread_t* th,const protocol_query_request_t* q ){
    (void)q;

    int start_ok=blb_thread_query_stream_start_response(th);
    if( start_ok!=0 ){
        V(prnl("unable to start query stream response"));
        return(-1);
    }

    protocol_entry_t __e,*e=&__e;
    e->sensorid="test-sensor-id";
    e->sensorid_len=strlen(e->sensorid);
    e->rdata="";
    e->rdata_len=0;
    e->rrname="test-rrname";
    e->rrname_len=strlen(e->rrname);
    e->rrtype="A";
    e->rrtype_len=1;
    e->count=23;
    e->first_seen=15000000;
    e->last_seen=15001000;
    int push_ok=blb_thread_query_stream_push_response(th,e);
    if( push_ok!=0 ){
        X(prnl("unable to push query response entry"));
        return(-1);
    }

    (void)blb_thread_query_stream_end_response(th);

    return(0);
}

static int blb_mock_input( thread_t* th,const protocol_input_request_t* i ){
    ASSERT( th->db->dbi==&blb_mock_dbi );
    //blb_mock_t* db=(blb_mock_t*)th->db;

    X(
        prnl("put `%.*s` `%.*s` `%.*s` `%.*s` %d"
            ,(int)i->entry.rdata_len,i->entry.rdata
            ,(int)i->entry.rrname_len,i->entry.rrname
            ,(int)i->entry.rrtype_len,i->entry.rrtype
            ,(int)i->entry.sensorid_len,i->entry.sensorid
            ,i->entry.count
        )
    );

    return(0);
}

static void blb_mock_backup( thread_t* th,const protocol_backup_request_t* b ){
    ASSERT( th->db->dbi==&blb_mock_dbi );
    //blb_mock_t* db=(blb_mock_t*)th->db;

    X(
        prnl("backup `%.*s`",(int)b->path_len,b->path)
    );
}

static void blb_mock_dump( thread_t* th,const protocol_dump_request_t* d ){
    ASSERT( th->db->dbi==&blb_mock_dbi );
    //blb_mock_t* db=(blb_mock_t*)th->db;

    X(
        prnl("dump `%.*s`",(int)d->path_len,d->path)
    );
}

db_t* blb_mock_open( ){
    blb_mock_t* db=blb_new(blb_mock_t);
    if( db==NULL ){ return(NULL); }
    db->dbi=&blb_mock_dbi;

    return((db_t*)db);
}
