// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <rocksdb.h>

static void blb_rocksdb_teardown( db_t* _db );
static db_t* blb_rocksdb_clone( db_t* db );
static int blb_rocksdb_query( thread_t* th,const query_t* q );
static int blb_rocksdb_input( thread_t* th,const input_t* i );
static void blb_rocksdb_backup( thread_t* th,const backup_t* b );
static void blb_rocksdb_dump( thread_t* th,const dump_t* d );

static const dbi_t blb_rocksdb_dbi={
    .clone=blb_rocksdb_clone
   ,.teardown=blb_rocksdb_teardown
   ,.query=blb_rocksdb_query
   ,.input=blb_rocksdb_input
   ,.backup=blb_rocksdb_backup
   ,.dump=blb_rocksdb_dump
};

struct blb_rocksdb_t{
    const dbi_t* dbi;
    rocksdb_t* db;
    rocksdb_options_t* options;
    rocksdb_writeoptions_t* writeoptions;
    rocksdb_readoptions_t* readoptions;
    rocksdb_mergeoperator_t* mergeop;
};

db_t* blb_rocksdb_clone( db_t* db ){
    return(db);
}

void blb_rocksdb_teardown( db_t* _db ){
    ASSERT( _db->dbi==&blb_rocksdb_dbi );

    blb_rocksdb_t* db=(blb_rocksdb_t*)_db;
    rocksdb_close(db->db);
    rocksdb_mergeoperator_destroy(db->mergeop);
    rocksdb_writeoptions_destroy(db->writeoptions);
    rocksdb_readoptions_destroy(db->readoptions);
    // keeping this causes segfault; rocksdb_close seems to handle dealloc...
    //rocksdb_options_destroy(db->options);
    blb_free(db);
}

static int blb_rocksdb_query_by_o( thread_t* th,const query_t* q ){
    ASSERT( th->db->dbi==&blb_rocksdb_dbi );
    blb_rocksdb_t* db=(blb_rocksdb_t*)th->db;
    size_t prefix_len=0;
    if( q->qsensorid_len>0 ){
        prefix_len=q->qsensorid_len+q->qrrname_len+4;
        (void)snprintf(th->scrtch_key,ENGINE_THREAD_SCRTCH_SZ
            ,"o\x1f%.*s\x1f%.*s\x1f"
            ,(int)q->qrrname_len,q->qrrname
            ,(int)q->qsensorid_len,q->qsensorid
        );
    }else{
        prefix_len=q->qrrname_len+3;
        (void)snprintf(th->scrtch_key,ENGINE_THREAD_SCRTCH_SZ
            ,"o\x1f%.*s\x1f"
            ,(int)q->qrrname_len,q->qrrname
        );
    }

    X(prnl("prefix key `%.*s`",(int)prefix_len,th->scrtch_key));

    int start_ok=blb_thread_query_stream_start_response(th);
    if( start_ok!=0 ){
        V(prnl("unable to start query stream response"));
        return(-1);
    }

    rocksdb_iterator_t* it=rocksdb_create_iterator(db->db,db->readoptions);
    rocksdb_iter_seek(it,th->scrtch_key,prefix_len);
    size_t keys_visited=0;
    size_t keys_hit=0;
    for(
        ;rocksdb_iter_valid(it)!=(unsigned char)0 && keys_hit<(size_t)q->limit
        ;rocksdb_iter_next(it)
    ){
        keys_visited+=1;
        size_t key_len=0;
        const char* key=rocksdb_iter_key(it,&key_len);
        if ( key==NULL ) {
            V(prnl("impossible: unable to extract key from rocksdb iterator"));
            goto stream_error;
        }

        enum TokIdx{
            RRNAME=0
           ,SENSORID=1
           ,RRTYPE=2
           ,RDATA=3
           ,FIELDS=4
        };

        struct Tok{
            const char* tok;
            int tok_len;
        };

        struct Tok toks[FIELDS]={NULL};

        enum TokIdx j=RRNAME;
        size_t last=1;
        for( size_t i=2;i<key_len;i++ ){
            if( key[i]=='\x1f' ){
                //we fixup the RDATA and skip extra \x1f's
                if( j<RDATA ){
                    toks[j].tok=&key[last+1];
                    toks[j].tok_len=i-last-1;
                    last=i;
                    j++;
                }
            }
        }
        toks[RDATA].tok=&key[last+1];
        toks[RDATA].tok_len=key_len-last-1;

        X(
            out("o %.*s %.*s %.*s %.*s\n"
               ,toks[RRNAME].tok_len,toks[RRNAME].tok
               ,toks[SENSORID].tok_len,toks[SENSORID].tok
               ,toks[RRTYPE].tok_len,toks[RRTYPE].tok
               ,toks[RDATA].tok_len,toks[RDATA].tok
            )
        );

        size_t qrrname_len=q->qrrname_len;
        if( toks[RRNAME].tok_len<=0
          ||memcmp(toks[RRNAME].tok,q->qrrname,blb_rocksdb_min((size_t)toks[RRNAME].tok_len,qrrname_len))!=0 ){
            break;
        }
        if( (size_t)toks[RRNAME].tok_len!=qrrname_len ){
            continue;
        }

        if( toks[SENSORID].tok_len==0
          ||( q->qsensorid_len>0
            &&(size_t)toks[SENSORID].tok_len!=q->qsensorid_len )
          ||( q->qsensorid_len>0
            &&memcmp(toks[SENSORID].tok,q->qsensorid,toks[SENSORID].tok_len)!=0 ) ){
            continue;
        }

        if( toks[RDATA].tok_len==0
          ||( q->qrdata_len>0
            &&(size_t)toks[RDATA].tok_len!=q->qrdata_len )
          ||( q->qrdata_len>0
            &&memcmp(toks[RDATA].tok,q->qrdata,toks[RDATA].tok_len)!=0 ) ){
            continue;
        }

        if( toks[RRTYPE].tok_len==0
          ||( q->qrrtype_len>0
            &&(size_t)toks[RRTYPE].tok_len!=q->qrrtype_len)
          ||( q->qrrtype_len>0
            &&memcmp(toks[RRTYPE].tok,q->qrrtype,toks[RRTYPE].tok_len)!=0 ) ){
            continue;
        }

        size_t val_size=0;
        value_t v;
        const char* val=rocksdb_iter_value(it,&val_size);
        int ret=blb_rocksdb_val_decode(&v,val,val_size);
        if( ret!=0 ){
            X(prnl("unable to decode observation value; skipping entry"));
            continue;
        }

        keys_hit+=1;
        entry_t __e,*e=&__e;
        e->sensorid=toks[SENSORID].tok;
        e->sensorid_len=toks[SENSORID].tok_len;
        e->rdata=toks[RDATA].tok;
        e->rdata_len=toks[RDATA].tok_len;
        e->rrname=toks[RRNAME].tok;
        e->rrname_len=toks[RRNAME].tok_len;
        e->rrtype=toks[RRTYPE].tok;
        e->rrtype_len=toks[RRTYPE].tok_len;
        e->count=v.count;
        e->first_seen=v.first_seen;
        e->last_seen=v.last_seen;
        int push_ok=blb_thread_query_stream_push_response(th,e);
        if( push_ok!=0 ){
            X(prnl("unable to push query response entry"));
            goto stream_error;
        }
    }
    rocksdb_iter_destroy(it);
    (void)blb_thread_query_stream_end_response(th);
    return(0);

stream_error:
    rocksdb_iter_destroy(it);
    return(-1);
}

static int blb_rocksdb_query_by_i( thread_t* th,const query_t* q ){
    ASSERT( th->db->dbi==&blb_rocksdb_dbi );
    blb_rocksdb_t* db=(blb_rocksdb_t*)th->db;
    size_t prefix_len=0;
    if( q->qsensorid_len>0 ){
        prefix_len=q->qrdata_len+q->qsensorid_len+4;
        (void)snprintf(th->scrtch_inv,ENGINE_THREAD_SCRTCH_SZ,
                "i\x1f%.*s\x1f%.*s\x1f"
               ,(int)q->qrdata_len,q->qrdata
               ,(int)q->qsensorid_len,q->qsensorid
        );
    }else{
        prefix_len=q->qrdata_len+3;
        (void)snprintf(th->scrtch_inv,ENGINE_THREAD_SCRTCH_SZ,
                "i\x1f%.*s\x1f"
                ,(int)q->qrdata_len,q->qrdata
        );
    }
    ASSERT( th->scrtch_inv[prefix_len]=='\0' );

    X(prnl("prefix key `%.*s`",(int)prefix_len,th->scrtch_inv));

    int start_ok=blb_thread_query_stream_start_response(th);
    if( start_ok!=0 ){
        V(prnl("unable to start query stream response"));
        return(-1);
    }

    rocksdb_iterator_t* it=rocksdb_create_iterator(db->db,db->readoptions);
    rocksdb_iter_seek(it,th->scrtch_inv,prefix_len);
    size_t keys_visited=0;
    int keys_hit=0;
    for( ;rocksdb_iter_valid(it)!=(unsigned char)0 && keys_hit<q->limit;rocksdb_iter_next(it) ){
        keys_visited+=1;
        size_t key_len=0;
        const char* key=rocksdb_iter_key(it,&key_len);
        char* err=NULL;

        enum TokIdx{
            RDATA=3
           ,SENSORID=2
           ,RRNAME=1
           ,RRTYPE=0
           ,FIELDS=4
        };

        struct Tok{
            const char* tok;
            int tok_len;
        };

        struct Tok toks[FIELDS]={NULL};

        enum TokIdx j=RRTYPE;
        size_t last=key_len;
        for( ssize_t i=key_len-1;i>0;i-- ){
            if( key[i]=='\x1f' ){
                if( j<FIELDS ){
                    toks[j].tok=&key[i+1];
                    toks[j].tok_len=last-i-1;
                    last=i;
                    j++;
                }
            }
        }
        toks[RDATA].tok=key+2;
        toks[RDATA].tok_len=toks[RDATA].tok_len+last-1;

        X(
            out("i %.*s | %.*s %.*s %.*s\n"
               ,toks[RDATA].tok_len,toks[RDATA].tok
               ,toks[SENSORID].tok_len,toks[SENSORID].tok
               ,toks[RRTYPE].tok_len,toks[RRTYPE].tok
               ,toks[RRNAME].tok_len,toks[RRNAME].tok
            )
        );

        size_t qrdata_len=q->qrdata_len;
        if( toks[RDATA].tok_len<=0
          ||memcmp(toks[RDATA].tok,q->qrdata,blb_rocksdb_min((size_t)toks[RDATA].tok_len,qrdata_len))!=0 ){
            break;
        }
        if( (size_t)toks[RDATA].tok_len!=qrdata_len ){
            continue;
        }

        if( toks[SENSORID].tok_len==0
          ||( q->qsensorid_len>0
            &&(size_t)toks[SENSORID].tok_len!=q->qsensorid_len )
          ||( q->qsensorid_len>0
            &&memcmp(toks[SENSORID].tok,q->qsensorid,toks[SENSORID].tok_len)!=0 ) ){
            continue;
        }

        if( toks[RRTYPE].tok_len==0
          ||( q->qrrtype_len>0
            &&(size_t)toks[RRTYPE].tok_len!=q->qrrtype_len )
          ||( q->qrrtype_len>0
            &&memcmp(toks[RRTYPE].tok,q->qrrtype,toks[RRTYPE].tok_len)!=0 ) ){
            continue;
        }

        memset(th->scrtch_key,'\0',ENGINE_THREAD_SCRTCH_SZ);
        (void)snprintf(th->scrtch_key,ENGINE_THREAD_SCRTCH_SZ
               ,"o\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s"
               ,toks[RRNAME].tok_len,toks[RRNAME].tok
               ,toks[SENSORID].tok_len,toks[SENSORID].tok
               ,toks[RRTYPE].tok_len,toks[RRTYPE].tok
               ,toks[RDATA].tok_len,toks[RDATA].tok
        );

        X(prnl("full key `%s`",th->scrtch_key));

        size_t fullkey_len=strlen(th->scrtch_key);
        size_t val_size=0;
        char* val=rocksdb_get(db->db,db->readoptions,th->scrtch_key,fullkey_len,&val_size,&err);
        if( val==NULL || err!=NULL ){
            X(prnl("rocksdb_get() observation not found"));
            continue;
        }

        value_t v;
        int ret=blb_rocksdb_val_decode(&v,val,val_size);
        if( ret!=0 ){
            X(prnl("unable to decode observation value; skipping entry"));
            free(val);
            continue;
        }
        free(val);

        keys_hit+=1;
        entry_t __e,*e=&__e;
        e->sensorid=toks[SENSORID].tok;
        e->sensorid_len=toks[SENSORID].tok_len;
        e->rdata=toks[RDATA].tok;
        e->rdata_len=toks[RDATA].tok_len;
        e->rrname=toks[RRNAME].tok;
        e->rrname_len=toks[RRNAME].tok_len;
        e->rrtype=toks[RRTYPE].tok;
        e->rrtype_len=toks[RRTYPE].tok_len;
        e->count=v.count;
        e->first_seen=v.first_seen;
        e->last_seen=v.last_seen;
        int push_ok=blb_thread_query_stream_push_response(th,e);
        if( push_ok!=0 ){
            X(prnl("unable to push query response entry"));
            goto stream_error;
        }
    }
    rocksdb_iter_destroy(it);
    (void)blb_thread_query_stream_end_response(th);
    return(0);

stream_error:
    rocksdb_iter_destroy(it);
    return(-1);
}

static int blb_rocksdb_query( thread_t* th,const query_t* q ){
    int rc=-1;
    if( q->qrrname_len>0 ){
        rc=blb_rocksdb_query_by_o(th,q);
    }else{
        rc=blb_rocksdb_query_by_i(th,q);
    }
    return(rc);
}

static void blb_rocksdb_backup( thread_t* th,const backup_t* b ){
    ASSERT( th->db->dbi==&blb_rocksdb_dbi );
    blb_rocksdb_t* db=(blb_rocksdb_t*)th->db;

    X(
        prnl("backup `%.*s`",(int)b->path_len,b->path)
    );

    if( b->path_len>=256 ){
        L(prnl("invalid path"));
        return;
    }

    char path[256];
    snprintf(path,sizeof(path),"%.*s",(int)b->path_len,b->path);

    char* err=NULL;
    rocksdb_backup_engine_t* be=rocksdb_backup_engine_open(db->options,path,&err);
    if( err!=NULL ){
        L(prnl("rocksdb_backup_engine_open() failed `%s`",err));
        free(err);
        return;
    }

    rocksdb_backup_engine_create_new_backup(be,db->db,&err);
    if( err!=NULL ){
        L(prnl("rocksdb_backup_engine_create_new_backup() failed `%s`",err));
        free(err);
        rocksdb_backup_engine_close(be);
        return;
    }
}

static void blb_rocksdb_dump( thread_t* th,const dump_t* d ){
    ASSERT( th->db->dbi==&blb_rocksdb_dbi );
    blb_rocksdb_t* db=(blb_rocksdb_t*)th->db;

    X(
        prnl("dump `%.*s`",(int)d->path_len,d->path)
    );

    uint64_t cnt=0;
    rocksdb_iterator_t* it=rocksdb_create_iterator(db->db,db->readoptions);
    // rocksdb_iter_seek_to_first(it);
    rocksdb_iter_seek(it,"o",1);
    for(
        ;rocksdb_iter_valid(it)!=(unsigned char)0
        ;rocksdb_iter_next(it) ){
        size_t key_len=0;
        const char* key=rocksdb_iter_key(it,&key_len);
        if ( key==NULL ) {
            L(prnl("impossible: unable to extract key from rocksdb iterator"));
            break;
        }

        if( key[0]=='i' ){ continue; }

        enum TokIdx{
            RRNAME=0
           ,SENSORID=1
           ,RRTYPE=2
           ,RDATA=3
           ,FIELDS=4
        };

        struct Tok{
            const char* tok;
            int tok_len;
        };

        struct Tok toks[FIELDS]={NULL};

        enum TokIdx j=RRNAME;
        size_t last=1;
        for( size_t i=2;i<key_len;i++ ){
            if( key[i]=='\x1f' ){
                //we fixup the RDATA and skip extra \x1f's
                if( j<RDATA ){
                    toks[j].tok=&key[last+1];
                    toks[j].tok_len=i-last-1;
                    last=i;
                    j++;
                }
            }
        }
        toks[RDATA].tok=&key[last+1];
        toks[RDATA].tok_len=key_len-last-1;

        X(
            out("o %.*s %.*s %.*s %.*s\n"
               ,toks[RRNAME].tok_len,toks[RRNAME].tok
               ,toks[SENSORID].tok_len,toks[SENSORID].tok
               ,toks[RRTYPE].tok_len,toks[RRTYPE].tok
               ,toks[RDATA].tok_len,toks[RDATA].tok
            )
        );

        size_t val_size=0;
        value_t v;
        const char* val=rocksdb_iter_value(it,&val_size);
        int ret=blb_rocksdb_val_decode(&v,val,val_size);
        if( ret!=0 ){
            X(prnl("unable to decode observation value; skipping entry"));
            continue;
        }

        cnt+=1;
        entry_t __e,*e=&__e;
        e->sensorid=toks[SENSORID].tok;
        e->sensorid_len=toks[SENSORID].tok_len;
        e->rdata=toks[RDATA].tok;
        e->rdata_len=toks[RDATA].tok_len;
        e->rrname=toks[RRNAME].tok;
        e->rrname_len=toks[RRNAME].tok_len;
        e->rrtype=toks[RRTYPE].tok;
        e->rrtype_len=toks[RRTYPE].tok_len;
        e->count=v.count;
        e->first_seen=v.first_seen;
        e->last_seen=v.last_seen;

        int rc=blb_thread_dump_entry(th,e);
        if( rc!=0 ){
            L(prnl("unable to dump entry"));
            break;
        }
    }

    char* err=NULL;
    rocksdb_iter_get_error(it,&err);
    if( err!=NULL ){
        L(prnl("iterator error `%s`",err));
    }
    rocksdb_iter_destroy(it);
    L(prnl("dumped `%"PRIu64"` entries",cnt));
}

static int blb_rocksdb_input( thread_t* th,const input_t* i ){
    ASSERT( th->db->dbi==&blb_rocksdb_dbi );
    blb_rocksdb_t* db=(blb_rocksdb_t*)th->db;

    X(
        prnl("put `%.*s` `%.*s` `%.*s` `%.*s` %d"
            ,(int)i->rdata_len,i->rdata
            ,(int)i->rrname_len,i->rrname
            ,(int)i->rrtype_len,i->rrtype
            ,(int)i->sensorid_len,i->sensorid
            ,i->count
        )
    );

    value_t v={.count=i->count,.first_seen=i->first_seen,.last_seen=i->last_seen};
    char val[sizeof(uint32_t)*3];
    size_t val_len=sizeof(val);
    (void)blb_rocksdb_val_encode(&v,val,val_len);

    (void)snprintf(th->scrtch_key,ENGINE_THREAD_SCRTCH_SZ,
            "o\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s"
           ,(int)i->rrname_len,i->rrname
           ,(int)i->sensorid_len,i->sensorid
           ,(int)i->rrtype_len,i->rrtype
           ,(int)i->rdata_len,i->rdata
    );

    (void)snprintf(th->scrtch_inv,ENGINE_THREAD_SCRTCH_SZ,
            "i\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s"
           ,(int)i->rdata_len,i->rdata
           ,(int)i->sensorid_len,i->sensorid
           ,(int)i->rrname_len,i->rrname
           ,(int)i->rrtype_len,i->rrtype
    );

    char *err=NULL;
    rocksdb_merge(db->db,db->writeoptions,th->scrtch_key,strlen(th->scrtch_key),val,val_len,&err);
    if( err!=NULL ){
        V(prnl("rocksdb_merge() failed: `%s`",err));
        free(err);
        return(-1);
    }

    //XXX: put vs merge
    rocksdb_put(db->db,db->writeoptions,th->scrtch_inv,strlen(th->scrtch_inv),"",0,&err);
    if( err!=NULL ){
        V(prnl("rocksdb_put() failed: `%s`",err));
        free(err);
        return(-1);
    }

    return(0);
}

rocksdb_t* blb_rocksdb_handle( db_t* _db ){
    ASSERT( _db->dbi==&blb_rocksdb_dbi );
    blb_rocksdb_t* db=(blb_rocksdb_t*)_db;
    return(db->db);
}

db_t* blb_rocksdb_open( blb_rocksdb_config_t* c ){
    V(
        prnl("rocksdb database at `%s`",c->path);
        prnl("parallelism `%d` membudget `%zu` max_log_file_size `%zu` keep_log_file_num `%d`"
            ,c->parallelism
            ,c->membudget
            ,c->max_log_file_size
            ,c->keep_log_file_num
        );
    );

    blb_rocksdb_t* db=blb_new(blb_rocksdb_t);
    if( db==NULL ){ return(NULL); }
    db->dbi=&blb_rocksdb_dbi;
    char* err=NULL;
    int level_compression[5]={
        rocksdb_lz4_compression
       ,rocksdb_lz4_compression
       ,rocksdb_lz4_compression
       ,rocksdb_lz4_compression
       ,rocksdb_lz4_compression
    };

    db->mergeop=blb_rocksdb_mergeoperator_create();
    db->options=rocksdb_options_create();
    db->writeoptions=rocksdb_writeoptions_create();
    db->readoptions=rocksdb_readoptions_create();

    rocksdb_options_increase_parallelism(db->options,c->parallelism);
    rocksdb_options_optimize_level_style_compaction(db->options,c->membudget);
    rocksdb_options_set_create_if_missing(db->options,1);
    rocksdb_options_set_max_log_file_size(db->options,c->max_log_file_size);
    rocksdb_options_set_keep_log_file_num(db->options,c->keep_log_file_num);
    rocksdb_options_set_max_open_files(db->options,c->max_open_files);
    rocksdb_options_set_merge_operator(db->options,db->mergeop);
    rocksdb_options_set_compression_per_level(db->options,level_compression,5);

    db->db=rocksdb_open(db->options,c->path,&err);
    if( err!=NULL ){
        V(prnl("rocksdb_open() failed: `%s`",err));
        rocksdb_options_destroy(db->options);
        rocksdb_mergeoperator_destroy(db->mergeop);
        rocksdb_writeoptions_destroy(db->writeoptions);
        rocksdb_readoptions_destroy(db->readoptions);
        free(err);
        blb_free(db);
        return(NULL);
    }

    V(prnl("rocksdb at %p",db));

    return((db_t*)db);
}
