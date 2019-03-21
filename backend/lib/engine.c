// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdatomic.h>

#include <protocol.h>
#include <engine.h>
#include <trace.h>
#include <mpack.h>

#define ENGINE_MAX_MESSAGE_SZ (128*1024*1024)
#define ENGINE_MAX_MESSAGE_NODES (1024)
#define ENGINE_POLL_READ_TIMEOUT (60)
#define ENGINE_POLL_WRITE_TIMEOUT (10)

static atomic_int blb_engine_stop=ATOMIC_VAR_INIT(0);
static atomic_int blb_thread_cnt=ATOMIC_VAR_INIT(0);

static void blb_engine_request_stop( ){
    atomic_fetch_add(&blb_engine_stop,1);
}

static int blb_engine_poll_stop( ){
    return(atomic_load(&blb_engine_stop));
}

static void blb_thread_cnt_incr( ){
    (void)atomic_fetch_add(&blb_thread_cnt,1);
}

static void blb_thread_cnt_decr( ){
    (void)atomic_fetch_sub(&blb_thread_cnt,1);
}

static int blb_thread_cnt_get( ){
    return(atomic_load(&blb_thread_cnt));
}

static inline int blb_engine_poll_write( int fd,int seconds ){
    if( blb_engine_poll_stop()>0 ){
        V(prnl("engine stop detected"));
        return(-1);
    }
    fd_set fds;
    struct timeval to;
    FD_ZERO(&fds);
    FD_SET(fd,&fds);
    to.tv_sec=seconds;
    to.tv_usec=0;
    int rc=select(fd+1,NULL,&fds,NULL,&to);
    if( rc==0 ){
        X(prnl("select() timeout"));
        return(-1);
    }else if( rc<0 ){
        X(prnl("select() failed `%s`",strerror(errno)));
        return(-1);
    }
    return(0);
}

static inline int blb_engine_poll_read( int fd,int seconds ){
    if( blb_engine_poll_stop()>0 ){
        V(prnl("engine stop detected"));
        return(-1);
    }
    fd_set fds;
    struct timeval to;
    FD_ZERO(&fds);
    FD_SET(fd,&fds);
    to.tv_sec=seconds;
    to.tv_usec=0;
    int rc=select(fd+1,&fds,NULL,NULL,&to);
    if( rc==0 ){
        X(prnl("select() timeout"));
        return(-1);
    }else if( rc<0 ){
        X(prnl("select() failed `%s`",strerror(errno)));
        return(-1);
    }
    return(0);
}

static size_t blb_thread_read_stream_cb( mpack_tree_t* tree,char* p,size_t p_sz ){
    thread_t* th=mpack_tree_context(tree);
    int fd_ok=blb_engine_poll_read(th->fd,ENGINE_POLL_READ_TIMEOUT);
    if( fd_ok!=0 ){
        V(prnl("engine poll read failed"));
        mpack_tree_flag_error(tree,mpack_error_io);
        return(0);
    }
    ssize_t rc=read(th->fd,p,p_sz);
    if( rc<0 ){
        V(prnl("read() failed: `%s`",strerror(errno)));
        mpack_tree_flag_error(tree,mpack_error_io);
    }else if( rc==0 ){
        X(prnl("read() eof"));
        mpack_tree_flag_error(tree,mpack_error_eof);
    }
    return(rc);
}

static int blb_engine_thread_consume_backup( thread_t* th,mpack_node_t node ){
    (void)th;
    const char* p=mpack_node_bin_data(node);
    size_t p_sz=mpack_node_bin_size(node);
    T(prnl("encoded message len %zu",p_sz));
    if( p==NULL || p_sz==0 ){
        V(prnl("invalid input request"));
        return(-1);
    }

    X(
        for( size_t i=0;i<p_sz;i++ ){
            out("%02x ",(int)(unsigned char)p[i]);
        }
        out("\n");
    );

    mpack_reader_t __rd={0},*rd=&__rd;
    mpack_reader_init(rd,(char*)p,p_sz,p_sz);

    uint32_t cnt=mpack_expect_map(rd);
    if( cnt!=1 || mpack_reader_error(rd)!=mpack_ok ){
        V(prnl("invalid inner message: backup map expected"));
        goto decode_error;
    }

    ASSERT( cnt<ENGINE_THREAD_SCRTCH_BUFFERS );

    char key[1]={'\0'};
    (void)mpack_expect_str_buf(rd,key,1);
    if( key[0]!=PROTOCOL_BACKUP_REQUEST_PATH_KEY[0] ){
        V(prnl("invalid inner message: path key expected"));
        goto decode_error;
    }

    backup_t __b,*b=&__b;
    b->path_len=mpack_expect_str_buf(rd,th->scrtch[0],ENGINE_THREAD_SCRTCH_SZ);
    b->path=th->scrtch[0];

    mpack_done_map(rd);
    if( mpack_reader_error(rd)!=mpack_ok ){
        L(prnl("invalid inner message; decode backup request failed"));
        goto decode_error;
    }

    mpack_reader_destroy(rd);

    blb_dbi_backup(th,b);

    return(0);

decode_error:
    mpack_reader_destroy(rd);
    return(-1);
}

static int blb_engine_thread_consume_dump( thread_t* th,mpack_node_t node ){
    (void)th;
    const char* p=mpack_node_bin_data(node);
    size_t p_sz=mpack_node_bin_size(node);
    T(prnl("encoded message len %zu",p_sz));
    if( p==NULL || p_sz==0 ){
        V(prnl("invalid input request"));
        return(-1);
    }

    X(
        for( size_t i=0;i<p_sz;i++ ){
            out("%02x ",(int)(unsigned char)p[i]);
        }
        out("\n");
    );

    mpack_reader_t __rd={0},*rd=&__rd;
    mpack_reader_init(rd,(char*)p,p_sz,p_sz);

    uint32_t cnt=mpack_expect_map(rd);
    if( cnt!=1 || mpack_reader_error(rd)!=mpack_ok ){
        V(prnl("invalid inner message: dump map expected"));
        goto decode_error;
    }

    ASSERT( cnt<ENGINE_THREAD_SCRTCH_BUFFERS );

    char key[1]={'\0'};
    (void)mpack_expect_str_buf(rd,key,1);
    if( key[0]!=PROTOCOL_DUMP_REQUEST_PATH_KEY[0] ){
        V(prnl("invalid inner message: path key expected"));
        goto decode_error;
    }

    dump_t __d,*d=&__d;
    d->path_len=mpack_expect_str_buf(rd,th->scrtch[0],ENGINE_THREAD_SCRTCH_SZ);
    d->path=th->scrtch[0];

    mpack_done_map(rd);
    if( mpack_reader_error(rd)!=mpack_ok ){
        L(prnl("invalid inner message; decode dump request failed"));
        goto decode_error;
    }

    mpack_reader_destroy(rd);

    blb_dbi_dump(th,d);

    return(0);

decode_error:
    mpack_reader_destroy(rd);
    return(-1);
}

static int blb_engine_thread_consume_query( thread_t* th,mpack_node_t node ){
    (void)th;
    const char* p=mpack_node_bin_data(node);
    size_t p_sz=mpack_node_bin_size(node);
    T(prnl("encoded message len %zu",p_sz));
    if( p==NULL || p_sz==0 ){
        V(prnl("invalid input request"));
        return(-1);
    }

    X(
        for( size_t i=0;i<p_sz;i++ ){
            out("%02x ",(int)(unsigned char)p[i]);
        }
        out("\n");
    );

    mpack_reader_t __rd={0},*rd=&__rd;
    mpack_reader_init(rd,(char*)p,p_sz,p_sz);

    uint32_t cnt=mpack_expect_map(rd);
    if( cnt!=9 || mpack_reader_error(rd)!=mpack_ok ){
        V(prnl("invalid inner message: query map expected"));
        goto decode_error;
    }

    ASSERT( cnt<ENGINE_THREAD_SCRTCH_BUFFERS );

    struct have_t{
        bool hrrname;
        bool hrdata;
        bool hrrtype;
        bool hsensorid;
    };

    struct have_t __h={0},*h=&__h;
    query_t __q={0},*q=&__q;
    uint32_t w=0;
    for( uint32_t j=0;j<cnt;j++ ){
        char key[64]={'\0'};
        size_t key_len=mpack_expect_str_buf(rd,key,64);
        if( key_len<=0 ){
            V(prnl("invalid inner message: invalid key"));
            goto decode_error;
        }
        if( strncmp(key,PROTOCOL_QUERY_REQUEST_LIMIT_KEY,key_len)==0 ){
            X(prnl("got query request limit"));
            q->limit=mpack_expect_int(rd);
        }else if( strncmp(key,PROTOCOL_QUERY_REQUEST_QRRNAME_KEY,key_len)==0 ){
            X(prnl("got input request rrname"));
            q->qrrname_len=mpack_expect_str_buf(rd,th->scrtch[w],ENGINE_THREAD_SCRTCH_SZ);
            q->qrrname=th->scrtch[w];
            w++;
        }else if( strncmp(key,PROTOCOL_QUERY_REQUEST_HRRNAME_KEY,key_len)==0 ){
            X(prnl("got input request have rrname"));
            h->hrrname=mpack_expect_bool(rd);
        }else if( strncmp(key,PROTOCOL_QUERY_REQUEST_QRRTYPE_KEY,key_len)==0 ){
            X(prnl("got input request rrtype"));
            q->qrrtype_len=mpack_expect_str_buf(rd,th->scrtch[w],ENGINE_THREAD_SCRTCH_SZ);
            q->qrrtype=th->scrtch[w];
            w++;
        }else if( strncmp(key,PROTOCOL_QUERY_REQUEST_HRRTYPE_KEY,key_len)==0 ){
            X(prnl("got input request have rrtype"));
            h->hrrtype=mpack_expect_bool(rd);
        }else if( strncmp(key,PROTOCOL_QUERY_REQUEST_QRDATA_KEY,key_len)==0 ){
            X(prnl("got input request rdata"));
            q->qrdata_len=mpack_expect_str_buf(rd,th->scrtch[w],ENGINE_THREAD_SCRTCH_SZ);
            q->qrdata=th->scrtch[w];
            w++;
        }else if( strncmp(key,PROTOCOL_QUERY_REQUEST_HRDATA_KEY,key_len)==0 ){
            X(prnl("got input request have rdata"));
            h->hrdata=mpack_expect_bool(rd);
        }else if( strncmp(key,PROTOCOL_QUERY_REQUEST_QSENSORID_KEY,key_len)==0 ){
            X(prnl("got input request sensorid"));
            q->qsensorid_len=mpack_expect_str_buf(rd,th->scrtch[w],ENGINE_THREAD_SCRTCH_SZ);
            q->qsensorid=th->scrtch[w];
            w++;
        }else if( strncmp(key,PROTOCOL_QUERY_REQUEST_HSENSORID_KEY,key_len)==0 ){
            X(prnl("got input request have sensorid"));
            h->hsensorid=mpack_expect_bool(rd);
        }else{
            V(prnl("invalid inner message: unkown key: `%.*s`",(int)key_len,key));
            goto decode_error;
        }
    }
    mpack_done_map(rd);
    if( mpack_reader_error(rd)!=mpack_ok ){
        L(prnl("invalid inner message; decode query request failed"));
        goto decode_error;
    }

    if( !h->hsensorid ){ q->qsensorid_len=0; }
    if( !h->hrrname ){ q->qrrname_len=0; }
    if( !h->hrrtype ){ q->qrrtype_len=0; }
    if( !h->hrdata ){ q->qrdata_len=0; }

    int input_ok=blb_dbi_query(th,q);
    if( input_ok!=0 ){
        V(prnl("input request failed"));
        goto decode_error;
    }

    mpack_reader_destroy(rd);
    return(0);

decode_error:
    mpack_reader_destroy(rd);
    return(-1);
}

static int blb_engine_thread_consume_input( thread_t* th,mpack_node_t node ){
    (void)th;
    const char* p=mpack_node_bin_data(node);
    size_t p_sz=mpack_node_bin_size(node);
    T(prnl("encoded message len %zu",p_sz));
    if( p==NULL || p_sz==0 ){
        V(prnl("invalid input request"));
        return(-1);
    }

    X(
        for( size_t i=0;i<p_sz;i++ ){
            printf("%02x ",(int)(unsigned char)p[i]);
        }
        printf("\n");
    );

    mpack_reader_t __rd={0},*rd=&__rd;
    mpack_reader_init(rd,(char*)p,p_sz,p_sz);

    uint32_t cnt=mpack_expect_map(rd);
    if( cnt!=1 || mpack_reader_error(rd)!=mpack_ok ){
        V(prnl("invalid inner message: observation map expected"));
        goto decode_error;
    }

    char key[1]={'\0'};
    (void)mpack_expect_str_buf(rd,key,1);
    if( key[0]!=PROTOCOL_INPUT_REQUEST_OBSERVATION_KEY0 ){
        V(prnl("invalid inner message: observation key expected"));
        goto decode_error;
    }

    uint32_t input_cnt=mpack_expect_array(rd);
    mpack_error_t array_ok=mpack_reader_error(rd);
    if( array_ok!=mpack_ok || input_cnt==0 ){
        V(prnl("invalid inner message: array expected"));
        goto decode_error;
    }

    for( uint32_t k=0;k<input_cnt;k++ ){
        uint32_t cnt=mpack_expect_map(rd);
        mpack_error_t map_ok=mpack_reader_error(rd);
        if( map_ok!=mpack_ok || cnt!=7 ){
            V(prnl("invalid inner message: map with 7 elements expected"));
            goto decode_error;
        }
        ASSERT( cnt<ENGINE_THREAD_SCRTCH_BUFFERS );
        input_t __i={0},*i=&__i;
        uint32_t w=0;
        for( uint32_t j=0;j<cnt;j++ ){
            char key[1]={'\0'};
            (void)mpack_expect_str_buf(rd,key,1);
            switch( key[0] ){
                case PROTOCOL_PDNS_ENTRY_COUNT_KEY0:{
                    X(prnl("got input request count"));
                    i->count=mpack_expect_int(rd);
                    break;
                }
                case PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY0:{
                    X(prnl("got input request first seen"));
                    mpack_timestamp_t ts=mpack_expect_timestamp(rd);
                    i->first_seen=ts.seconds;
                    break;
                }
                case PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY0:{
                    X(prnl("got input request last seen"));
                    mpack_timestamp_t ts=mpack_expect_timestamp(rd);
                    i->last_seen=ts.seconds;
                    break;
                }
                case PROTOCOL_PDNS_ENTRY_RRNAME_KEY0:{
                    X(prnl("got input request rrname"));
                    i->rrname_len=mpack_expect_str_buf(rd,th->scrtch[w],ENGINE_THREAD_SCRTCH_SZ);
                    i->rrname=th->scrtch[w];
                    w++;
                    break;
                }
                case PROTOCOL_PDNS_ENTRY_RRTYPE_KEY0:{
                    X(prnl("got input request rrtype"));
                    i->rrtype_len=mpack_expect_str_buf(rd,th->scrtch[w],ENGINE_THREAD_SCRTCH_SZ);
                    i->rrtype=th->scrtch[w];
                    w++;
                    break;
                }
                case PROTOCOL_PDNS_ENTRY_RDATA_KEY0:{
                    X(prnl("got input request rdata"));
                    i->rdata_len=mpack_expect_str_buf(rd,th->scrtch[w],ENGINE_THREAD_SCRTCH_SZ);
                    i->rdata=th->scrtch[w];
                    w++;
                    break;
                }
                case PROTOCOL_PDNS_ENTRY_SENSORID_KEY0:{
                    X(prnl("got input request sensorid"));
                    i->sensorid_len=mpack_expect_str_buf(rd,th->scrtch[w],ENGINE_THREAD_SCRTCH_SZ);
                    i->sensorid=th->scrtch[w];
                    w++;
                    break;
                }
                default:
                    V(prnl("invalid inner message: invalid key: %02x",(int)(unsigned char)key[0]));
                    return(-1);
            }
        }
        mpack_done_map(rd);
        if( mpack_reader_error(rd)!=mpack_ok ){
            V(prnl("invalid inner message; map decode failed"));
            goto decode_error;
        }
        int input_ok=blb_dbi_input(th,i);
        if( input_ok!=0 ){
            V(prnl("input request failed"));
            goto decode_error;
        }
    }
    mpack_done_array(rd);
    mpack_done_map(rd);
    if( mpack_reader_error(rd)!=mpack_ok ){
        V(prnl("invalid inner message: array decode failed"));
        goto decode_error;
    }

    mpack_reader_destroy(rd);
    return(0);

decode_error:
    mpack_reader_destroy(rd);
    return(-1);
}

static int blb_engine_thread_consume( thread_t* th,mpack_tree_t* tree ){
    (void)th;
    mpack_node_t root=mpack_tree_root(tree);
    X(
        prnl("got message kv-pairs=%zu tree-size=%zu",mpack_node_map_count(root),mpack_tree_size(tree));
        for( size_t i=0;i<mpack_node_map_count(root);i++ ){
            mpack_node_t key=mpack_node_map_key_at(root,i);
            prnl("key[%zu]=%.*s",i,1,mpack_node_str(key));
        }
    );
    mpack_node_t type=mpack_node_map_cstr(root,PROTOCOL_TYPED_MESSAGE_TYPE_KEY);
    mpack_node_t payload=mpack_node_map_cstr(root,PROTOCOL_TYPED_MESSAGE_ENCODED_KEY);
    if( mpack_node_is_nil(type) || mpack_node_is_nil(payload) ){
        V(prnl("invalid message received"));
        return(-1);
    }
    switch( mpack_node_int(type) ){
        case PROTOCOL_INPUT_REQUEST:
            T(prnl("got input request"));
            return(blb_engine_thread_consume_input(th,payload));
        case PROTOCOL_QUERY_REQUEST:
            T(prnl("got query request"));
            return(blb_engine_thread_consume_query(th,payload));
        case PROTOCOL_BACKUP_REQUEST:
            T(prnl("got backup request"));
            return(blb_engine_thread_consume_backup(th,payload));
        case PROTOCOL_DUMP_REQUEST:
            T(prnl("got dump request"));
            return(blb_engine_thread_consume_dump(th,payload));
        default:
            T(prnl("invalid message type"));
            return(-1);
    }
}

int blb_thread_query_stream_start_response( thread_t* th ){
    T(prnl("query stream start response"));

    if( blb_engine_poll_stop()>0 ){
        L(prnl("thread <%04lx> engine stop detected",th->thread));
        return(-1);
    }

    mpack_writer_t __wr={0},*wr=&__wr;
    //encode outer message
    mpack_writer_init(wr,(char*)th->scrtch_response_outer,ENGINE_MAX_MESSAGE_SZ);
    mpack_start_map(wr,2);
        mpack_write_cstr(wr,PROTOCOL_TYPED_MESSAGE_TYPE_KEY);
        mpack_write_int(wr,PROTOCOL_QUERY_STREAM_START_RESPONSE);
        mpack_write_cstr(wr,PROTOCOL_TYPED_MESSAGE_ENCODED_KEY);
        mpack_write_bin(wr,"",0);
    mpack_finish_map(wr);
    if( mpack_writer_error(wr)!=mpack_ok ){
        V(prnl("unable to encode outer stream start response"));
        goto encode_error;
    }
    size_t used_outer=mpack_writer_buffer_used(wr);
    ASSERT( used_outer<ENGINE_MAX_MESSAGE_SZ );
    mpack_writer_destroy(wr);
    X(prnl("encoded outer message size %zu",used_outer));

    int rc=blb_engine_poll_write(th->fd,ENGINE_POLL_WRITE_TIMEOUT);
    if( rc!=0 ){
        X(prnl("egnine poll write failed"));
        goto encode_error;
    }

    uint8_t* p=(uint8_t*)th->scrtch_response_outer;
    ssize_t r=used_outer;
    while( r>0 ){
        ssize_t rc=write(th->fd,p,r);
        if( rc<0 ){
            V(prnl("write() failed error `%s`\n",strerror(errno)));
            goto encode_error;
        }else if( rc==0 && errno==EINTR ) { continue; }
        r-=rc;
        p+=rc;
    }
    return(0);

encode_error:
    mpack_writer_destroy(wr);
    return(-1);
}

int blb_thread_dump_entry( thread_t* th,const entry_t* entry ){
    X(
        prnl("dump stream push entry");
        prnl("<%d `%.*s` `%.*s` `%.*s` `%.*s`>",
            entry->count
           ,(int)entry->rrname_len,entry->rrname
           ,(int)entry->rrtype_len,entry->rrtype
           ,(int)entry->rdata_len,entry->rdata
           ,(int)entry->sensorid_len,entry->sensorid
        );
    );

    if( blb_engine_poll_stop()>0 ){
        L(prnl("thread <%04lx> engine stop detected",th->thread));
        return(-1);
    }

    mpack_writer_t __wr={0},*wr=&__wr;
    mpack_writer_init(wr,(char*)th->scrtch_response_outer,ENGINE_MAX_MESSAGE_SZ);

    mpack_start_map(wr,OBS_FIELDS);
    mpack_write_uint(wr,OBS_RRNAME_IDX);
    mpack_write_bin(wr,entry->rrname,entry->rrname_len);
    mpack_write_uint(wr,OBS_RRTYPE_IDX);
    mpack_write_bin(wr,entry->rrtype,entry->rrtype_len);
    mpack_write_uint(wr,OBS_RDATA_IDX);
    mpack_write_bin(wr,entry->rdata,entry->rdata_len);
    mpack_write_uint(wr,OBS_SENSOR_IDX);
    mpack_write_bin(wr,entry->sensorid,entry->sensorid_len);
    mpack_write_uint(wr,OBS_COUNT_IDX);
    mpack_write_uint(wr,entry->count);
    mpack_write_uint(wr,OBS_FIRST_SEEN_IDX);
    mpack_write_uint(wr,entry->first_seen);
    mpack_write_uint(wr,OBS_LAST_SEEN_IDX);
    mpack_write_uint(wr,entry->last_seen);
    mpack_finish_map(wr);

    mpack_error_t err=mpack_writer_error(wr);
    if( err!=mpack_ok ){
        fprintf(stderr,"encoding msgpack data failed err=%d\n",err);
        mpack_writer_destroy(wr);
        return(-1);
    }

    size_t used=mpack_writer_buffer_used(wr);
    X(prnl("encoded observation entry size `%zu`",used));
    ASSERT( used<ENGINE_MAX_MESSAGE_SZ );

    mpack_writer_destroy(wr);

    uint8_t* p=(uint8_t*)th->scrtch_response_outer;
    ssize_t r=used;
    while( r>0 ){
        ssize_t rc=write(th->fd,p,r);
        if( rc<0 ){
            L(prnl("write() failed `%s`",strerror(errno)));
            return(-1);
        }else if( rc==0 && errno==EINTR ) { continue; }
        r-=rc;
        p+=rc;
    }
    return(0);
}

int blb_thread_query_stream_push_response( thread_t* th,const entry_t* entry ){
    T(
        prnl("query stream push entry");
        prnl("<%d `%.*s` `%.*s` `%.*s` `%.*s`>",
            entry->count
           ,(int)entry->rrname_len,entry->rrname
           ,(int)entry->rrtype_len,entry->rrtype
           ,(int)entry->rdata_len,entry->rdata
           ,(int)entry->sensorid_len,entry->sensorid
        );
    );

    if( blb_engine_poll_stop()>0 ){
        L(prnl("thread <%04lx> engine stop detected",th->thread));
        return(-1);
    }

    mpack_writer_t __wr={0},*wr=&__wr;
    //encode inner message
    mpack_writer_init(wr,(char*)th->scrtch_response_inner,ENGINE_MAX_MESSAGE_SZ);
    mpack_start_map(wr,7);
        mpack_write_cstr(wr,PROTOCOL_PDNS_ENTRY_COUNT_KEY);
        mpack_write_uint(wr,entry->count);
        mpack_write_cstr(wr,PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY);
        //mpack_write_timestamp_seconds(wr,entry->first_seen);
        mpack_write_uint(wr,entry->first_seen);
        mpack_write_cstr(wr,PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY);
        //mpack_write_timestamp_seconds(wr,entry->last_seen);
        mpack_write_uint(wr,entry->last_seen);
        mpack_write_cstr(wr,PROTOCOL_PDNS_ENTRY_RDATA_KEY);
        mpack_write_str(wr,entry->rdata,entry->rdata_len);
        mpack_write_cstr(wr,PROTOCOL_PDNS_ENTRY_RRNAME_KEY);
        mpack_write_str(wr,entry->rrname,entry->rrname_len);
        mpack_write_cstr(wr,PROTOCOL_PDNS_ENTRY_RRTYPE_KEY);
        mpack_write_str(wr,entry->rrtype,entry->rrtype_len);
        mpack_write_cstr(wr,PROTOCOL_PDNS_ENTRY_SENSORID_KEY);
        mpack_write_str(wr,entry->sensorid,entry->sensorid_len);
    mpack_finish_map(wr);
    if( mpack_writer_error(wr)!=mpack_ok ){
        V(prnl("unable to encode inner stream data response"));
        goto encode_error;
    }
    size_t used_inner=mpack_writer_buffer_used(wr);
    ASSERT( used_inner<ENGINE_MAX_MESSAGE_SZ );
    mpack_writer_destroy(wr);
    //encode outer message
    mpack_writer_init(wr,(char*)th->scrtch_response_outer,ENGINE_MAX_MESSAGE_SZ);
    mpack_start_map(wr,2);
        mpack_write_cstr(wr,PROTOCOL_TYPED_MESSAGE_TYPE_KEY);
        mpack_write_int(wr,PROTOCOL_QUERY_STREAM_DATA_RESPONSE);
        mpack_write_cstr(wr,PROTOCOL_TYPED_MESSAGE_ENCODED_KEY);
        mpack_write_bin(wr,th->scrtch_response_inner,used_inner);
    mpack_finish_map(wr);
    if( mpack_writer_error(wr)!=mpack_ok ){
        V(prnl("unable to encode outer stream data response"));
        goto encode_error;
    }
    size_t used_outer=mpack_writer_buffer_used(wr);
    ASSERT( used_outer<ENGINE_MAX_MESSAGE_SZ );
    mpack_writer_destroy(wr);
    X(prnl("encoded inner message size %zu",used_inner));
    X(prnl("encoded outer message size %zu",used_outer));
    uint8_t* p=(uint8_t*)th->scrtch_response_outer;
    ssize_t r=used_outer;
    while( r>0 ){
        ssize_t rc=write(th->fd,p,r);
        if( rc<0 ){
            V(prnl("write() failed error `%s`",strerror(errno)));
            goto encode_error;
        }else if( rc==0 && errno==EINTR ) { continue; }
        r-=rc;
        p+=rc;
    }
    return(0);

encode_error:
    mpack_writer_destroy(wr);
    return(-1);
}

int blb_thread_query_stream_end_response( thread_t* th ){
    T(prnl("query stream end response"));

    if( blb_engine_poll_stop()>0 ){
        L(prnl("thread <%04lx> engine stop detected",th->thread));
        return(-1);
    }

    mpack_writer_t __wr={0},*wr=&__wr;
    //encode outer message
    mpack_writer_init(wr,(char*)th->scrtch_response_outer,ENGINE_MAX_MESSAGE_SZ);
    mpack_start_map(wr,2);
        mpack_write_cstr(wr,PROTOCOL_TYPED_MESSAGE_TYPE_KEY);
        mpack_write_int(wr,PROTOCOL_QUERY_STREAM_END_RESPONSE);
        mpack_write_cstr(wr,PROTOCOL_TYPED_MESSAGE_ENCODED_KEY);
        mpack_write_bin(wr,"",0);
    mpack_finish_map(wr);
    if( mpack_writer_error(wr)!=mpack_ok ){
        V(prnl("unable to encode outer stream end response"));
        goto encode_error;
    }
    size_t used_outer=mpack_writer_buffer_used(wr);
    ASSERT( used_outer<ENGINE_MAX_MESSAGE_SZ );
    mpack_writer_destroy(wr);
    X(prnl("encoded outer message size %zu",used_outer));
    uint8_t* p=(uint8_t*)th->scrtch_response_outer;
    ssize_t r=used_outer;
    while( r>0 ){
        ssize_t rc=write(th->fd,p,r);
        if( rc<0 ){
            V(prnl("write() failed error `%s`",strerror(errno)));
            goto encode_error;
        }else if( rc==0 && errno==EINTR ) { continue; }
        r-=rc;
        p+=rc;
    }

    return(0);

encode_error:
    mpack_writer_destroy(wr);
    return(-1);
}

static thread_t* blb_engine_thread_new( engine_t* e,int fd ){
    //thread_t* th=malloc(sizeof(thread_t));
    thread_t* th=blb_new(thread_t);
    if( th==NULL ){ return(NULL); }
    db_t* db=blb_dbi_clone(e->db);
    if( db==NULL ){ blb_free(th);return(NULL); }
    th->db=db;
    th->engine=e;
    th->fd=fd;
    return(th);
}

static void blb_engine_thread_teardown( thread_t *th ){
    //db_t* db=blb_dbi_clone(e->db);
    close(th->fd);
    blb_free(th);
}

static void* blb_engine_thread_fn( void* usr ){
    ASSERT( usr!=NULL );
    thread_t* th=usr;
    blb_thread_cnt_incr();
    T(prnl("new thread is <%04lx>",th->thread));

    mpack_tree_t __tree,*tree=&__tree;
    mpack_tree_init_stream(tree,blb_thread_read_stream_cb,th,ENGINE_MAX_MESSAGE_SZ,ENGINE_MAX_MESSAGE_NODES);

    while( 1 ){
        if( blb_engine_poll_stop()>0 ){
            V(prnl("thread <%04lx> engine stop detected",th->thread));
            goto thread_exit;
        }
        mpack_tree_parse(tree);
        if( mpack_tree_error(tree)!=mpack_ok ){
            goto thread_exit;
        }
        int rc=blb_engine_thread_consume(th,tree);
        if( rc!=0 ){
            goto thread_exit;
        }
    }

thread_exit:
    T(prnl("finished thread is <%04lx>",th->thread));
    mpack_tree_destroy(tree);
    blb_thread_cnt_decr();
    blb_engine_thread_teardown(th);
    //pthread_exit(NULL);
    return(NULL);
}

engine_t* blb_engine_new( db_t* db,const char* name,int port,int thread_throttle_limit ){
    ASSERT( db!=NULL );

    struct sockaddr_in __ipv4,*ipv4=&__ipv4;
    int rc = inet_pton(AF_INET,name,&ipv4->sin_addr);
    ASSERT( rc>=0 );
    if( rc!=1 ){ errno=EINVAL;return(NULL); }
    ipv4->sin_family=AF_INET;
    ipv4->sin_port=htons((uint16_t)port);
    int fd=socket(ipv4->sin_family,SOCK_STREAM,0);
    if( fd<0 ){ return(NULL); }
    int reuse=1;
    (void)setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse));
    int bind_rc=bind(fd,ipv4,sizeof(struct sockaddr_in));
    if( bind_rc<0 ){ close(fd);return(NULL); }
    int listen_rc=listen(fd,SOMAXCONN);
    if( listen_rc<0 ){ close(fd);return(NULL); }

    engine_t* e=blb_new(engine_t);
    if( e==NULL ){ close(fd);return(NULL); }

    e->thread_throttle_limit=thread_throttle_limit;
    e->db=db;
    e->listen_fd=fd;

    V(prnl("listening on host `%s` port `%d` fd `%d`",name,port,fd));

    return(e);
}

static void* blb_engine_sigint_consume( void* usr ){
    (void)usr;
    sigset_t s;
    sigemptyset(&s);
    sigaddset(&s,SIGQUIT);
    sigaddset(&s,SIGUSR1);
    sigaddset(&s,SIGUSR2);
    sigaddset(&s,SIGINT);
    sigaddset(&s,SIGPIPE);
    V(prnl("signal consumer thread started"));
    //int unblock_ok=pthread_sigmask(SIG_UNBLOCK,s,NULL);
    //V(prnl("pthread_sigmask returned `%d`",unblock_ok));
    while( 1 ){
        int sig=0;
        int rc=sigwait(&s,&sig);
        V(prnl("sigwait() returned `%d` (signal `%d`)",rc,sig));
        if( rc!=0 ){
            continue;
        }
        L(prnl("got signal `%d`",sig));
        switch( sig ){
            case SIGINT:
                L(prnl("got SIGINT; requesting engine stop"));
                blb_engine_request_stop();
                break;
            default: break;
        }
    }
    return(NULL);
}

void blb_engine_signals_init( void ){
    sigset_t s;
    sigemptyset(&s);
    sigaddset(&s,SIGQUIT);
    sigaddset(&s,SIGUSR1);
    sigaddset(&s,SIGUSR2);
    sigaddset(&s,SIGINT);
    sigaddset(&s,SIGPIPE);
    int rc=pthread_sigmask(SIG_BLOCK,&s,NULL);
    if( rc!=0 ){
        L(prnl("pthread_sigmask() failed `%d`",rc));
    }

    pthread_t signal_consumer;
    pthread_create(&signal_consumer,NULL,blb_engine_sigint_consume,NULL);
}

void blb_engine_run( engine_t* e ){
    struct sockaddr_in __addr,*addr=&__addr;
    socklen_t addrlen=sizeof(struct sockaddr_in);

    (void)signal(SIGPIPE,SIG_IGN);
    //(void)signal(SIGINT,blb_engine_sigint_consume);

    pthread_attr_t __attr;
    pthread_attr_init(&__attr);
    pthread_attr_setdetachstate(&__attr,PTHREAD_CREATE_DETACHED);

    fd_set fds;
    struct timeval to;
    while( 1 ){
timeout_retry:
        if( blb_engine_poll_stop()>0 ){
            V(prnl("engine stop detected"));
            goto wait;
        }
        if( blb_thread_cnt_get()>=e->thread_throttle_limit ){
            sleep(1);
            V(prnl("thread throttle reached"));
            goto timeout_retry;
        }
        FD_ZERO(&fds);
        FD_SET(e->listen_fd,&fds);
        to.tv_sec=5;
        to.tv_usec=0;
        int rc=select(e->listen_fd+1,&fds,NULL,NULL,&to);
        if( rc==0 ){
            X(prnl("select() timeout"));
            goto timeout_retry;
        }else if( rc<0 ){
            X(prnl("select() failed `%s`",strerror(errno)));
            goto wait;
        }
        socket_t fd=accept(e->listen_fd,(struct sockaddr*)addr,&addrlen);
        if( fd<0 ){
            V(prnl("accept() failed: `%s`; exiting",strerror(errno)));
            blb_engine_request_stop();
            goto wait;
        }
        thread_t* th=blb_engine_thread_new(e,fd);
        if( th==NULL ){
            V(prnl("blb_engine_thread_new() failed; exiting"));
            blb_engine_request_stop();
            goto wait;
        }

        (void)pthread_create(&th->thread,&__attr,blb_engine_thread_fn,(void*)th);
    }

wait:

    pthread_attr_destroy(&__attr);

    while( blb_thread_cnt_get()>0 ){
        L(prnl("waiting for `%d` threads to finish",blb_thread_cnt_get()));
        sleep(1);
    }

    L(prnl("done"));
}

void blb_engine_teardown( engine_t* e ){
    blb_free(e);
}
