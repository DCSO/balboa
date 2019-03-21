// balboa
// Copyright (c) 2019, DCSO GmbH

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <engine.h>
#include <protocol.h>
#include <trace.h>
#include <bs.h>
#include <ketopt.h>
#include <mpack.h>

static int verbosity=0;

typedef struct state_t state_t;
struct state_t{
    uint8_t* scrtch0;
    size_t scrtch0_sz;
    uint8_t* scrtch1;
    size_t scrtch1_sz;
    uint8_t* scrtch2;
    size_t scrtch2_sz;
    FILE* os;
    int sock;
    int (*dump_entry_cb)( state_t* state,entry_t* entry );
};

static int dump_state_init( state_t* state ){
    state->scrtch0_sz=1024*1024*10;
    state->scrtch0=malloc(state->scrtch0_sz);
    if( state->scrtch0==NULL ){ return(-1); }
    state->scrtch1_sz=1024*1024*10;
    state->scrtch1=malloc(state->scrtch1_sz);
    if( state->scrtch1==NULL ){ free(state->scrtch0);return(-1); }
    state->scrtch2_sz=1024*1024*10;
    state->scrtch2=malloc(state->scrtch2_sz);
    if( state->scrtch2==NULL ){ free(state->scrtch0);free(state->scrtch1);return(-1); }
    state->os=NULL;
    state->sock=-1;
    return(0);
}

static void dump_state_teardown( state_t* state ){
    free(state->scrtch0);
    free(state->scrtch1);
    free(state->scrtch2);
}

static inline int dump_entry_decode( mpack_reader_t* rd, entry_t* entry, const bytestring_sink_t* in ){
    bytestring_sink_t sink=*in;
    for( int i=0;i<OBS_FIELDS;i++ ){
        uint32_t field=mpack_expect_uint(rd);
        switch( field ){
            case OBS_RRTYPE_IDX:{
                size_t sz=mpack_expect_bin_buf(rd,(char*)sink.p,sink.available);
                entry->rrtype=(const char*)sink.p;
                entry->rrtype_len=sz;
                sink=bs_sink_slice0(&sink,sz);
                if( sink.p==0 ){return(-1);}
                break;
            }
            case OBS_RDATA_IDX:{
                size_t sz=mpack_expect_bin_buf(rd,(char*)sink.p,sink.available);
                entry->rdata=(const char*)sink.p;
                entry->rdata_len=sz;
                sink=bs_sink_slice0(&sink,sz);
                if( sink.p==0 ){return(-1);}
                break;
            }
            case OBS_SENSOR_IDX:{
                size_t sz=mpack_expect_bin_buf(rd,(char*)sink.p,sink.available);
                entry->sensorid=(const char*)sink.p;
                entry->sensorid_len=sz;
                sink=bs_sink_slice0(&sink,sz);
                if( sink.p==0 ){return(-1);}
                break;
            }
            case OBS_RRNAME_IDX:{
                size_t sz=mpack_expect_bin_buf(rd,(char*)sink.p,sink.available);
                entry->rrname=(const char*)sink.p;
                entry->rrname_len=sz;
                sink=bs_sink_slice0(&sink,sz);
                if( sink.p==0 ){return(-1);}
                break;
            }
            case OBS_COUNT_IDX:
                entry->count=mpack_expect_uint(rd);
                break;
            case OBS_LAST_SEEN_IDX:
                entry->last_seen=mpack_expect_uint(rd);
                break;
            case OBS_FIRST_SEEN_IDX:
                entry->first_seen=mpack_expect_uint(rd);
                break;
            default:
                V(fprintf(stderr,"unknown field index=%u\n",field));
                return(-1);
        }
    }
    return(0);
}

static ssize_t dump_process( state_t* state,FILE* is ){
    bytestring_sink_t sink=bs_sink(state->scrtch0,state->scrtch0_sz);
    mpack_reader_t __rd={0},*rd=&__rd;
    mpack_reader_init_stdfile(rd,is,0);
    ssize_t entries=0;
    mpack_error_t err=mpack_ok;
    while( err==mpack_ok ){
        uint32_t cnt=mpack_expect_map(rd);
        mpack_error_t map_ok=mpack_reader_error(rd);
        if( map_ok!=mpack_ok ){
            if( map_ok==mpack_error_eof ){
                V(fprintf(stderr,"dump finished; eof reached\n"));
            }else{
                fprintf(stderr,"unexpected mpack decode error=%d\n",map_ok);
            }
            break;
        }
        V(fprintf(stderr,"got map with cnt=%d\n",cnt));
        if( cnt!=OBS_FIELDS ){
            fprintf(stderr,"invalid map with #entries=%u; aborting\n",cnt);
            break;
        }
        entry_t __entry={0},*entry=&__entry;
        int entry_ok=dump_entry_decode(rd,entry,&sink);
        if( entry_ok!=0 ){
            fprintf(stderr,"decoding entry failed; aborting\n");
            break;
        }
        mpack_done_map(rd);
        err=mpack_reader_error(rd);
        if( err==mpack_ok ){
            entries++;
            int rc=state->dump_entry_cb(state,entry);
            if( rc!=0 ){
                fprintf(stderr, "dump_entry_db failed with rc=%d\n",rc);
                break;
            }
        }
    }
    if( err!=mpack_error_eof ){
        V(fprintf(stderr,"mpack decode error=%d after entries=%zu\n",err,entries));
    }
    mpack_reader_destroy(rd);
    return(entries);
}

static int dump( state_t* state,const char* dump_file ){
    ASSERT( state!=NULL );
    ASSERT( state->dump_entry_cb!=NULL );
    V(fprintf(stderr,"dump file is `%s`\n",dump_file));
    FILE* f=NULL;
    if( strcmp(dump_file,"-")==0 ){
        f=stdin;
    }else{
        f=fopen(dump_file,"rb");
    }
    if( f==NULL ){
        fprintf(stderr,"unable to open file `%s`\n",dump_file);
        return(-1);
    }
    ssize_t rc=dump_process(state,f);
    fprintf(stderr,"processed %zd entries\n",rc);
    dump_state_teardown(state);
    fclose(f);
    if( rc<0 ){ return(-1); }else{ return(0); }
}

static int dump_entry_json_cb( state_t* state,entry_t* entry ){
    assert( state->os!=NULL );
    bytestring_sink_t __sink=bs_sink(state->scrtch1,state->scrtch1_sz);
    bytestring_sink_t* sink=&__sink;
    int ok=0;
    char buf[64]={0};
    ok+=bs_cat(sink,"{\"rrname\":\"",11);
    ok+=bs_append_escape(sink,(const uint8_t*)entry->rrname,entry->rrname_len);
    ok+=bs_cat(sink,"\",\"rrtype\":\"",12);
    ok+=bs_append_escape(sink,(const uint8_t*)entry->rrtype,entry->rrtype_len);
    ok+=bs_cat(sink,"\",\"sensor_id\":\"",15);
    ok+=bs_append_escape(sink,(const uint8_t*)entry->sensorid,entry->sensorid_len);
    ok+=bs_cat(sink,"\",\"rdata\":\"",11);
    ok+=bs_append_escape(sink,(const uint8_t*)entry->rdata,entry->rdata_len);
    ok+=bs_cat(sink,"\",\"count\":",10);
    snprintf(buf,63,"%u",entry->count);
    ok+=bs_cat(sink,buf,strlen(buf));
    ok+=bs_cat(sink,",\"first_seen\":",14);
    snprintf(buf,63,"%u",entry->first_seen);
    ok+=bs_cat(sink,buf,strlen(buf));
    ok+=bs_cat(sink,",\"last_seen\":",13);
    snprintf(buf,63,"%u",entry->last_seen);
    ok+=bs_cat(sink,buf,strlen(buf));
    ok+=bs_append1(sink,'}');
    ok+=bs_append1(sink,'\n');
    if( ok==0 ){
        fwrite(sink->p,sink->index,1,state->os);
        return(0);
    }else{
        fputs("{\"error\":\"buffer-out-of-space\"}",state->os);
        return(-1);
    }
    return(0);
}

static int dump_entry_replay_cb( state_t* state,entry_t* entry ){
    ASSERT( state->sock!=-1 );
    mpack_writer_t __wr={0},*wr=&__wr;
    //encode inner message
    mpack_writer_init(wr,(char*)state->scrtch1,state->scrtch1_sz);
    mpack_start_map(wr,1);
        mpack_write_cstr(wr,"O");
        mpack_start_array(wr,1);
            mpack_start_map(wr,7);
                mpack_write_cstr(wr,"D");
                mpack_write_str(wr,(const char*)entry->rdata,entry->rdata_len);
                mpack_write_cstr(wr,"N");
                mpack_write_str(wr,(const char*)entry->rrname,entry->rrname_len);
                mpack_write_cstr(wr,"T");
                mpack_write_str(wr,(const char*)entry->rrtype,entry->rrtype_len);
                mpack_write_cstr(wr,"I");
                mpack_write_str(wr,(const char*)entry->sensorid,entry->sensorid_len);
                mpack_write_cstr(wr,"C");
                mpack_write_uint(wr,entry->count);
                mpack_write_cstr(wr,"F");
                mpack_write_timestamp_seconds(wr,entry->first_seen);
                mpack_write_cstr(wr,"L");
                mpack_write_timestamp_seconds(wr,entry->last_seen);
            mpack_finish_map(wr);
        mpack_finish_array(wr);
    mpack_finish_map(wr);
    mpack_error_t err=mpack_writer_error(wr);
    if( err!=mpack_ok ){
        fprintf(stderr,"encoding inner msgpack data failed err=%d\n",err);
        mpack_writer_destroy(wr);
        return(-1);
    }
    size_t used_inner=mpack_writer_buffer_used(wr);
    V(fprintf(stderr,"encoded inner message size=%zu\n",used_inner));
    assert( used_inner<state->scrtch1_sz );
    mpack_writer_destroy(wr);
    //encode outer message
    mpack_writer_init(wr,(char*)state->scrtch2,state->scrtch2_sz);
    mpack_start_map(wr,2);
        mpack_write_cstr(wr,"T");
        mpack_write_int(wr,1);
        mpack_write_cstr(wr,"M");
        mpack_write_bin(wr,(char*)state->scrtch1,used_inner);
    mpack_finish_map(wr);
    mpack_error_t outer_err=mpack_writer_error(wr);
    if( outer_err!=mpack_ok ){
        fprintf(stderr,"encoding outer msgpack data failed err=%d\n",err);
        mpack_writer_destroy(wr);
        return(-1);
    }
    size_t used_outer=mpack_writer_buffer_used(wr);
    assert( used_outer<state->scrtch1_sz );
    V(fprintf(stderr,"encoded outer message size=%zu\n",used_outer));
    mpack_writer_destroy(wr);
    uint8_t* p=state->scrtch2;
    ssize_t r=used_outer;
    while( r>0 ){
        ssize_t rc=write(state->sock,p,r);
        if( rc<0 ){
            fprintf(stderr,"write() failed error=%s\n",strerror(errno));
            return(-1);
        }else if( rc==0 && errno==EINTR ) { continue; }
        r-=rc;
        p+=rc;
    }
    return(0);
}

static int main_jsonize( int argc,char** argv ){
    const char* db="-";
    trace_config_t trace_config={
        .stream=stderr
       ,.host="pdns"
       ,.app="balboa-backend-console"
       // leaking process number ...
       ,.procid=getpid()
    };
    ketopt_t opt=KETOPT_INIT;
    int c;
    while( (c=ketopt(&opt,argc,argv,1,"r:v",NULL))>=0 ){
        switch( c ){
            case 'r': db=opt.arg;break;
            case 'v': verbosity+=1;break;
            default: break;
        }
    }

    theTrace_stream_use(&trace_config);
    theTrace_set_verbosity(verbosity);

    V(prnl("dump file is `%s`",db));

    state_t __state={0},*state=&__state;
    int state_ok=dump_state_init(state);
    if( state_ok!=0 ){
        fprintf(stderr,"unable to initialize the dump state\n");
        return(-1);
    }
    state->os=stdout;
    state->dump_entry_cb=dump_entry_json_cb;
    int rc=dump(state,db);
    return(rc);
}

static int dump_connect( const char* host,const char* _port ){
    int port=atoi(_port);
    struct sockaddr_in addr;
    int addr_ok=inet_pton(AF_INET,host,&addr.sin_addr);
    if( addr_ok!=1 ){ return(-1); }
    addr.sin_family=AF_INET;
    addr.sin_port=htons((uint16_t)port);
    int fd=socket(addr.sin_family,SOCK_STREAM,0);
    if( fd<0 ){ return(-1); }
    int rc=connect(fd,&addr,sizeof(struct sockaddr_in));
    if( rc<0 ){ close(fd);return(-1); }
    return(fd);
}

__attribute__((noreturn)) void version( void ){
    fprintf(stderr,"balboa-backend-console v2.0.0\n");
    exit(1);
}

__attribute__((noreturn)) void usage( void ){
    fprintf(stderr,"\
`balboa-backend-console` is a management tool for `balboa-backends`\n\
\n\
Usage: balboa-backend-console <--version|help|jsonize|dump|replay> [options]\n\
\n\
Command help:\n\
    show help\n\
\n\
Command jsonize:\n\
    read a dump file and print all entries as json\n\
\n\
    -r <path> path to the dump file to read\n\
\n\
Command dump:\n\
    connect to a `balboa-backend` and request a dump of all data to local stdout\n\
\n\
    -h <host> ip address of the `balboa-backend` (default: 127.0.0.1)\n\
    -p <port> port of the `balboa-backend` (default: 4242)\n\
    -v increase verbosity; can be passed multiple times\n\
    -r <remote-path> unused (default: -)\n\
\n\
Command replay:\n\
    replay a previously generated database dump\n\
\n\
    -d <path> database dump file or `-` for stdin (default: -)\n\
    -h <host> ip address of the `balboa-backend` (default: 127.0.0.1)\n\
    -p <port> port of the `balboa-backend` (default: 4242)\n\
    -v increase verbosity; can be passed multiple times\n\
\n\
Examples:\n\
\n\
balboa-backend-console jsonize -r /tmp/pdns.dmp\n\
lz4cat /tmp/pdns.dmp.lz4 | balboa-backend-console jsonize\n\
\n\
\n");
    exit(1);
}

static int main_dump( int argc,char** argv ){
    const char* host="127.0.0.1";
    const char* port="4242";
    const char* remote_path="-";
    trace_config_t trace_config={
        .stream=stderr
       ,.host="pdns"
       ,.app="balboa-backend-console"
       // leaking process number ...
       ,.procid=getpid()
    };
    ketopt_t opt=KETOPT_INIT;
    int c;
    while( (c=ketopt(&opt,argc,argv,1,"h:p:v:r:",NULL))>=0 ){
        switch( c ){
            case 'h': host=opt.arg;break;
            case 'p': port=opt.arg;break;
            case 'v': verbosity+=1;break;
            case 'r': remote_path=opt.arg;break;
            default:break;
        }
    }

    theTrace_stream_use(&trace_config);
    theTrace_set_verbosity(verbosity);

    V(prnl("host `%s` port `%s` remote_path `%s`",host,port,remote_path));
    int sock=dump_connect(host,port);
    if( sock<0 ){
        L(prnl("unable to connect to backend"));
        return(-1);
    }

    char scrtch[1024];
    size_t scrtch_sz=sizeof(scrtch);
    protocol_dump_request_t req={.path=remote_path};
    ssize_t used=blb_protocol_encode_dump_request(&req,scrtch,scrtch_sz);
    if( used<=0 ){
        L(prnl("blb_protocol_encode_dump_request() failed `%zd`",used));
        close(sock);
        return(-1);
    }
    char* p=scrtch;
    ssize_t r=scrtch_sz;
    while( r>0 ){
        ssize_t rc=write(sock,p,r);
        if( rc<0 ){
            L(prnl("write() failed `%s`",strerror(errno)));
            close(sock);
            return(-1);
        }else if( rc==0 && errno==EINTR ) { continue; }
        r-=rc;
        p+=rc;
    }

    while( 1 ){
        ssize_t rc=read(sock,scrtch,scrtch_sz);
        if( rc==0 ){
            fflush(stdout);
            close(sock);
            return(0);
        }else if( rc<0 ){
            L(prnl("read() failed `%s`",strerror(errno)));
            close(sock);
            return(-1);
        }
        rc=fwrite(scrtch,rc,1,stdout);
        if( rc!=1 ){
            L(prnl("fwrite() failed `%s`",strerror(errno)));
            close(sock);
            return(-1);
        }
    }
}

static int main_replay( int argc,char** argv ){
    const char* host="127.0.0.1";
    const char* port="4242";
    const char* db="/tmp/balboa";
    trace_config_t trace_config={
        .stream=stderr
       ,.host="pdns"
       ,.app="balboa-backend-console"
       // leaking process number ...
       ,.procid=getpid()
    };
    ketopt_t opt=KETOPT_INIT;
    int c;
    while( (c=ketopt(&opt,argc,argv,1,"d:h:p:v",NULL))>=0 ){
        switch( c ){
            case 'd': db=opt.arg;break;
            case 'h': host=opt.arg;break;
            case 'p': port=opt.arg;break;
            case 'v': verbosity+=1;break;
            default:break;
        }
    }

    theTrace_stream_use(&trace_config);
    theTrace_set_verbosity(verbosity);

    V(fprintf(stderr,"host=%s port=%s db=%s\n",host,port,db));
    int sock=dump_connect(host,port);
    if( sock<0 ){
        fprintf(stderr,"unable to connect to backend\n");
        return(-1);
    }
    state_t __state={0},*state=&__state;
    int state_ok=dump_state_init(state);
    if( state_ok!=0 ){
        fprintf(stderr,"unable to initialize the dump state\n");
        return(-1);
    }
    state->sock=sock;
    state->dump_entry_cb=dump_entry_replay_cb;
    int rc=dump(state,db);
    return(rc);
}

int main( int argc,char** argv ){
    int res=-1;
    if( argc<2 ){
        usage();
    }else if( strcmp(argv[1],"jsonize")==0 ){
        argc--;
        argv++;
        res=main_jsonize(argc,argv);
    }else if( strcmp(argv[1],"replay")==0 ){
        argc--;
        argv++;
        res=main_replay(argc,argv);
    }else if( strcmp(argv[1],"dump")==0 ){
        argc--;
        argv++;
        res=main_dump(argc,argv);
    }else if( strcmp(argv[1],"--version")==0 ){
        version();
    }else{
        usage();
    }

    return(res);
}