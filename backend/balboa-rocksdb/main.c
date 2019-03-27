// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <engine.h>
#include <ketopt.h>
#include <rocksdb-impl.h>
#include <unistd.h>
#include <trace.h>

__attribute__((noreturn)) void version( void ){
    fprintf(stderr,"balboa-rocksdb v2.0.0\n");
    exit(1);
}

__attribute__((noreturn)) void usage( const blb_rocksdb_config_t* c ){
    fprintf(stderr,"\
`balboa-rocksdb` provides a pdns database backend for `balboa`\n\
\n\
Usage: balboa-rocksdb [options]\n\
\n\
    -h display help\n\
    -D daemonize (default: off)\n\
    -d <path> path to rocksdb database (default: `%s`)\n\
    -l listen address (default: 127.0.0.1)\n\
    -p listen port (default: 4242)\n\
    -v increase verbosity; can be passed multiple times\n\
    -j thread throttle limit, maximum concurrent connections (default: 64)\n\
    --membudget <memory-in-bytes> rocksdb membudget option (value: %"PRIu64")\n\
    --parallelism <number-of-threads> rocksdb parallelism option (value: %d)\n\
    --max_log_file_size <size> rocksdb log file size option (value: %"PRIu64")\n\
    --max_open_files <number> rocksdb max number of open files (value: %d)\n\
    --keep_log_file_num <number> rocksdb max number of log files (value: %d)\n\
    --database_path <path> same as `-d`\n"
,c->path
,c->membudget
,c->parallelism
,c->max_log_file_size
,c->max_open_files
,c->keep_log_file_num
    );
    exit(1);
}

int main( int argc,char** argv ){
    int verbosity=0;
    int daemonize=0;
    char* host="127.0.0.1";
    int port=4242;
    int thread_throttle_limit=64;
    blb_rocksdb_config_t config=blb_rocksdb_config_init();
    trace_config_t trace_config={
        .stream=stderr
       ,.host="pdns"
       ,.app="balboa-rocksdb"
       // leaking process number ...
       ,.procid=getpid()
    };
    ketopt_t opt=KETOPT_INIT;
    static ko_longopt_t opts[]={
        {"membudget",ko_required_argument,301}
       ,{"parallelism",ko_required_argument,302}
       ,{"max_log_file_size",ko_required_argument,303}
       ,{"max_open_files",ko_required_argument,304}
       ,{"keep_log_file_num",ko_required_argument,305}
       ,{"database_path",ko_required_argument,306}
       ,{"version",ko_no_argument,307}
       ,{NULL,0,0}
    };
    int c;
    while( (c=ketopt(&opt,argc,argv,1,"j:d:l:p:vDh",opts))>=0 ){
        switch( c ){
            case 'D': daemonize=1;break;
            case 'd': config.path=opt.arg;break;
            case 'l': host=opt.arg;break;
            case 'p': port=atoi(opt.arg);break;
            case 'v': verbosity+=1;break;
            case 'j': thread_throttle_limit=atoi(opt.arg);break;
            case 'h': usage(&config);
            case 301: config.membudget=atoll(opt.arg);break;
            case 302: config.parallelism=atoi(opt.arg);break;
            case 303: config.max_log_file_size=atoi(opt.arg);break;
            case 304: config.max_open_files=atoi(opt.arg);break;
            case 305: config.keep_log_file_num=atoi(opt.arg);break;
            case 306: config.path=opt.arg;break;
            case 307: version();
            default:usage(&config);
        }
    }

    theTrace_stream_use(&trace_config);
    if( daemonize ){ theTrace_set_verbosity(0); }
    else{ theTrace_set_verbosity(verbosity); }

    blb_engine_signals_init();

    db_t* db=blb_rocksdb_open(&config);
    if( db==NULL ){
        V(prnl("unable to open rocksdb at path `%s`",config.path));
        return(1);
    }

    engine_t* e=blb_engine_new(db,host,port,thread_throttle_limit);
    if( e==NULL ){
        V(prnl("unable to create engine"));
        blb_dbi_teardown(db);
        return(1);
    }

    blb_engine_run(e);

    blb_engine_teardown(e);

    blb_dbi_teardown(db);

    return(0);
}