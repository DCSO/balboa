// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <engine.h>
#include <ketopt.h>
#include <rocksdb.h>
#include <trace.h>

int main( int argc,char** argv ){
    int verbosity=0;
    int daemonize=0;
    char* host="127.0.0.1";
    int port=4242;
    int thread_throttle_limit=64;
    blb_rocksdb_config_t config=blb_rocksdb_config_init();

    ketopt_t opt=KETOPT_INIT;
    static ko_longopt_t opts[]={
        {"membudget",ko_required_argument,301}
       ,{"parallelism",ko_required_argument,302}
       ,{"max_log_file_size",ko_required_argument,303}
       ,{"max_open_files",ko_required_argument,304}
       ,{"keep_log_file_num",ko_required_argument,305}
       ,{"database_path",ko_required_argument,306}
       ,{NULL,0,0}
    };
    int c;
    while( (c=ketopt(&opt,argc,argv,1,"j:d:l:p:vD",opts))>=0 ){
        switch( c ){
            case 'D': daemonize=1;break;
            case 'd': config.path=opt.arg;break;
            case 'l': host=opt.arg;break;
            case 'p': port=atoi(opt.arg);break;
            case 'v': verbosity+=1;break;
            case 'j': thread_throttle_limit=atoi(opt.arg);break;
            case 301: config.membudget=atoll(opt.arg);break;
            case 302: config.parallelism=atoi(opt.arg);break;
            case 303: config.max_log_file_size=atoi(opt.arg);break;
            case 304: config.max_open_files=atoi(opt.arg);break;
            case 305: config.keep_log_file_num=atoi(opt.arg);break;
            case 306: config.path=opt.arg;break;
            default:break;
        }
    }

    theTrace_stdout_use();
    theTrace_init();
    if( daemonize ){ theTrace_set_verbosity(0); }
    else{ theTrace_set_verbosity(verbosity); }

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

    blb_dbi_teardown(db);

    blb_engine_teardown(e);

    return(0);
}