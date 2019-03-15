// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <engine.h>
#include <ketopt.h>
#include <mock.h>
#include <trace.h>

int main( int argc,char** argv ){
    int verbosity=0;
    int daemonize=0;
    char* host="127.0.0.1";
    int port=4242;
    int thread_throttle_limit=64;
    ketopt_t opt=KETOPT_INIT;
    int c;
    while( (c=ketopt(&opt,argc,argv,1,"j:l:p:vD",NULL))>=0 ){
        switch( c ){
            case 'D': daemonize=1;break;
            case 'l': host=opt.arg;break;
            case 'p': port=atoi(opt.arg);break;
            case 'v': verbosity+=1;break;
            case 'j': thread_throttle_limit=atoi(opt.arg);break;
            default:break;
        }
    }

    theTrace_stdout_use();
    theTrace_init();
    if( daemonize ){ theTrace_set_verbosity(0); }
    else{ theTrace_set_verbosity(verbosity); }

    db_t* db=blb_mock_open();
    if( db==NULL ){
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