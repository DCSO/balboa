
#ifdef __TRACE__

#include <inttypes.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>

#include <trace.h>

// try to be https://tools.ietf.org/html/rfc5424 conform

static trace_t* theTrace=NULL;

void theTrace_set( trace_t* trace ){
    theTrace=trace;
}

trace_t* theTrace_get( void ){
    return(theTrace);
}

static void trace_lock( trace_t* trace ){
    (void)trace;
    pthread_mutex_lock(&trace->_lock);
}

static void trace_release( trace_t* trace ){
    (void)trace;
    //fflush(stdout);
    pthread_mutex_unlock(&trace->_lock);
}

static void trace_output( trace_t* trace,int priority,const char* fmt,va_list ap ){
    (void)trace;
    time_t now;
    time(&now);
    struct tm tm;
    localtime_r(&now,&tm);
    char b[128];
    size_t len=strftime(b,sizeof(b),"%FT%T%z",&tm);
    fprintf(trace->config.stream
        ,"<%d>1 %s %s %.*s "
        ,priority
        ,trace->config.host
        ,trace->config.app
        ,(int)len,b
    );
    vfprintf(trace->config.stream,fmt,ap);
}

static void trace_flush( trace_t* trace ){
    (void)trace;
    fflush(stdout);
}

static void trace_init( trace_t* trace,const trace_config_t* config ){
    trace->config=*config;
    pthread_mutex_init(&trace->_lock,NULL);
}

static trace_t __theTrace_stdout={
    .verbosity=ATOMIC_VAR_INIT(0)
   ,.config={0}
   ,.release=trace_release
   ,.init=trace_init
   ,.lock=trace_lock
   ,.output=trace_output
   ,.flush=trace_flush
};

static trace_t*const theTrace_stdout=&__theTrace_stdout;

void theTrace_stream_use( const trace_config_t* config ){
    theTrace_set(theTrace_stdout);
    theTrace_init(config);
}

#endif