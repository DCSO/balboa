
#ifdef __TRACE__

#include <inttypes.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>

#include <trace.h>

static trace_t* theTrace=NULL;

static pthread_mutex_t theMutex=PTHREAD_MUTEX_INITIALIZER;

void theTrace_set( trace_t* trace ){
    theTrace=trace;
}

trace_t* theTrace_get( void ){
    return(theTrace);
}

static void stdout_lock( trace_t* trace ){
    (void)trace;
    pthread_mutex_lock(&theMutex);
}

static void stdout_release( trace_t* trace ){
    (void)trace;
    //fflush(stdout);
    pthread_mutex_unlock(&theMutex);
}

static void stdout_output( trace_t* trace, const char* fmt, va_list ap ){
    (void)trace;
    vfprintf(stdout,fmt,ap);
}

static void stdout_flush( trace_t* trace ){
    (void)trace;
    fflush(stdout);
}

static void stdout_init( trace_t* trace ){
    (void)trace;
    //pthread_mutex_init(&theMutex,NULL);
}

static void stderr_lock( trace_t* trace ){
    (void)trace;
    pthread_mutex_lock(&theMutex);
}

static void stderr_release( trace_t* trace ){
    (void)trace;
    //fflush(stdout);
    pthread_mutex_unlock(&theMutex);
}

static void stderr_output( trace_t* trace, const char* fmt, va_list ap ){
    (void)trace;
    vfprintf(stderr,fmt,ap);
}

static void stderr_flush( trace_t* trace ){
    (void)trace;
    fflush(stderr);
}

static void stderr_init( trace_t* trace ){
    (void)trace;
    //pthread_mutex_init(&theMutex,NULL);
}

static trace_t __theTrace_stdout={
    .verbosity=0,
    .release=stdout_release,
    .init=stdout_init,
    .lock=stdout_lock,
    .output=stdout_output,
    .flush=stdout_flush,
};

static trace_t __theTrace_stderr={
    .verbosity=0,
    .release=stderr_release,
    .init=stderr_init,
    .lock=stderr_lock,
    .output=stderr_output,
    .flush=stderr_flush,
};

static trace_t*const theTrace_stdout=&__theTrace_stdout;
static trace_t*const theTrace_stderr=&__theTrace_stderr;

void theTrace_stdout_use( void ){
    theTrace_set(theTrace_stdout);
}

void theTrace_stderr_use( void ){
    theTrace_set(theTrace_stderr);
}

#endif