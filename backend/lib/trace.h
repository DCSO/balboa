
#ifndef __TRACE_H
#define __TRACE_H

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>

#ifdef __TRACE__
#define V( body )do{if( theTrace_get_verbosity()>0 ){theTrace_lock();body;theTrace_release();}}while( 0 )
#define T( body )do{if( theTrace_get_verbosity()>1 ){theTrace_lock();body;theTrace_release();}}while( 0 )
#define X( body )do{if( theTrace_get_verbosity()>2 ){theTrace_lock();body;theTrace_release();}}while( 0 )
#define L( body )do{theTrace_lock();body;theTrace_release();}while( 0 )

#define prnl(fmt,...)theTrace_output("(%s) "fmt"\n",__func__,##__VA_ARGS__)
#define out(fmt,...)theTrace_output(fmt,##__VA_ARGS__)
#define panic(fmt,...)do{theTrace_output("(%s) "fmt"\n",__func__,##__VA_ARGS__);exit(0);}while( 0 )

#define ASSERT(p) \
    do{\
        if( !(p) ){ \
            prnl("assert failed `%s` [%s:%s:%d]",#p,__FILE__,__FUNCTION__,__LINE__); \
            __builtin_trap();\
            exit(0); \
        }\
    }while( 0 )
#else
#define T(body)
#define V(body)
#define X(body)
#define L(body)do{theTrace_lock();body;theTrace_release();}while( 0 )
#define ASSERT(p) do{if( !(p) ){exit(0);}}while( 0 )
#define panic(fmt,...)do{exit(0);}while( 0 )
#endif

typedef struct trace_t trace_t;
struct trace_t{
    atomic_int verbosity;
    void ( *init )( trace_t* trace );
    void ( *lock )( trace_t* trace );
    void ( *release )( trace_t* trace );
    void ( *output )( trace_t* trace, const char* fmt, va_list ap );
    void ( *flush )( trace_t* trace );
};

void theTrace_set( trace_t* trace );
trace_t* theTrace_get( void );
void theTrace_stdout_use( void );
void theTrace_stderr_use( void );

__attribute__((format(printf,1,2))) static inline void theTrace_output( const char* fmt, ... ){
    va_list ap;
    va_start(ap,fmt);
    theTrace_get()->output(theTrace_get(),fmt,ap);
    va_end(ap);
}

static inline void theTrace_lock( void ){
    theTrace_get()->lock(theTrace_get());
}

static inline void theTrace_release( void ){
    theTrace_get()->release(theTrace_get());
}

static inline int theTrace_get_verbosity( void ){
    return(atomic_load(&theTrace_get()->verbosity));
}

static inline void theTrace_set_verbosity( int verbosity ){
    atomic_store(&theTrace_get()->verbosity,verbosity);
}

static inline void theTrace_init( void ){
    theTrace_get()->init(theTrace_get());
}

static inline void theTrace_flush( void ){
    theTrace_get()->flush(theTrace_get());
}

#endif
