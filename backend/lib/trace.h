
#ifndef __TRACE_H
#define __TRACE_H

#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>

#define V( body )                        \
  do {                                   \
    if( theTrace_get_verbosity() > 0 ) { \
      theTrace_lock();                   \
      body;                              \
      theTrace_release();                \
    }                                    \
  } while( 0 )
#define T( body )                        \
  do {                                   \
    if( theTrace_get_verbosity() > 1 ) { \
      theTrace_lock();                   \
      body;                              \
      theTrace_release();                \
    }                                    \
  } while( 0 )
#define X( body )                        \
  do {                                   \
    if( theTrace_get_verbosity() > 2 ) { \
      theTrace_lock();                   \
      body;                              \
      theTrace_release();                \
    }                                    \
  } while( 0 )
#define L( body )       \
  do {                  \
    theTrace_lock();    \
    body;               \
    theTrace_release(); \
  } while( 0 )

#define prnl( fmt, ... ) \
  theTrace_output( 16 * 8 + 6, "(%s) " fmt "\n", __func__, ##__VA_ARGS__ )
#define out( fmt, ... ) theTrace_output( 16 * 8 + 7, fmt, ##__VA_ARGS__ )
#define panic( fmt, ... )                                                     \
  do {                                                                        \
    theTrace_output( 16 * 8 + 0, "(%s) " fmt "\n", __func__, ##__VA_ARGS__ ); \
    exit( 0 );                                                                \
  } while( 0 )

#define WHEN_V if( theTrace_get_verbosity() > 0 )
#define WHEN_T if( theTrace_get_verbosity() > 1 )
#define WHEN_X if( theTrace_get_verbosity() > 2 )

#define ASSERT( p )                        \
  do {                                     \
    if( !( p ) ) {                         \
      prnl(                                \
          "assert failed `%s` [%s:%s:%d]", \
          #p,                              \
          __FILE__,                        \
          __FUNCTION__,                    \
          __LINE__ );                      \
      __builtin_trap();                    \
      exit( 0 );                           \
    }                                      \
  } while( 0 )

typedef struct trace_config_t trace_config_t;
struct trace_config_t {
  FILE* stream;
  const char* host;
  const char* app;
  pid_t procid;
};

typedef struct trace_t trace_t;
struct trace_t {
  atomic_int verbosity;
  pthread_mutex_t _lock;
  trace_config_t config;
  void ( *init )( trace_t* trace, const trace_config_t* config );
  void ( *lock )( trace_t* trace );
  void ( *release )( trace_t* trace );
  void ( *output )( trace_t* trace, int priority, const char* fmt, va_list ap );
  void ( *flush )( trace_t* trace );
};

void theTrace_set( trace_t* trace );
trace_t* theTrace_get( void );
void theTrace_stream_use( const trace_config_t* config );

__attribute__( ( format( printf, 2, 3 ) ) ) static inline void theTrace_output(
    int priority, const char* fmt, ... ) {
  va_list ap;
  va_start( ap, fmt );
  theTrace_get()->output( theTrace_get(), priority, fmt, ap );
  va_end( ap );
}

static inline void theTrace_lock( void ) {
  theTrace_get()->lock( theTrace_get() );
}

static inline void theTrace_release( void ) {
  theTrace_get()->release( theTrace_get() );
}

static inline int theTrace_get_verbosity( void ) {
  return ( atomic_load( &theTrace_get()->verbosity ) );
}

static inline void theTrace_set_verbosity( int verbosity ) {
  atomic_store( &theTrace_get()->verbosity, verbosity );
}

static inline void theTrace_init( const trace_config_t* config ) {
  theTrace_get()->init( theTrace_get(), config );
}

static inline void theTrace_flush( void ) {
  theTrace_get()->flush( theTrace_get() );
}

#endif
