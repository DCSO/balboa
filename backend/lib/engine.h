
#ifndef __ENGINE_H
#define __ENGINE_H

#include <inttypes.h>
#include <protocol.h>
#include <pthread.h>
#include <trace.h>

#define ENGINE_THREAD_SCRTCH_SZ ( 1024 * 10 )
#define ENGINE_THREAD_SCRTCH_BUFFERS ( 10 )

static inline void* blb_realloc_impl(
    const char* name, void* p, size_t new_size ) {
  void* pp = realloc( p, new_size );
  X( prnl( "(%s) realloc `%p` `%p` `%zu`", name, p, pp, new_size ) );
  return ( pp );
}

#define blb_new( ty )                                 \
  ( {                                                 \
    size_t p_sz = sizeof( ty );                       \
    void* p = malloc( p_sz );                         \
    X( prnl( "new `%s` `%zu` `%p`", #ty, p_sz, p ) ); \
    p;                                                \
  } )
#define blb_malloc( sz )                      \
  ( {                                         \
    size_t p_sz = ( sz );                     \
    void* p = malloc( p_sz );                 \
    X( prnl( "alloc `%zu` `%p`", p_sz, p ) ); \
    p;                                        \
  } )
#define blb_realloc( p, sz )                             \
  ( {                                                    \
    size_t p_sz = ( sz );                                \
    void* pp = realloc( ( p ), p_sz );                   \
    X( prnl( "realloc `%p` `%zu` `%p`", pp, p_sz, p ) ); \
    pp;                                                  \
  } )
#define blb_free( p )                \
  do {                               \
    X( prnl( "free `%p`", ( p ) ) ); \
    free( p );                       \
  } while( 0 )

typedef int socket_t;

typedef struct dbi_t dbi_t;
typedef struct db_t db_t;
typedef struct engine_t engine_t;
typedef struct thread_t thread_t;

#define QUERY_TYPE_DEFAULT 0

struct dbi_t {
  db_t* ( *thread_init )( thread_t*, db_t* );
  void ( *thread_deinit )( thread_t*, db_t* );
  void ( *teardown )( db_t* db );
  int ( *query )( thread_t* th, const protocol_query_request_t* query );
  int ( *input )( thread_t* th, const protocol_input_request_t* input );
  void ( *backup )( thread_t* th, const protocol_backup_request_t* backup );
  void ( *dump )( thread_t* th, const protocol_dump_request_t* dump );
};

struct db_t {
  const dbi_t* dbi;
};

struct engine_t {
  int thread_throttle_limit;
  db_t* db;
  socket_t listen_fd;
};

struct thread_t {
  pthread_t thread;
  engine_t* engine;
  db_t* db;
  void* usr_ctx;
  socket_t fd;
  char scrtch[ENGINE_THREAD_SCRTCH_BUFFERS][ENGINE_THREAD_SCRTCH_SZ];
  char scrtch_response_outer[ENGINE_THREAD_SCRTCH_SZ];
  char scrtch_response_inner[ENGINE_THREAD_SCRTCH_SZ];
  char scrtch_key[ENGINE_THREAD_SCRTCH_SZ];
  char scrtch_inv[ENGINE_THREAD_SCRTCH_SZ];
};

static inline db_t* blb_dbi_thread_init( thread_t* th, db_t* db ) {
  return ( db->dbi->thread_init( th, db ) );
}

static inline void blb_dbi_thread_deinit( thread_t* th, db_t* db ) {
  db->dbi->thread_deinit( th, db );
}

static inline void blb_dbi_teardown( db_t* db ) {
  db->dbi->teardown( db );
}

static inline int blb_dbi_query(
    thread_t* th, const protocol_query_request_t* q ) {
  return ( th->db->dbi->query( th, q ) );
}

static inline int blb_dbi_input(
    thread_t* th, const protocol_input_request_t* i ) {
  return ( th->db->dbi->input( th, i ) );
}

static inline void blb_dbi_backup(
    thread_t* th, const protocol_backup_request_t* b ) {
  th->db->dbi->backup( th, b );
}

static inline void blb_dbi_dump(
    thread_t* th, const protocol_dump_request_t* d ) {
  th->db->dbi->dump( th, d );
}

void blb_engine_signals_init( void );
engine_t* blb_engine_new(
    db_t* db, const char* name, int port, int thread_throttle_limit );
void blb_engine_teardown( engine_t* e );
void blb_engine_run( engine_t* e );

int blb_thread_query_stream_start_response( thread_t* th );
int blb_thread_query_stream_push_response(
    thread_t*, const protocol_entry_t* entry );
int blb_thread_query_stream_end_response( thread_t* th );
int blb_thread_dump_entry( thread_t* th, const protocol_entry_t* entry );

#endif