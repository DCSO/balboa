// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite-impl.h>

#include <sqlite3.h>

typedef struct blb_sqlite_t blb_sqlite_t;

static void blb_sqlite_teardown( db_t* _db );
static db_t* blb_sqlite_thread_init( thread_t* th, db_t* db );
static void blb_sqlite_thread_deinit( thread_t* th, db_t* db );
static int blb_sqlite_query( thread_t* th, const protocol_query_request_t* q );
static int blb_sqlite_input( thread_t* th, const protocol_input_request_t* i );
static void blb_sqlite_dump( thread_t* th, const protocol_dump_request_t* d );
static void blb_sqlite_backup(
    thread_t* th, const protocol_backup_request_t* b );

static const dbi_t blb_sqlite_dbi = {.thread_init = blb_sqlite_thread_init,
                                     .thread_deinit = blb_sqlite_thread_deinit,
                                     .teardown = blb_sqlite_teardown,
                                     .query = blb_sqlite_query,
                                     .input = blb_sqlite_input,
                                     .backup = blb_sqlite_backup,
                                     .dump = blb_sqlite_dump};

struct blb_sqlite_t {
  const dbi_t* dbi;
  sqlite3* db;
  sqlite3_stmt* insert_stmt;
  sqlite3_stmt* query_stmt;
};

db_t* blb_sqlite_thread_init( thread_t* th, db_t* db ) {
  th->usr_ctx = NULL;
  return ( db );
}

void blb_sqlite_thread_deinit( thread_t* th, db_t* db ) {
  ( void )th;
  ( void )db;
}

void blb_sqlite_teardown( db_t* _db ) {
  ASSERT( _db->dbi == &blb_sqlite_dbi );
  blb_sqlite_t* db = ( blb_sqlite_t* )_db;
  sqlite3_finalize( db->insert_stmt );
  sqlite3_finalize( db->query_stmt );
  sqlite3_close( db->db );
  blb_free( _db );
}

static int blb_sqlite_query( thread_t* th, const protocol_query_request_t* q ) {
  ( void )q;

  int start_ok = blb_thread_query_stream_start_response( th );
  if( start_ok != 0 ) {
    L( log_error( "unable to start query stream response" ) );
    return ( -1 );
  }

  protocol_entry_t __e, *e = &__e;
  e->sensorid = "test-sensor-id";
  e->sensorid_len = strlen( e->sensorid );
  e->rdata = "";
  e->rdata_len = 0;
  e->rrname = "test-rrname";
  e->rrname_len = strlen( e->rrname );
  e->rrtype = "A";
  e->rrtype_len = 1;
  e->count = 23;
  e->first_seen = 15000000;
  e->last_seen = 15001000;
  int push_ok = blb_thread_query_stream_push_response( th, e );
  if( push_ok != 0 ) {
    L( log_error( "unable to push query response entry" ) );
    return ( -1 );
  }

  ( void )blb_thread_query_stream_end_response( th );

  return ( 0 );
}

#define SQLITE_BIND_RRNAME_IDX ( 1 )
#define SQLITE_BIND_RRTYPE_IDX ( 2 )
#define SQLITE_BIND_RDATA_IDX ( 3 )
#define SQLITE_BIND_SENSORID_IDX ( 4 )
#define SQLITE_BIND_COUNT_IDX ( 5 )
#define SQLITE_BIND_FIRSTSEEN_IDX ( 6 )
#define SQLITE_BIND_LASTSEEN_IDX ( 7 )

static int blb_sqlite_input( thread_t* th, const protocol_input_request_t* i ) {
  ASSERT( th->db->dbi == &blb_sqlite_dbi );
  blb_sqlite_t* db = ( blb_sqlite_t* )th->db;

  X( blb_protocol_log_entry( &i->entry ) );

  sqlite3_bind_text(
      db->insert_stmt,
      SQLITE_BIND_RRNAME_IDX,
      i->entry.rrname,
      i->entry.rrname_len,
      SQLITE_STATIC );
  sqlite3_bind_text(
      db->insert_stmt,
      SQLITE_BIND_RRTYPE_IDX,
      i->entry.rrtype,
      i->entry.rrtype_len,
      SQLITE_STATIC );
  sqlite3_bind_text(
      db->insert_stmt,
      SQLITE_BIND_RDATA_IDX,
      i->entry.rdata,
      i->entry.rdata_len,
      SQLITE_STATIC );
  sqlite3_bind_text(
      db->insert_stmt,
      SQLITE_BIND_SENSORID_IDX,
      i->entry.sensorid,
      i->entry.sensorid_len,
      SQLITE_STATIC );
  sqlite3_bind_int( db->insert_stmt, SQLITE_BIND_COUNT_IDX, i->entry.count );
  sqlite3_bind_int(
      db->insert_stmt, SQLITE_BIND_FIRSTSEEN_IDX, i->entry.first_seen );
  sqlite3_bind_int(
      db->insert_stmt, SQLITE_BIND_LASTSEEN_IDX, i->entry.last_seen );

  int step_ok = sqlite3_step( db->insert_stmt );
  if( step_ok != SQLITE_DONE ) {
    L( log_error( "sqlite3_step() failed with rc `%d`", step_ok ) );
    sqlite3_clear_bindings( db->insert_stmt );
    return ( -1 );
  }

  sqlite3_clear_bindings( db->insert_stmt );
  sqlite3_reset( db->insert_stmt );

  return ( 0 );
}

static void blb_sqlite_backup(
    thread_t* th, const protocol_backup_request_t* b ) {
  ASSERT( th->db->dbi == &blb_sqlite_dbi );
  // blb_sqlite_t* db=(blb_sqlite_t*)th->db;

  X( log_debug( "backup `%.*s`", ( int )b->path_len, b->path ) );
}

static void blb_sqlite_dump( thread_t* th, const protocol_dump_request_t* d ) {
  ASSERT( th->db->dbi == &blb_sqlite_dbi );
  // blb_sqlite_t* db=(blb_sqlite_t*)th->db;

  X( log_debug( "dump `%.*s`", ( int )d->path_len, d->path ) );
}

static const char* _create_table =
    "\n\
create table if not exists pdns(\n\
    rrname text not null\n\
   ,rrtype text not null\n\
   ,rdata blob not null\n\
   ,sensorid text not null\n\
   ,count integer not null\n\
   ,first_seen integer not null\n\
   ,last_seen integer not null\n\
   ,primary key ( rrname,rrtype,rdata,sensorid )\n\
);\n\
pragma journal_mode=%s;\n\
pragma synchronous=off;\n\
";

static const char* _insert_stmt =
    "\n\
insert into pdns (rrname,rrtype,rdata,sensorid,count,first_seen,last_seen)\n\
        values (?,?,?,?,?,?,?)\n\
    on conflict (rrname,rrtype,rdata,sensorid) do update\n\
        set\n\
            count=count+excluded.count\n\
           ,first_seen=min(first_seen,excluded.first_seen)\n\
           ,last_seen=max(last_seen,excluded.last_seen)\n\
;";

db_t* blb_sqlite_open( const blb_sqlite_config_t* config ) {
  ASSERT( config->path != NULL );
  blb_sqlite_t* db = blb_new( blb_sqlite_t );
  if( db == NULL ) { return ( NULL ); }
  db->dbi = &blb_sqlite_dbi;

  V( log_info( "sqlite database is `%s`", config->path ) );

  if( strcmp( config->journal_mode, "wal" ) != 0
      && strcmp( config->journal_mode, "memory" ) != 0 ) {
    L( log_error( "unknown journal_mode `%s`", config->journal_mode ) );
    blb_free( db );
    return ( NULL );
  }

  db->insert_stmt = NULL;
  db->query_stmt = NULL;
  int rc = sqlite3_open( config->path, &db->db );
  if( rc != SQLITE_OK ) {
    L( log_error(
        "sqlite_open() failed with `%s`", sqlite3_errmsg( db->db ) ) );
    ASSERT( db->db != NULL );
    sqlite3_close( db->db );
    blb_free( db );
    return ( NULL );
  }

  char _create_table_stmt[1024];
  ( void )snprintf(
      _create_table_stmt,
      sizeof( _create_table_stmt ),
      _create_table,
      config->journal_mode );

  char* err = NULL;
  int stmt_ok = sqlite3_exec( db->db, _create_table_stmt, NULL, NULL, &err );
  if( stmt_ok != SQLITE_OK ) {
    ASSERT( err != NULL );
    L( log_error( "sqlite3_exec() failed with `%s`", err ) );
    sqlite3_free( err );
    sqlite3_close( db->db );
    blb_free( db );
    return ( NULL );
  }

  int insert_stmt_ok = sqlite3_prepare_v2(
      db->db, _insert_stmt, strlen( _insert_stmt ), &db->insert_stmt, NULL );
  if( insert_stmt_ok != SQLITE_OK ) {
    L( log_error(
        "sqlite3_prepare_v2() failed with `%s`", sqlite3_errmsg( db->db ) ) );
    sqlite3_close( db->db );
    blb_free( db );
    return ( NULL );
  }

  ASSERT( db->insert_stmt != NULL );

  return ( ( db_t* )db );
}
