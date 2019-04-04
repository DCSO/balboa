// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <rocksdb-impl.h>
#include <rocksdb/c.h>

static void blb_rocksdb_teardown( db_t* _db );
static db_t* blb_rocksdb_thread_init( thread_t* th, db_t* db );
static void blb_rocksdb_thread_deinit( thread_t* th, db_t* db );
static int blb_rocksdb_query( thread_t* th, const protocol_query_request_t* q );
static int blb_rocksdb_input( thread_t* th, const protocol_input_request_t* i );
static void blb_rocksdb_backup(
    thread_t* th, const protocol_backup_request_t* b );
static void blb_rocksdb_dump( thread_t* th, const protocol_dump_request_t* d );

static const dbi_t blb_rocksdb_dbi = {
    .thread_init = blb_rocksdb_thread_init,
    .thread_deinit = blb_rocksdb_thread_deinit,
    .teardown = blb_rocksdb_teardown,
    .query = blb_rocksdb_query,
    .input = blb_rocksdb_input,
    .backup = blb_rocksdb_backup,
    .dump = blb_rocksdb_dump};

struct blb_rocksdb_t {
  const dbi_t* dbi;
  rocksdb_t* db;
  rocksdb_options_t* options;
  rocksdb_writeoptions_t* writeoptions;
  rocksdb_readoptions_t* readoptions;
  rocksdb_mergeoperator_t* mergeop;
};

rocksdb_t* blb_rocksdb_handle( db_t* db );
rocksdb_readoptions_t* blb_rocksdb_readoptions( db_t* db );

typedef struct value_t value_t;
struct value_t {
  uint32_t count;
  uint32_t first_seen;
  uint32_t last_seen;
};

static inline value_t blb_rocksdb_val_init() {
  return ( ( value_t ){.count = 0, .first_seen = UINT32_MAX, .last_seen = 0} );
}

#define blb_rocksdb_max( a, b ) \
  ( {                           \
    __typeof__( a ) _a = ( a ); \
    __typeof__( b ) _b = ( b ); \
    _a > _b ? _a : _b;          \
  } )

#define blb_rocksdb_min( a, b ) \
  ( {                           \
    __typeof__( a ) _a = ( a ); \
    __typeof__( b ) _b = ( b ); \
    _a < _b ? _a : _b;          \
  } )

static inline void _write_u32_le( unsigned char* p, uint32_t v ) {
  p[0] = v >> 0;
  p[1] = v >> 8;
  p[2] = v >> 16;
  p[3] = v >> 24;
}

static inline uint32_t _read_u32_le( const unsigned char* p ) {
  return (
      ( ( ( uint32_t )p[0] ) << 0 ) | ( ( ( uint32_t )p[1] ) << 8 )
      | ( ( ( uint32_t )p[2] ) << 16 ) | ( ( ( uint32_t )p[3] ) << 24 ) );
}

static inline int blb_rocksdb_val_encode(
    const struct value_t* o, char* buf, size_t buflen ) {
  size_t minlen = sizeof( uint32_t ) * 3;
  if( buflen < minlen ) { return ( -1 ); }

  unsigned char* p = ( unsigned char* )buf;
  _write_u32_le( p + 0, o->count );
  _write_u32_le( p + 4, o->last_seen );
  _write_u32_le( p + 8, o->first_seen );

  return ( 0 );
}

static inline int blb_rocksdb_val_decode(
    value_t* o, const char* buf, size_t buflen ) {
  size_t minlen = sizeof( uint32_t ) * 3;
  if( buflen < minlen ) { return ( -1 ); };

  const unsigned char* p = ( const unsigned char* )buf;
  o->count = _read_u32_le( p + 0 );
  o->last_seen = _read_u32_le( p + 4 );
  o->first_seen = _read_u32_le( p + 8 );

  return ( 0 );
}

static inline void blb_rocksdb_val_merge( value_t* lhs, const value_t* rhs ) {
  lhs->count += rhs->count;
  lhs->last_seen = blb_rocksdb_max( lhs->last_seen, rhs->last_seen );
  lhs->first_seen = blb_rocksdb_min( lhs->first_seen, rhs->first_seen );
}

static char* blb_rocksdb_merge_fully(
    void* state,
    const char* key,
    size_t key_length,
    value_t* obs,
    const char* const* operands_list,
    const size_t* operands_list_length,
    int num_operands,
    unsigned char* success,
    size_t* new_value_length ) {
  ( void )state;
  if( key_length < 1 ) {
    L( prnl( "impossible: key too short" ) );
    *success = ( unsigned char )0;
    return ( NULL );
  }
  if( key[0] == 'i' ) {
    L( prnl( "impossible: got an inverted key during merge" ) );
    // this is an inverted index key with no meaningful value
    char* res = malloc( sizeof( char ) * 1 );
    if( res == NULL ) { return ( NULL ); }
    *res = '\0';
    *new_value_length = 1;
    *success = 1;
    return ( res );
  } else if( key[0] == 'o' ) {
    // this is an observation value
    size_t buf_length = sizeof( uint32_t ) * 3;
    char* buf = malloc( buf_length );
    if( buf == NULL ) { return ( NULL ); }
    for( int i = 0; i < num_operands; i++ ) {
      value_t nobs = {0, 0, 0};
      blb_rocksdb_val_decode(
          &nobs, operands_list[i], operands_list_length[i] );
      blb_rocksdb_val_merge( obs, &nobs );
    }
    blb_rocksdb_val_encode( obs, buf, buf_length );
    *new_value_length = buf_length;
    *success = ( unsigned char )1;
    return ( buf );
  } else {
    L( prnl( "impossbile: unknown key format encountered" ) );
    *success = ( unsigned char )0;
    return ( NULL );
  }
}

static char* blb_rocksdb_mergeop_full_merge(
    void* state,
    const char* key,
    size_t key_length,
    const char* existing_value,
    size_t existing_value_length,
    const char* const* operands_list,
    const size_t* operands_list_length,
    int num_operands,
    unsigned char* success,
    size_t* new_value_length ) {
  ( void )state;
  if( key_length < 1 ) {
    L( prnl( "impossible: key to short" ) );
    *success = ( unsigned char )0;
    return ( NULL );
  }
  value_t obs = blb_rocksdb_val_init();
  if( key[0] == 'o' && existing_value != NULL ) {
    blb_rocksdb_val_decode( &obs, existing_value, existing_value_length );
  }
  char* result = blb_rocksdb_merge_fully(
      state,
      key,
      key_length,
      &obs,
      operands_list,
      operands_list_length,
      num_operands,
      success,
      new_value_length );
  return ( result );
}

static char* blb_rocksdb_mergeop_partial_merge(
    void* state,
    const char* key,
    size_t key_length,
    const char* const* operands_list,
    const size_t* operands_list_length,
    int num_operands,
    unsigned char* success,
    size_t* new_value_length ) {
  if( key_length < 1 ) {
    V( prnl( "impossible: key too short" ) );
    *success = ( unsigned char )0;
    return ( NULL );
  }
  value_t obs = blb_rocksdb_val_init();
  char* result = blb_rocksdb_merge_fully(
      state,
      key,
      key_length,
      &obs,
      operands_list,
      operands_list_length,
      num_operands,
      success,
      new_value_length );
  return ( result );
}

static void blb_rocksdb_mergeop_destructor( void* state ) {
  ( void )state;
}

static const char* blb_rocksdb_mergeop_name( void* state ) {
  ( void )state;
  return ( "observation-mergeop" );
}

static inline rocksdb_mergeoperator_t* blb_rocksdb_mergeoperator_create() {
  return ( rocksdb_mergeoperator_create(
      NULL,
      blb_rocksdb_mergeop_destructor,
      blb_rocksdb_mergeop_full_merge,
      blb_rocksdb_mergeop_partial_merge,
      NULL,
      blb_rocksdb_mergeop_name ) );
}

db_t* blb_rocksdb_thread_init( thread_t* th, db_t* db ) {
  ( void )th;
  return ( db );
}

void blb_rocksdb_thread_deinit( thread_t* th, db_t* db ) {
  ( void )th;
  ( void )db;
}

void blb_rocksdb_teardown( db_t* _db ) {
  ASSERT( _db->dbi == &blb_rocksdb_dbi );

  blb_rocksdb_t* db = ( blb_rocksdb_t* )_db;
  rocksdb_close( db->db );
  rocksdb_mergeoperator_destroy( db->mergeop );
  rocksdb_writeoptions_destroy( db->writeoptions );
  rocksdb_readoptions_destroy( db->readoptions );
  // keeping this causes segfault; rocksdb_close seems to handle dealloc...
  // rocksdb_options_destroy(db->options);
  blb_free( db );
}

static int blb_rocksdb_query_by_o(
    thread_t* th, const protocol_query_request_t* q ) {
  ASSERT( th->db->dbi == &blb_rocksdb_dbi );
  blb_rocksdb_t* db = ( blb_rocksdb_t* )th->db;
  size_t prefix_len = 0;
  if( q->qsensorid_len > 0 ) {
    prefix_len = q->qsensorid_len + q->qrrname_len + 4;
    ( void )snprintf(
        th->scrtch_key,
        ENGINE_THREAD_SCRTCH_SZ,
        "o\x1f%.*s\x1f%.*s\x1f",
        ( int )q->qrrname_len,
        q->qrrname,
        ( int )q->qsensorid_len,
        q->qsensorid );
  } else {
    prefix_len = q->qrrname_len + 3;
    ( void )snprintf(
        th->scrtch_key,
        ENGINE_THREAD_SCRTCH_SZ,
        "o\x1f%.*s\x1f",
        ( int )q->qrrname_len,
        q->qrrname );
  }

  X( prnl( "prefix key `%.*s`", ( int )prefix_len, th->scrtch_key ) );

  int start_ok = blb_thread_query_stream_start_response( th );
  if( start_ok != 0 ) {
    V( prnl( "unable to start query stream response" ) );
    return ( -1 );
  }

  rocksdb_iterator_t* it = rocksdb_create_iterator( db->db, db->readoptions );
  rocksdb_iter_seek( it, th->scrtch_key, prefix_len );
  size_t keys_visited = 0;
  size_t keys_hit = 0;
  for( ; rocksdb_iter_valid( it ) != ( unsigned char )0
         && keys_hit < ( size_t )q->limit;
       rocksdb_iter_next( it ) ) {
    keys_visited += 1;
    size_t key_len = 0;
    const char* key = rocksdb_iter_key( it, &key_len );
    if( key == NULL ) {
      V( prnl( "impossible: unable to extract key from rocksdb iterator" ) );
      goto stream_error;
    }

    enum TokIdx { RRNAME = 0, SENSORID = 1, RRTYPE = 2, RDATA = 3, FIELDS = 4 };

    struct Tok {
      const char* tok;
      int tok_len;
    };

    struct Tok toks[FIELDS] = {NULL};

    enum TokIdx j = RRNAME;
    size_t last = 1;
    for( size_t i = 2; i < key_len; i++ ) {
      if( key[i] == '\x1f' ) {
        // we fixup the RDATA and skip extra \x1f's
        if( j < RDATA ) {
          toks[j].tok = &key[last + 1];
          toks[j].tok_len = i - last - 1;
          last = i;
          j++;
        }
      }
    }
    toks[RDATA].tok = &key[last + 1];
    toks[RDATA].tok_len = key_len - last - 1;

    X(
        out( "o %.*s %.*s %.*s %.*s\n",
             toks[RRNAME].tok_len,
             toks[RRNAME].tok,
             toks[SENSORID].tok_len,
             toks[SENSORID].tok,
             toks[RRTYPE].tok_len,
             toks[RRTYPE].tok,
             toks[RDATA].tok_len,
             toks[RDATA].tok ) );

    size_t qrrname_len = q->qrrname_len;
    if( toks[RRNAME].tok_len <= 0
        || memcmp(
               toks[RRNAME].tok,
               q->qrrname,
               blb_rocksdb_min( ( size_t )toks[RRNAME].tok_len, qrrname_len ) )
               != 0 ) {
      break;
    }
    if( ( size_t )toks[RRNAME].tok_len != qrrname_len ) { continue; }

    if( toks[SENSORID].tok_len == 0
        || ( q->qsensorid_len > 0
             && ( size_t )toks[SENSORID].tok_len != q->qsensorid_len )
        || ( q->qsensorid_len > 0
             && memcmp(
                    toks[SENSORID].tok, q->qsensorid, toks[SENSORID].tok_len )
                    != 0 ) ) {
      continue;
    }

    if( toks[RDATA].tok_len == 0
        || ( q->qrdata_len > 0
             && ( size_t )toks[RDATA].tok_len != q->qrdata_len )
        || ( q->qrdata_len > 0
             && memcmp( toks[RDATA].tok, q->qrdata, toks[RDATA].tok_len )
                    != 0 ) ) {
      continue;
    }

    if( toks[RRTYPE].tok_len == 0
        || ( q->qrrtype_len > 0
             && ( size_t )toks[RRTYPE].tok_len != q->qrrtype_len )
        || ( q->qrrtype_len > 0
             && memcmp( toks[RRTYPE].tok, q->qrrtype, toks[RRTYPE].tok_len )
                    != 0 ) ) {
      continue;
    }

    size_t val_size = 0;
    value_t v;
    const char* val = rocksdb_iter_value( it, &val_size );
    int ret = blb_rocksdb_val_decode( &v, val, val_size );
    if( ret != 0 ) {
      X( prnl( "unable to decode observation value; skipping entry" ) );
      continue;
    }

    keys_hit += 1;
    protocol_entry_t __e, *e = &__e;
    e->sensorid = toks[SENSORID].tok;
    e->sensorid_len = toks[SENSORID].tok_len;
    e->rdata = toks[RDATA].tok;
    e->rdata_len = toks[RDATA].tok_len;
    e->rrname = toks[RRNAME].tok;
    e->rrname_len = toks[RRNAME].tok_len;
    e->rrtype = toks[RRTYPE].tok;
    e->rrtype_len = toks[RRTYPE].tok_len;
    e->count = v.count;
    e->first_seen = v.first_seen;
    e->last_seen = v.last_seen;
    int push_ok = blb_thread_query_stream_push_response( th, e );
    if( push_ok != 0 ) {
      X( prnl( "unable to push query response entry" ) );
      goto stream_error;
    }
  }
  rocksdb_iter_destroy( it );
  ( void )blb_thread_query_stream_end_response( th );
  return ( 0 );

stream_error:
  rocksdb_iter_destroy( it );
  return ( -1 );
}

static int blb_rocksdb_query_by_i(
    thread_t* th, const protocol_query_request_t* q ) {
  ASSERT( th->db->dbi == &blb_rocksdb_dbi );
  blb_rocksdb_t* db = ( blb_rocksdb_t* )th->db;
  size_t prefix_len = 0;
  if( q->qsensorid_len > 0 ) {
    prefix_len = q->qrdata_len + q->qsensorid_len + 4;
    ( void )snprintf(
        th->scrtch_inv,
        ENGINE_THREAD_SCRTCH_SZ,
        "i\x1f%.*s\x1f%.*s\x1f",
        ( int )q->qrdata_len,
        q->qrdata,
        ( int )q->qsensorid_len,
        q->qsensorid );
  } else {
    prefix_len = q->qrdata_len + 3;
    ( void )snprintf(
        th->scrtch_inv,
        ENGINE_THREAD_SCRTCH_SZ,
        "i\x1f%.*s\x1f",
        ( int )q->qrdata_len,
        q->qrdata );
  }
  ASSERT( th->scrtch_inv[prefix_len] == '\0' );

  X( prnl( "prefix key `%.*s`", ( int )prefix_len, th->scrtch_inv ) );

  int start_ok = blb_thread_query_stream_start_response( th );
  if( start_ok != 0 ) {
    V( prnl( "unable to start query stream response" ) );
    return ( -1 );
  }

  rocksdb_iterator_t* it = rocksdb_create_iterator( db->db, db->readoptions );
  rocksdb_iter_seek( it, th->scrtch_inv, prefix_len );
  size_t keys_visited = 0;
  int keys_hit = 0;
  for( ; rocksdb_iter_valid( it ) != ( unsigned char )0 && keys_hit < q->limit;
       rocksdb_iter_next( it ) ) {
    keys_visited += 1;
    size_t key_len = 0;
    const char* key = rocksdb_iter_key( it, &key_len );
    char* err = NULL;

    enum TokIdx { RDATA = 3, SENSORID = 2, RRNAME = 1, RRTYPE = 0, FIELDS = 4 };

    struct Tok {
      const char* tok;
      int tok_len;
    };

    struct Tok toks[FIELDS] = {NULL};

    enum TokIdx j = RRTYPE;
    size_t last = key_len;
    for( ssize_t i = key_len - 1; i > 0; i-- ) {
      if( key[i] == '\x1f' ) {
        if( j < FIELDS ) {
          toks[j].tok = &key[i + 1];
          toks[j].tok_len = last - i - 1;
          last = i;
          j++;
        }
      }
    }
    toks[RDATA].tok = key + 2;
    toks[RDATA].tok_len = toks[RDATA].tok_len + last - 1;

    X(
        out( "i %.*s | %.*s %.*s %.*s\n",
             toks[RDATA].tok_len,
             toks[RDATA].tok,
             toks[SENSORID].tok_len,
             toks[SENSORID].tok,
             toks[RRTYPE].tok_len,
             toks[RRTYPE].tok,
             toks[RRNAME].tok_len,
             toks[RRNAME].tok ) );

    size_t qrdata_len = q->qrdata_len;
    if( toks[RDATA].tok_len <= 0
        || memcmp(
               toks[RDATA].tok,
               q->qrdata,
               blb_rocksdb_min( ( size_t )toks[RDATA].tok_len, qrdata_len ) )
               != 0 ) {
      break;
    }
    if( ( size_t )toks[RDATA].tok_len != qrdata_len ) { continue; }

    if( toks[SENSORID].tok_len == 0
        || ( q->qsensorid_len > 0
             && ( size_t )toks[SENSORID].tok_len != q->qsensorid_len )
        || ( q->qsensorid_len > 0
             && memcmp(
                    toks[SENSORID].tok, q->qsensorid, toks[SENSORID].tok_len )
                    != 0 ) ) {
      continue;
    }

    if( toks[RRTYPE].tok_len == 0
        || ( q->qrrtype_len > 0
             && ( size_t )toks[RRTYPE].tok_len != q->qrrtype_len )
        || ( q->qrrtype_len > 0
             && memcmp( toks[RRTYPE].tok, q->qrrtype, toks[RRTYPE].tok_len )
                    != 0 ) ) {
      continue;
    }

    memset( th->scrtch_key, '\0', ENGINE_THREAD_SCRTCH_SZ );
    ( void )snprintf(
        th->scrtch_key,
        ENGINE_THREAD_SCRTCH_SZ,
        "o\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s",
        toks[RRNAME].tok_len,
        toks[RRNAME].tok,
        toks[SENSORID].tok_len,
        toks[SENSORID].tok,
        toks[RRTYPE].tok_len,
        toks[RRTYPE].tok,
        toks[RDATA].tok_len,
        toks[RDATA].tok );

    X( prnl( "full key `%s`", th->scrtch_key ) );

    size_t fullkey_len = strlen( th->scrtch_key );
    size_t val_size = 0;
    char* val = rocksdb_get(
        db->db, db->readoptions, th->scrtch_key, fullkey_len, &val_size, &err );
    if( val == NULL || err != NULL ) {
      X( prnl( "rocksdb_get() observation not found" ) );
      continue;
    }

    value_t v;
    int ret = blb_rocksdb_val_decode( &v, val, val_size );
    if( ret != 0 ) {
      X( prnl( "unable to decode observation value; skipping entry" ) );
      free( val );
      continue;
    }
    free( val );

    keys_hit += 1;
    protocol_entry_t __e, *e = &__e;
    e->sensorid = toks[SENSORID].tok;
    e->sensorid_len = toks[SENSORID].tok_len;
    e->rdata = toks[RDATA].tok;
    e->rdata_len = toks[RDATA].tok_len;
    e->rrname = toks[RRNAME].tok;
    e->rrname_len = toks[RRNAME].tok_len;
    e->rrtype = toks[RRTYPE].tok;
    e->rrtype_len = toks[RRTYPE].tok_len;
    e->count = v.count;
    e->first_seen = v.first_seen;
    e->last_seen = v.last_seen;
    int push_ok = blb_thread_query_stream_push_response( th, e );
    if( push_ok != 0 ) {
      X( prnl( "unable to push query response entry" ) );
      goto stream_error;
    }
  }
  rocksdb_iter_destroy( it );
  ( void )blb_thread_query_stream_end_response( th );
  return ( 0 );

stream_error:
  rocksdb_iter_destroy( it );
  return ( -1 );
}

static int blb_rocksdb_query(
    thread_t* th, const protocol_query_request_t* q ) {
  int rc = -1;
  if( q->qrrname_len > 0 ) {
    rc = blb_rocksdb_query_by_o( th, q );
  } else {
    rc = blb_rocksdb_query_by_i( th, q );
  }
  return ( rc );
}

static void blb_rocksdb_backup(
    thread_t* th, const protocol_backup_request_t* b ) {
  ASSERT( th->db->dbi == &blb_rocksdb_dbi );
  blb_rocksdb_t* db = ( blb_rocksdb_t* )th->db;

  X( prnl( "backup `%.*s`", ( int )b->path_len, b->path ) );

  if( b->path_len >= 256 ) {
    L( prnl( "invalid path" ) );
    return;
  }

  char path[256];
  snprintf( path, sizeof( path ), "%.*s", ( int )b->path_len, b->path );

  char* err = NULL;
  rocksdb_backup_engine_t* be =
      rocksdb_backup_engine_open( db->options, path, &err );
  if( err != NULL ) {
    L( prnl( "rocksdb_backup_engine_open() failed `%s`", err ) );
    free( err );
    return;
  }

  rocksdb_backup_engine_create_new_backup( be, db->db, &err );
  if( err != NULL ) {
    L( prnl( "rocksdb_backup_engine_create_new_backup() failed `%s`", err ) );
    free( err );
    rocksdb_backup_engine_close( be );
    return;
  }
}

static void blb_rocksdb_dump( thread_t* th, const protocol_dump_request_t* d ) {
  ASSERT( th->db->dbi == &blb_rocksdb_dbi );
  blb_rocksdb_t* db = ( blb_rocksdb_t* )th->db;

  X( prnl( "dump `%.*s`", ( int )d->path_len, d->path ) );

  uint64_t cnt = 0;
  rocksdb_iterator_t* it = rocksdb_create_iterator( db->db, db->readoptions );
  // rocksdb_iter_seek_to_first(it);
  rocksdb_iter_seek( it, "o", 1 );
  for( ; rocksdb_iter_valid( it ) != ( unsigned char )0;
       rocksdb_iter_next( it ) ) {
    size_t key_len = 0;
    const char* key = rocksdb_iter_key( it, &key_len );
    if( key == NULL ) {
      L( prnl( "impossible: unable to extract key from rocksdb iterator" ) );
      break;
    }

    if( key[0] == 'i' ) { continue; }

    enum TokIdx { RRNAME = 0, SENSORID = 1, RRTYPE = 2, RDATA = 3, FIELDS = 4 };

    struct Tok {
      const char* tok;
      int tok_len;
    };

    struct Tok toks[FIELDS] = {NULL};

    enum TokIdx j = RRNAME;
    size_t last = 1;
    for( size_t i = 2; i < key_len; i++ ) {
      if( key[i] == '\x1f' ) {
        // we fixup the RDATA and skip extra \x1f's
        if( j < RDATA ) {
          toks[j].tok = &key[last + 1];
          toks[j].tok_len = i - last - 1;
          last = i;
          j++;
        }
      }
    }
    toks[RDATA].tok = &key[last + 1];
    toks[RDATA].tok_len = key_len - last - 1;

    X(
        out( "o %.*s %.*s %.*s %.*s\n",
             toks[RRNAME].tok_len,
             toks[RRNAME].tok,
             toks[SENSORID].tok_len,
             toks[SENSORID].tok,
             toks[RRTYPE].tok_len,
             toks[RRTYPE].tok,
             toks[RDATA].tok_len,
             toks[RDATA].tok ) );

    size_t val_size = 0;
    value_t v;
    const char* val = rocksdb_iter_value( it, &val_size );
    int ret = blb_rocksdb_val_decode( &v, val, val_size );
    if( ret != 0 ) {
      X( prnl( "unable to decode observation value; skipping entry" ) );
      continue;
    }

    cnt += 1;
    protocol_entry_t __e, *e = &__e;
    e->sensorid = toks[SENSORID].tok;
    e->sensorid_len = toks[SENSORID].tok_len;
    e->rdata = toks[RDATA].tok;
    e->rdata_len = toks[RDATA].tok_len;
    e->rrname = toks[RRNAME].tok;
    e->rrname_len = toks[RRNAME].tok_len;
    e->rrtype = toks[RRTYPE].tok;
    e->rrtype_len = toks[RRTYPE].tok_len;
    e->count = v.count;
    e->first_seen = v.first_seen;
    e->last_seen = v.last_seen;

    int rc = blb_thread_dump_entry( th, e );
    if( rc != 0 ) {
      L( prnl( "unable to dump entry" ) );
      break;
    }
  }

  char* err = NULL;
  rocksdb_iter_get_error( it, &err );
  if( err != NULL ) { L( prnl( "iterator error `%s`", err ) ); }
  rocksdb_iter_destroy( it );
  L( prnl( "dumped `%" PRIu64 "` entries", cnt ) );
}

static int blb_rocksdb_input(
    thread_t* th, const protocol_input_request_t* i ) {
  ASSERT( th->db->dbi == &blb_rocksdb_dbi );
  blb_rocksdb_t* db = ( blb_rocksdb_t* )th->db;

  X( prnl(
      "put `%.*s` `%.*s` `%.*s` `%.*s` %d",
      ( int )i->entry.rdata_len,
      i->entry.rdata,
      ( int )i->entry.rrname_len,
      i->entry.rrname,
      ( int )i->entry.rrtype_len,
      i->entry.rrtype,
      ( int )i->entry.sensorid_len,
      i->entry.sensorid,
      i->entry.count ) );

  value_t v = {.count = i->entry.count,
               .first_seen = i->entry.first_seen,
               .last_seen = i->entry.last_seen};
  char val[sizeof( uint32_t ) * 3];
  size_t val_len = sizeof( val );
  ( void )blb_rocksdb_val_encode( &v, val, val_len );

  ( void )snprintf(
      th->scrtch_key,
      ENGINE_THREAD_SCRTCH_SZ,
      "o\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s",
      ( int )i->entry.rrname_len,
      i->entry.rrname,
      ( int )i->entry.sensorid_len,
      i->entry.sensorid,
      ( int )i->entry.rrtype_len,
      i->entry.rrtype,
      ( int )i->entry.rdata_len,
      i->entry.rdata );

  ( void )snprintf(
      th->scrtch_inv,
      ENGINE_THREAD_SCRTCH_SZ,
      "i\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s",
      ( int )i->entry.rdata_len,
      i->entry.rdata,
      ( int )i->entry.sensorid_len,
      i->entry.sensorid,
      ( int )i->entry.rrname_len,
      i->entry.rrname,
      ( int )i->entry.rrtype_len,
      i->entry.rrtype );

  char* err = NULL;
  rocksdb_merge(
      db->db,
      db->writeoptions,
      th->scrtch_key,
      strlen( th->scrtch_key ),
      val,
      val_len,
      &err );
  if( err != NULL ) {
    V( prnl( "rocksdb_merge() failed: `%s`", err ) );
    free( err );
    return ( -1 );
  }

  // XXX: put vs merge
  rocksdb_put(
      db->db,
      db->writeoptions,
      th->scrtch_inv,
      strlen( th->scrtch_inv ),
      "",
      0,
      &err );
  if( err != NULL ) {
    V( prnl( "rocksdb_put() failed: `%s`", err ) );
    free( err );
    return ( -1 );
  }

  return ( 0 );
}

rocksdb_t* blb_rocksdb_handle( db_t* _db ) {
  ASSERT( _db->dbi == &blb_rocksdb_dbi );
  blb_rocksdb_t* db = ( blb_rocksdb_t* )_db;
  return ( db->db );
}

db_t* blb_rocksdb_open( const blb_rocksdb_config_t* c ) {
  V( prnl( "rocksdb database at `%s`", c->path ) );
  V( prnl(
      "parallelism `%d` membudget `%zu` max_log_file_size `%zu` "
      "keep_log_file_num `%d`",
      c->parallelism,
      c->membudget,
      c->max_log_file_size,
      c->keep_log_file_num ) );

  blb_rocksdb_t* db = blb_new( blb_rocksdb_t );
  if( db == NULL ) { return ( NULL ); }
  db->dbi = &blb_rocksdb_dbi;
  char* err = NULL;
  int level_compression[5] = {rocksdb_lz4_compression,
                              rocksdb_lz4_compression,
                              rocksdb_lz4_compression,
                              rocksdb_lz4_compression,
                              rocksdb_lz4_compression};

  db->mergeop = blb_rocksdb_mergeoperator_create();
  db->options = rocksdb_options_create();
  db->writeoptions = rocksdb_writeoptions_create();
  db->readoptions = rocksdb_readoptions_create();

  rocksdb_options_increase_parallelism( db->options, c->parallelism );
  rocksdb_options_optimize_level_style_compaction( db->options, c->membudget );
  rocksdb_options_set_create_if_missing( db->options, 1 );
  rocksdb_options_set_max_log_file_size( db->options, c->max_log_file_size );
  rocksdb_options_set_keep_log_file_num( db->options, c->keep_log_file_num );
  rocksdb_options_set_max_open_files( db->options, c->max_open_files );
  rocksdb_options_set_merge_operator( db->options, db->mergeop );
  rocksdb_options_set_compression_per_level(
      db->options, level_compression, 5 );

  db->db = rocksdb_open( db->options, c->path, &err );
  if( err != NULL ) {
    V( prnl( "rocksdb_open() failed: `%s`", err ) );
    rocksdb_options_destroy( db->options );
    rocksdb_mergeoperator_destroy( db->mergeop );
    rocksdb_writeoptions_destroy( db->writeoptions );
    rocksdb_readoptions_destroy( db->readoptions );
    free( err );
    blb_free( db );
    return ( NULL );
  }

  V( prnl( "rocksdb at %p", db ) );

  return ( ( db_t* )db );
}
