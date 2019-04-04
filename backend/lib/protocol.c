
#include <protocol.h>
#include <trace.h>
#include <mpack.h>

struct protocol_stream_t {
  mpack_tree_t tree;
  void* usr;
  ssize_t ( *read_cb )( void* usr, char* p, size_t p_sz );
};

static ssize_t blb_protocol_encode_outer_request(
    int type, char* p, size_t p_sz, size_t used_inner ) {
  ASSERT( used_inner < p_sz );
  mpack_writer_t __wr = {0}, *wr = &__wr;

  // encode outer message
  mpack_writer_init( wr, p + used_inner, p_sz - used_inner );
  mpack_start_map( wr, 2 );
  mpack_write_cstr( wr, PROTOCOL_TYPED_MESSAGE_TYPE_KEY );
  mpack_write_int( wr, type );
  mpack_write_cstr( wr, PROTOCOL_TYPED_MESSAGE_ENCODED_KEY );
  mpack_write_bin( wr, p, used_inner );
  mpack_finish_map( wr );
  mpack_error_t outer_err = mpack_writer_error( wr );
  if( outer_err != mpack_ok ) {
    X( prnl( "encoding outer msgpack data failed `%d`", outer_err ) );
    mpack_writer_destroy( wr );
    return ( -1 );
  }

  size_t used_outer = mpack_writer_buffer_used( wr );
  X( prnl( "encoded outer message size `%zu`", used_outer ) );
  mpack_writer_destroy( wr );
  ASSERT( ( used_inner + used_outer ) < p_sz );
  memmove( p, p + used_inner, used_outer );
  return ( used_outer );
}

ssize_t blb_protocol_encode_dump_request(
    const protocol_dump_request_t* r, char* p, size_t p_sz ) {
  mpack_writer_t __wr = {0}, *wr = &__wr;

  // encode inner message
  mpack_writer_init( wr, p, p_sz );
  mpack_start_map( wr, 1 );
  mpack_write_cstr( wr, PROTOCOL_DUMP_REQUEST_PATH_KEY );
  mpack_write_str( wr, r->path, r->path_len );
  mpack_finish_map( wr );
  mpack_error_t err = mpack_writer_error( wr );
  if( err != mpack_ok ) {
    L( prnl( "encoding inner msgpack data failed `%d`", err ) );
    mpack_writer_destroy( wr );
    return ( -1 );
  }

  size_t used_inner = mpack_writer_buffer_used( wr );
  X( prnl( "encoded inner message size `%zu`", used_inner ) );
  ASSERT( used_inner < p_sz );
  mpack_writer_destroy( wr );

  return ( blb_protocol_encode_outer_request(
      PROTOCOL_DUMP_REQUEST, p, p_sz, used_inner ) );
}

ssize_t blb_protocol_encode_backup_request(
    const protocol_backup_request_t* r, char* p, size_t p_sz ) {
  mpack_writer_t __wr = {0}, *wr = &__wr;

  // encode inner message
  mpack_writer_init( wr, p, p_sz );
  mpack_start_map( wr, 1 );
  mpack_write_cstr( wr, PROTOCOL_BACKUP_REQUEST_PATH_KEY );
  mpack_write_str( wr, r->path, r->path_len );
  mpack_finish_map( wr );
  mpack_error_t err = mpack_writer_error( wr );
  if( err != mpack_ok ) {
    L( prnl( "encoding inner msgpack data failed `%d`", err ) );
    mpack_writer_destroy( wr );
    return ( -1 );
  }

  size_t used_inner = mpack_writer_buffer_used( wr );
  X( prnl( "encoded inner message size `%zu`", used_inner ) );
  ASSERT( used_inner < p_sz );
  mpack_writer_destroy( wr );

  return ( blb_protocol_encode_outer_request(
      PROTOCOL_BACKUP_REQUEST, p, p_sz, used_inner ) );
}

ssize_t blb_protocol_encode_dump_entry(
    const protocol_entry_t* entry, char* p, size_t p_sz ) {
  mpack_writer_t __wr = {0}, *wr = &__wr;
  mpack_writer_init( wr, p, p_sz );

  mpack_start_map( wr, OBS_FIELDS );
  mpack_write_uint( wr, OBS_RRNAME_IDX );
  mpack_write_bin( wr, entry->rrname, entry->rrname_len );
  mpack_write_uint( wr, OBS_RRTYPE_IDX );
  mpack_write_bin( wr, entry->rrtype, entry->rrtype_len );
  mpack_write_uint( wr, OBS_RDATA_IDX );
  mpack_write_bin( wr, entry->rdata, entry->rdata_len );
  mpack_write_uint( wr, OBS_SENSOR_IDX );
  mpack_write_bin( wr, entry->sensorid, entry->sensorid_len );
  mpack_write_uint( wr, OBS_COUNT_IDX );
  mpack_write_uint( wr, entry->count );
  mpack_write_uint( wr, OBS_FIRST_SEEN_IDX );
  mpack_write_uint( wr, entry->first_seen );
  mpack_write_uint( wr, OBS_LAST_SEEN_IDX );
  mpack_write_uint( wr, entry->last_seen );
  mpack_finish_map( wr );

  mpack_error_t err = mpack_writer_error( wr );
  if( err != mpack_ok ) {
    L( prnl( "encoding dump entry failed with mpack_error_t `%d`", err ) );
    mpack_writer_destroy( wr );
    return ( -1 );
  }

  size_t used = mpack_writer_buffer_used( wr );
  X( prnl( "encoded dump entry size `%zu`", used ) );

  ASSERT( used < p_sz );

  mpack_writer_destroy( wr );

  return ( used );
}

ssize_t blb_protocol_encode_stream_start_response( char* p, size_t p_sz ) {
  return ( blb_protocol_encode_outer_request(
      PROTOCOL_QUERY_STREAM_START_RESPONSE, p, p_sz, 0 ) );
}

ssize_t blb_protocol_encode_stream_end_response( char* p, size_t p_sz ) {
  return ( blb_protocol_encode_outer_request(
      PROTOCOL_QUERY_STREAM_END_RESPONSE, p, p_sz, 0 ) );
}

ssize_t blb_protocol_encode_stream_entry(
    const protocol_entry_t* entry, char* p, size_t p_sz ) {
  mpack_writer_t __wr = {0}, *wr = &__wr;
  mpack_writer_init( wr, p, p_sz );

  mpack_start_map( wr, 7 );
  mpack_write_cstr( wr, PROTOCOL_PDNS_ENTRY_COUNT_KEY );
  mpack_write_uint( wr, entry->count );
  mpack_write_cstr( wr, PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY );
  // mpack_write_timestamp_seconds(wr,entry->first_seen);
  mpack_write_uint( wr, entry->first_seen );
  mpack_write_cstr( wr, PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY );
  // mpack_write_timestamp_seconds(wr,entry->last_seen);
  mpack_write_uint( wr, entry->last_seen );
  mpack_write_cstr( wr, PROTOCOL_PDNS_ENTRY_RDATA_KEY );
  mpack_write_str( wr, entry->rdata, entry->rdata_len );
  mpack_write_cstr( wr, PROTOCOL_PDNS_ENTRY_RRNAME_KEY );
  mpack_write_str( wr, entry->rrname, entry->rrname_len );
  mpack_write_cstr( wr, PROTOCOL_PDNS_ENTRY_RRTYPE_KEY );
  mpack_write_str( wr, entry->rrtype, entry->rrtype_len );
  mpack_write_cstr( wr, PROTOCOL_PDNS_ENTRY_SENSORID_KEY );
  mpack_write_str( wr, entry->sensorid, entry->sensorid_len );
  mpack_finish_map( wr );

  size_t used_inner = mpack_writer_buffer_used( wr );

  mpack_error_t err = mpack_writer_error( wr );
  if( err != mpack_ok ) {
    L( prnl( "encoding stream entry failed with mpack_error_t `%d`", err ) );
    mpack_writer_destroy( wr );
    return ( -1 );
  }

  mpack_writer_destroy( wr );

  return ( blb_protocol_encode_outer_request(
      PROTOCOL_BACKUP_REQUEST, p, p_sz, used_inner ) );
}

#define PROTOCOL_POLL_READ_TIMEOUT ( 60 )

static size_t blb_protocol_stream_cb( mpack_tree_t* tree, char* p, size_t p_sz ) {
  protocol_stream_t* s = mpack_tree_context( tree );

  ssize_t rc = s->read_cb( s->usr, p, p_sz );
  if( rc < 0 ) {
    V( prnl( "read() failed: `%s`", strerror( errno ) ) );
    mpack_tree_flag_error( tree, mpack_error_io );
  } else if( rc == 0 ) {
    X( prnl( "read() eof" ) );
    mpack_tree_flag_error( tree, mpack_error_eof );
  }
  return ( rc );
}

protocol_stream_t* blb_protocol_stream_new(
    void* usr,
    ssize_t ( *read_cb )( void* usr, char* p, size_t p_sz ),
    size_t max_sz,
    size_t max_nodes ) {
  protocol_stream_t* s = blb_new( protocol_stream_t );
  if( s == NULL ) {
    L( prnl( "blb_new() failed" ) );
    return ( NULL );
  }
  s->read_cb = read_cb;
  s->usr = usr;
  mpack_tree_init_stream(
      &s->tree, blb_protocol_stream_cb, s, max_sz, max_nodes );
  return ( s );
}