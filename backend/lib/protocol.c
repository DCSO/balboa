
#include <mpack.h>
#include <protocol.h>
#include <trace.h>

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
