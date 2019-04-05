// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <engine.h>
#include <protocol.h>
#include <trace.h>

#define ENGINE_MAX_MESSAGE_SZ ENGINE_THREAD_SCRTCH_SZ
#define ENGINE_MAX_MESSAGE_NODES ( 1024 )
#define ENGINE_POLL_READ_TIMEOUT ( 60 )
#define ENGINE_POLL_WRITE_TIMEOUT ( 10 )

static atomic_int blb_engine_stop = ATOMIC_VAR_INIT( 0 );
static atomic_int blb_thread_cnt = ATOMIC_VAR_INIT( 0 );

static void blb_engine_request_stop() {
  atomic_fetch_add( &blb_engine_stop, 1 );
}

static int blb_engine_poll_stop() {
  return ( atomic_load( &blb_engine_stop ) );
}

static void blb_thread_cnt_incr() {
  ( void )atomic_fetch_add( &blb_thread_cnt, 1 );
}

static void blb_thread_cnt_decr() {
  ( void )atomic_fetch_sub( &blb_thread_cnt, 1 );
}

static int blb_thread_cnt_get() {
  return ( atomic_load( &blb_thread_cnt ) );
}

static int blb_thread_write_all( thread_t* th, char* _p, size_t _p_sz ) {
  char* p = _p;
  ssize_t r = _p_sz;
  while( r > 0 ) {
    ssize_t rc = write( th->fd, p, r );
    if( rc < 0 ) {
      L( prnl( "write() failed error `%s`", strerror( errno ) ) );
      return ( -1 );
    } else if( rc == 0 && errno == EINTR ) {
      continue;
    }
    r -= rc;
    p += rc;
  }
  return ( 0 );
}

static inline int blb_engine_poll_write( int fd, int seconds ) {
  if( blb_engine_poll_stop() > 0 ) {
    V( prnl( "engine stop detected" ) );
    return ( -1 );
  }
  fd_set fds;
  struct timeval to;
  FD_ZERO( &fds );
  FD_SET( fd, &fds );
  to.tv_sec = seconds;
  to.tv_usec = 0;
  int rc = select( fd + 1, NULL, &fds, NULL, &to );
  if( rc == 0 ) {
    X( prnl( "select() timeout" ) );
    return ( -1 );
  } else if( rc < 0 ) {
    X( prnl( "select() failed `%s`", strerror( errno ) ) );
    return ( -1 );
  }
  return ( 0 );
}

static inline int blb_engine_poll_read( int fd, int seconds ) {
timeout_retry:
  if( blb_engine_poll_stop() > 0 ) {
    V( prnl( "engine stop detected" ) );
    return ( -1 );
  }
  fd_set fds;
  struct timeval to;
  FD_ZERO( &fds );
  FD_SET( fd, &fds );
  to.tv_sec = seconds;
  to.tv_usec = 0;
  int rc = select( fd + 1, &fds, NULL, NULL, &to );
  if( rc == 0 ) {
    X( prnl( "select() timeout" ) );
    goto timeout_retry;
  } else if( rc < 0 ) {
    X( prnl( "select() failed `%s`", strerror( errno ) ) );
    return ( -1 );
  }
  return ( 0 );
}

int blb_thread_query_stream_start_response( thread_t* th ) {
  if( blb_engine_poll_stop() > 0 ) {
    L( prnl( "thread <%04lx> engine stop detected", th->thread ) );
    return ( -1 );
  }

  ssize_t used = blb_protocol_encode_stream_start_response(
      th->scrtch_response, ENGINE_THREAD_SCRTCH_SZ );
  if( used <= 0 ) {
    L( prnl( "blb_protocol_encode_stream_start_response() failed" ) );
    return ( -1 );
  }

  return ( blb_thread_write_all( th, th->scrtch_response, used ) );
}

int blb_thread_dump_entry( thread_t* th, const protocol_entry_t* entry ) {
  WHEN_X {
    theTrace_lock();
    prnl( "dump stream push entry" );
    prnl(
        "<%d `%.*s` `%.*s` `%.*s` `%.*s`>",
        entry->count,
        ( int )entry->rrname_len,
        entry->rrname,
        ( int )entry->rrtype_len,
        entry->rrtype,
        ( int )entry->rdata_len,
        entry->rdata,
        ( int )entry->sensorid_len,
        entry->sensorid );
    theTrace_release();
  }

  if( blb_engine_poll_stop() > 0 ) {
    L( prnl( "thread <%04lx> engine stop detected", th->thread ) );
    return ( -1 );
  }

  ssize_t used = blb_protocol_encode_dump_entry(
      entry, th->scrtch_response, ENGINE_THREAD_SCRTCH_SZ );
  if( used <= 0 ) {
    L( prnl( "blb_protocol_encode_dump_entry() failed" ) );
    return ( -1 );
  }

  return ( blb_thread_write_all( th, th->scrtch_response, used ) );
}

int blb_thread_query_stream_push_response(
    thread_t* th, const protocol_entry_t* entry ) {
  WHEN_T {
    theTrace_lock();
    prnl( "query stream push entry" );
    prnl(
        "<%d `%.*s` `%.*s` `%.*s` `%.*s`>",
        entry->count,
        ( int )entry->rrname_len,
        entry->rrname,
        ( int )entry->rrtype_len,
        entry->rrtype,
        ( int )entry->rdata_len,
        entry->rdata,
        ( int )entry->sensorid_len,
        entry->sensorid );
    theTrace_release();
  };

  if( blb_engine_poll_stop() > 0 ) {
    L( prnl( "thread <%04lx> engine stop detected", th->thread ) );
    return ( -1 );
  }

  ssize_t used = blb_protocol_encode_stream_entry(
      entry, th->scrtch_response, ENGINE_THREAD_SCRTCH_SZ );
  if( used <= 0 ) {
    L( prnl( "blb_protocol_encode_stream_entry() failed" ) );
    return ( -1 );
  }

  return ( blb_thread_write_all( th, th->scrtch_response, used ) );
}

int blb_thread_query_stream_end_response( thread_t* th ) {
  if( blb_engine_poll_stop() > 0 ) {
    L( prnl( "thread <%04lx> engine stop detected", th->thread ) );
    return ( -1 );
  }

  ssize_t used = blb_protocol_encode_stream_end_response(
      th->scrtch_response, ENGINE_THREAD_SCRTCH_SZ );
  if( used <= 0 ) {
    L( prnl( "blb_protocol_encode_stream_end_response() failed" ) );
    return ( -1 );
  }

  X( prnl( "blb_protocol_encode_stream_end_response() returned `%zd`", used ) );

  return ( blb_thread_write_all( th, th->scrtch_response, used ) );
}

static thread_t* blb_engine_thread_new( engine_t* e, int fd ) {
  thread_t* th = blb_new( thread_t );
  if( th == NULL ) { return ( NULL ); }
  th->usr_ctx = NULL;
  db_t* db = blb_dbi_thread_init( th, e->db );
  if( db == NULL ) {
    blb_free( th );
    return ( NULL );
  }
  th->db = db;
  th->engine = e;
  th->fd = fd;
  return ( th );
}

static void blb_engine_thread_teardown( thread_t* th ) {
  blb_dbi_thread_deinit( th, th->db );
  close( th->fd );
  blb_free( th );
}

static ssize_t blb_thread_read_stream_cb( void* usr, char* p, size_t p_sz ) {
  thread_t* th = usr;
  int fd_ok = blb_engine_poll_read( th->fd, ENGINE_POLL_READ_TIMEOUT );
  if( fd_ok != 0 ) {
    V( prnl( "engine poll read failed" ) );
    return ( 0 );
  }
  ssize_t rc = read( th->fd, p, p_sz );
  if( rc < 0 ) {
    V( prnl( "read() failed: `%s`", strerror( errno ) ) );
    return ( -1 );
  } else if( rc == 0 ) {
    X( prnl( "read() eof" ) );
    return ( 0 );
  }
  return ( rc );
}

static inline int blb_engine_thread_consume_backup(
    thread_t* th, const protocol_backup_request_t* backup ) {
  blb_dbi_backup( th, backup );
  return ( 0 );
}

static inline int blb_engine_thread_consume_dump(
    thread_t* th, const protocol_dump_request_t* dump ) {
  blb_dbi_dump( th, dump );
  return ( 0 );
}

static inline int blb_engine_thread_consume_query(
    thread_t* th, const protocol_query_request_t* query ) {
  int query_ok = blb_dbi_query( th, query );
  if( query_ok != 0 ) {
    L( prnl( "blb_dbi_query() failed" ) );
    return ( -1 );
  }
  return ( 0 );
}

static inline int blb_engine_thread_consume_input(
    thread_t* th, const protocol_input_request_t* input ) {
  int input_ok = blb_dbi_input( th, input );
  if( input_ok != 0 ) {
    L( prnl( "blb_dbi_input() failed" ) );
    return ( -1 );
  }
  return ( 0 );
}

static inline int blb_engine_thread_consume(
    thread_t* th, protocol_message_t* msg ) {
  switch( msg->ty ) {
  case PROTOCOL_INPUT_REQUEST:
    return ( blb_engine_thread_consume_input( th, &msg->u.input ) );
  case PROTOCOL_BACKUP_REQUEST:
    return ( blb_engine_thread_consume_backup( th, &msg->u.backup ) );
  case PROTOCOL_DUMP_REQUEST:
    return ( blb_engine_thread_consume_dump( th, &msg->u.dump ) );
  case PROTOCOL_QUERY_REQUEST:
    return ( blb_engine_thread_consume_query( th, &msg->u.query ) );
  default: L( prnl( "invalid message type `%d`", msg->ty ) ); return ( -1 );
  }
}

static void* blb_engine_thread_fn( void* usr ) {
  ASSERT( usr != NULL );
  thread_t* th = usr;
  blb_thread_cnt_incr();
  T( prnl( "new thread is <%04lx>", th->thread ) );

  protocol_stream_t* stream = blb_protocol_stream_new(
      th,
      blb_thread_read_stream_cb,
      ENGINE_MAX_MESSAGE_SZ,
      ENGINE_MAX_MESSAGE_NODES );
  if( stream == NULL ) {
    L( prnl( "unalbe to create protocol decode stream" ) );
    goto thread_exit;
  }

  protocol_message_t msg;
  while( 1 ) {
    if( blb_engine_poll_stop() > 0 ) {
      V( prnl( "thread <%04lx> engine stop detected", th->thread ) );
      goto thread_exit;
    }
    int rc = blb_protocol_stream_decode( stream, &msg );
    if( rc != 0 ) {
      L( prnl( "blb_protocol_stream_decode() failed" ) );
      goto thread_exit;
    }
    int th_rc = blb_engine_thread_consume( th, &msg );
    if( th_rc != 0 ) { goto thread_exit; }
  }

thread_exit:
  T( prnl( "finished thread is <%04lx>", th->thread ) );
  if( stream != NULL ) { blb_protocol_stream_teardown( stream ); }
  blb_thread_cnt_decr();
  blb_engine_thread_teardown( th );
  return ( NULL );
}

engine_t* blb_engine_new(
    db_t* db, const char* name, int port, int thread_throttle_limit ) {
  ASSERT( db != NULL );

  struct sockaddr_in __ipv4, *ipv4 = &__ipv4;
  int rc = inet_pton( AF_INET, name, &ipv4->sin_addr );
  ASSERT( rc >= 0 );
  if( rc != 1 ) {
    errno = EINVAL;
    return ( NULL );
  }
  ipv4->sin_family = AF_INET;
  ipv4->sin_port = htons( ( uint16_t )port );
  int fd = socket( ipv4->sin_family, SOCK_STREAM, 0 );
  if( fd < 0 ) { return ( NULL ); }
  int reuse = 1;
  ( void )setsockopt( fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof( reuse ) );
  int bind_rc = bind( fd, ipv4, sizeof( struct sockaddr_in ) );
  if( bind_rc < 0 ) {
    close( fd );
    return ( NULL );
  }
  int listen_rc = listen( fd, SOMAXCONN );
  if( listen_rc < 0 ) {
    close( fd );
    return ( NULL );
  }

  engine_t* e = blb_new( engine_t );
  if( e == NULL ) {
    close( fd );
    return ( NULL );
  }

  e->thread_throttle_limit = thread_throttle_limit;
  e->db = db;
  e->listen_fd = fd;

  V( prnl( "listening on host `%s` port `%d` fd `%d`", name, port, fd ) );

  return ( e );
}

static void* blb_engine_signal_consume( void* usr ) {
  ( void )usr;
  sigset_t s;
  sigemptyset( &s );
  sigaddset( &s, SIGQUIT );
  sigaddset( &s, SIGUSR1 );
  sigaddset( &s, SIGUSR2 );
  sigaddset( &s, SIGINT );
  sigaddset( &s, SIGPIPE );
  sigaddset( &s, SIGTERM );
  V( prnl( "signal consumer thread started" ) );
  while( 1 ) {
    int sig = 0;
    int rc = sigwait( &s, &sig );
    V( prnl( "sigwait() returned `%d` (signal `%d`)", rc, sig ) );
    if( rc != 0 ) { continue; }
    L( prnl( "received signal `%d`", sig ) );
    switch( sig ) {
    case SIGINT:
    case SIGTERM:
    case SIGQUIT:
      L( prnl( "requesting engine stop due to received signal" ) );
      blb_engine_request_stop();
      break;
    default: L( prnl( "ignoring signal" ) ); break;
    }
  }
  return ( NULL );
}

void blb_engine_signals_init( void ) {
  sigset_t s;
  sigemptyset( &s );
  sigaddset( &s, SIGQUIT );
  sigaddset( &s, SIGUSR1 );
  sigaddset( &s, SIGUSR2 );
  sigaddset( &s, SIGINT );
  sigaddset( &s, SIGPIPE );
  sigaddset( &s, SIGTERM );
  int rc = pthread_sigmask( SIG_BLOCK, &s, NULL );
  if( rc != 0 ) { L( prnl( "pthread_sigmask() failed `%d`", rc ) ); }

  pthread_t signal_consumer;
  pthread_create( &signal_consumer, NULL, blb_engine_signal_consume, NULL );
}

void blb_engine_run( engine_t* e ) {
  struct sockaddr_in __addr, *addr = &__addr;
  socklen_t addrlen = sizeof( struct sockaddr_in );

  pthread_attr_t __attr;
  pthread_attr_init( &__attr );
  pthread_attr_setdetachstate( &__attr, PTHREAD_CREATE_DETACHED );

  fd_set fds;
  struct timeval to;
  while( 1 ) {
  timeout_retry:
    if( blb_engine_poll_stop() > 0 ) {
      V( prnl( "engine stop detected" ) );
      goto wait;
    }
    if( blb_thread_cnt_get() >= e->thread_throttle_limit ) {
      sleep( 1 );
      V( prnl( "thread throttle reached" ) );
      goto timeout_retry;
    }
    FD_ZERO( &fds );
    FD_SET( e->listen_fd, &fds );
    to.tv_sec = 5;
    to.tv_usec = 0;
    int rc = select( e->listen_fd + 1, &fds, NULL, NULL, &to );
    if( rc == 0 ) {
      X( prnl( "select() timeout" ) );
      goto timeout_retry;
    } else if( rc < 0 ) {
      X( prnl( "select() failed `%s`", strerror( errno ) ) );
      goto wait;
    }
    socket_t fd = accept( e->listen_fd, ( struct sockaddr* )addr, &addrlen );
    if( fd < 0 ) {
      V( prnl( "accept() failed: `%s`; exiting", strerror( errno ) ) );
      blb_engine_request_stop();
      goto wait;
    }
    thread_t* th = blb_engine_thread_new( e, fd );
    if( th == NULL ) {
      V( prnl( "blb_engine_thread_new() failed; exiting" ) );
      blb_engine_request_stop();
      goto wait;
    }

    ( void )pthread_create(
        &th->thread, &__attr, blb_engine_thread_fn, ( void* )th );
  }

wait:

  pthread_attr_destroy( &__attr );

  while( blb_thread_cnt_get() > 0 ) {
    L( prnl( "waiting for `%d` threads to finish", blb_thread_cnt_get() ) );
    sleep( 1 );
  }

  L( prnl( "done" ) );
}

void blb_engine_teardown( engine_t* e ) {
  blb_free( e );
}
