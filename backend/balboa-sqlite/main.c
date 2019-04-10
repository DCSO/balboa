// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <engine.h>
#include <ketopt.h>
#include <trace.h>
#include <unistd.h>

#include <sqlite-impl.h>

__attribute__( ( noreturn ) ) void version( void ) {
  fprintf( stderr, "balboa-sqlite v2.0.0\n" );
  exit( 1 );
}

__attribute__( ( noreturn ) ) void usage( const blb_sqlite_config_t* c ) {
  fprintf(
      stderr,
      "\
`balboa-sqlite` provides a pdns database backend for `balboa`\n\
\n\
Usage: balboa-sqlite [options]\n\
\n\
    -h display help\n\
    -D daemonize (default: off)\n\
    -d <path> path to sqlite database (default: `%s`)\n\
    -l listen address (default: 127.0.0.1)\n\
    -p listen port (default: 4242)\n\
    -v increase verbosity; can be passed multiple times\n\
    -j connection throttle limit, maximum concurrent connections (default: 64)\n\
    -n <database-name> name of the database (default: %s)\n\
    --database_path <path> same as `-d`\n\
    --database_name <name> same as `-n`\n\
    --journal_mode <mode> journale mode to use. one of {wal,memory} (default: %s)\n\
\n",
      c->path,
      c->name,
      c->journal_mode );
  exit( 1 );
}

int main( int argc, char** argv ) {
  int verbosity = 0;
  int daemonize = 0;
  char* host = "127.0.0.1";
  int port = 4242;
  int conn_throttle_limit = 64;
  blb_sqlite_config_t config = blb_sqlite_config_init();
  trace_config_t trace_config = {.stream = stderr,
                                 .host = "pdns",
                                 .app = "balboa-sqlite",
                                 // leaking process number ...
                                 .procid = getpid()};

  ketopt_t opt = KETOPT_INIT;
  static ko_longopt_t opts[] = {{"database_path", ko_required_argument, 301},
                                {"database_name", ko_required_argument, 302},
                                {"journal_mode", ko_required_argument, 303},
                                {"version", ko_no_argument, 999},
                                {NULL, 0, 0}};

  int c;
  while( ( c = ketopt( &opt, argc, argv, 1, "j:d:n:l:p:vDh", opts ) ) >= 0 ) {
    switch( c ) {
    case 'D': daemonize = 1; break;
    case 'd': config.path = opt.arg; break;
    case 'n': config.name = opt.arg; break;
    case 'l': host = opt.arg; break;
    case 'p': port = atoi( opt.arg ); break;
    case 'v': verbosity += 1; break;
    case 'j': conn_throttle_limit = atoi( opt.arg ); break;
    case 'h': usage( &config );
    case 301: config.path = opt.arg; break;
    case 302: config.name = opt.arg; break;
    case 303: config.journal_mode = opt.arg; break;
    case 999: version();
    default: usage( &config );
    }
  }

  theTrace_stream_use( &trace_config );
  if( daemonize ) {
    theTrace_set_verbosity( 0 );
  } else {
    theTrace_set_verbosity( verbosity );
  }

  blb_engine_signals_init();

  db_t* db = blb_sqlite_open( &config );
  if( db == NULL ) {
    L( log_error( "unable to open sqlite database at `%s`", config.path ) );
    return ( 1 );
  }

  engine_t* e = blb_engine_new( db, host, port, conn_throttle_limit );
  if( e == NULL ) {
    L( log_error( "unable to create io engine" ) );
    blb_dbi_teardown( db );
    return ( 1 );
  }

  blb_engine_run( e );

  blb_engine_teardown( e );

  blb_dbi_teardown( db );

  return ( 0 );
}