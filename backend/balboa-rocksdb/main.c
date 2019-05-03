// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <engine.h>
#include <ketopt.h>
#include <rocksdb-impl.h>
#include <trace.h>
#include <unistd.h>

__attribute__((noreturn)) void version(void) {
  fprintf(stderr, "balboa-rocksdb v2.0.0\n");
  exit(1);
}

__attribute__((noreturn)) void usage(const blb_rocksdb_config_t* c) {
  fprintf(
      stderr,
      "\
`balboa-rocksdb` provides a pdns database backend for `balboa`\n\
\n\
Usage: balboa-rocksdb [options]\n\
\n\
    -h display help\n\
    -D daemonize (default: off)\n\
    -d <path> path to rocksdb database (default: `%s`)\n\
    -l listen address (default: 127.0.0.1)\n\
    -p listen port (default: 4242)\n\
    -v increase verbosity; can be passed multiple times\n\
    -S disable signal handling\n\
    -R disable engine stats reporter\n\
    -j connection throttle limit, maximum concurrent connections (default: 64)\n\
    --membudget <memory-in-bytes> rocksdb membudget option (value: %" PRIu64
      ")\n\
    --parallelism <number-of-threads> rocksdb parallelism option (value: %d)\n\
    --max_log_file_size <size> rocksdb log file size option (value: %" PRIu64
      ")\n\
    --max_open_files <number> rocksdb max number of open files (value: %d)\n\
    --keep_log_file_num <number> rocksdb max number of log files (value: %d)\n\
    --database_path <path> same as `-d`\n\
    --version show version then exit\n\
\n",
      c->path,
      c->membudget,
      c->parallelism,
      c->max_log_file_size,
      c->max_open_files,
      c->keep_log_file_num);
  exit(1);
}

int main(int argc, char** argv) {
  int daemonize = 0;
  blb_rocksdb_config_t rocksdb_config = blb_rocksdb_config_init();
  engine_config_t engine_config = blb_engine_server_config_init();
  trace_config_t trace_config = {.stream = stderr,
                                 .host = "pdns",
                                 .app = "balboa-rocksdb",
                                 // leaking process number ...
                                 .procid = getpid(),
                                 .verbosity = 0};
  ketopt_t opt = KETOPT_INIT;
  static ko_longopt_t opts[] = {
      {"membudget", ko_required_argument, 301},
      {"parallelism", ko_required_argument, 302},
      {"max_log_file_size", ko_required_argument, 303},
      {"max_open_files", ko_required_argument, 304},
      {"keep_log_file_num", ko_required_argument, 305},
      {"database_path", ko_required_argument, 306},
      {"version", ko_no_argument, 307},
      {NULL, 0, 0}};
  int c;
  while((c = ketopt(&opt, argc, argv, 1, "j:d:l:p:vDSRh", opts)) >= 0) {
    switch(c) {
    case 'D': daemonize = 1; break;
    case 'd': rocksdb_config.path = opt.arg; break;
    case 'l': engine_config.host = opt.arg; break;
    case 'p': engine_config.port = atoi(opt.arg); break;
    case 'v': trace_config.verbosity += 1; break;
    case 'j': engine_config.conn_throttle_limit = atoi(opt.arg); break;
    case 'S': engine_config.enable_signal_consumer = false; break;
    case 'R': engine_config.enable_stats_reporter = false; break;
    case 'h': usage(&rocksdb_config);
    case 301: rocksdb_config.membudget = atoll(opt.arg); break;
    case 302: rocksdb_config.parallelism = atoi(opt.arg); break;
    case 303: rocksdb_config.max_log_file_size = atoi(opt.arg); break;
    case 304: rocksdb_config.max_open_files = atoi(opt.arg); break;
    case 305: rocksdb_config.keep_log_file_num = atoi(opt.arg); break;
    case 306: rocksdb_config.path = opt.arg; break;
    case 307: version();
    default: usage(&rocksdb_config);
    }
  }

  theTrace_stream_use(&trace_config);
  if(daemonize
     && (trace_config.stream == stderr || trace_config.stream == stdout)) {
    theTrace_set_verbosity(0);
  }

  db_t* db = blb_rocksdb_open(&rocksdb_config);
  if(db == NULL) {
    L(log_error("unable to open rocksdb at path `%s`", rocksdb_config.path));
    return (1);
  }

  engine_config.db = db;
  engine_t* e = blb_engine_server_new(&engine_config);
  if(e == NULL) {
    L(log_error("unable to create engine"));
    blb_dbi_teardown(db);
    return (1);
  }

  blb_engine_run(e);

  blb_engine_teardown(e);

  blb_dbi_teardown(db);

  return (0);
}