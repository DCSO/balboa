// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <engine.h>
#include <ketopt.h>
#include <mock-impl.h>
#include <trace.h>
#include <unistd.h>

int main(int argc, char** argv) {
  int daemonize = 0;
  engine_config_t engine_config = blb_engine_server_config_init();
  trace_config_t trace_config = {.stream = stderr,
                                 .host = "pdns",
                                 .app = argv[0],
                                 // leaking process number ...
                                 .procid = getpid(),
                                 .verbosity = 0};
  ketopt_t opt = KETOPT_INIT;
  int c;
  while((c = ketopt(&opt, argc, argv, 1, "j:l:p:vDSR", NULL)) >= 0) {
    switch(c) {
    case 'D': daemonize = 1; break;
    case 'l': engine_config.host = opt.arg; break;
    case 'p': engine_config.port = atoi(opt.arg); break;
    case 'v': trace_config.verbosity += 1; break;
    case 'j': engine_config.conn_throttle_limit = atoi(opt.arg); break;
    case 'S': engine_config.enable_signal_consumer = false; break;
    case 'R': engine_config.enable_stats_reporter = false; break;
    default: break;
    }
  }

  theTrace_stream_use(&trace_config);
  if(daemonize
     && (trace_config.stream == stderr || trace_config.stream == stdout)) {
    theTrace_set_verbosity(0);
  }

  db_t* db = blb_mock_open();
  if(db == NULL) { return (1); }

  engine_config.db = db;
  engine_t* e = blb_engine_server_new(&engine_config);
  if(e == NULL) {
    L(log_error("unable to create engine"));
    blb_dbi_teardown(db);
    return (1);
  }

  blb_engine_run(e);

  blb_dbi_teardown(db);

  blb_engine_teardown(e);

  return (0);
}