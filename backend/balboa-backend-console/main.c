// balboa
// Copyright (c) 2019, DCSO GmbH

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <bs.h>
#include <engine.h>
#include <ketopt.h>
#include <protocol.h>
#include <trace.h>

typedef struct state_t state_t;
struct state_t {
  uint8_t* scrtch0;
  size_t scrtch0_sz;
  FILE* os;
  int sock;
  int (*dump_entry_cb)(state_t* state, protocol_entry_t* entry);
};

static int dump_state_init(state_t* state) {
  state->scrtch0_sz = 1024 * 1024 * 10;
  state->scrtch0 = malloc(state->scrtch0_sz);
  if(state->scrtch0 == NULL) { return (-1); }
  state->os = NULL;
  state->sock = -1;
  return (0);
}

static void dump_state_teardown(state_t* state) {
  free(state->scrtch0);
}

static ssize_t dump_process(state_t* state, FILE* is) {
  protocol_dump_stream_t* stream = blb_protocol_dump_stream_new(is);
  ssize_t entries = 0;
  while(1) {
    protocol_entry_t entry;
    int rc = blb_protocol_dump_stream_decode(stream, &entry);
    switch(rc) {
    case 0: {
      int rc = state->dump_entry_cb(state, &entry);
      if(rc != 0) {
        L(log_error("dump_entry_cb() failed with `%d`", rc));
        return (-entries);
      }
      entries++;
      continue;
    }
    case -1: return (entries);
    default:
      L(log_error("blb_dump_stream_decode() failed with `%d`", rc));
      blb_protocol_dump_stream_teardown(stream);
      return (-entries);
    }
  }
  blb_protocol_dump_stream_teardown(stream);
  return (entries);
}

static int dump(state_t* state, const char* dump_file) {
  ASSERT(state != NULL);
  ASSERT(state->dump_entry_cb != NULL);
  V(log_info("dump file is `%s`", dump_file));
  FILE* f = NULL;
  if(strcmp(dump_file, "-") == 0) {
    f = stdin;
  } else {
    f = fopen(dump_file, "rb");
  }
  if(f == NULL) {
    L(log_error("unable to open file `%s`", dump_file));
    return (-1);
  }
  ssize_t rc = dump_process(state, f);
  L(log_info("done; processed `%zd` entries", rc));
  dump_state_teardown(state);
  fclose(f);
  if(rc < 0) {
    return (-1);
  } else {
    return (0);
  }
}

static int dump_entry_json_cb(state_t* state, protocol_entry_t* entry) {
  assert(state->os != NULL);
  bytestring_sink_t __sink = bs_sink(state->scrtch0, state->scrtch0_sz);
  bytestring_sink_t* sink = &__sink;
  int ok = 0;
  char buf[64] = {0};
  ok += bs_cat(sink, "{\"rrname\":\"", 11);
  ok +=
      bs_append_escape(sink, (const uint8_t*)entry->rrname, entry->rrname_len);
  ok += bs_cat(sink, "\",\"rrtype\":\"", 12);
  ok +=
      bs_append_escape(sink, (const uint8_t*)entry->rrtype, entry->rrtype_len);
  ok += bs_cat(sink, "\",\"sensor_id\":\"", 15);
  ok += bs_append_escape(
      sink, (const uint8_t*)entry->sensorid, entry->sensorid_len);
  ok += bs_cat(sink, "\",\"rdata\":\"", 11);
  ok += bs_append_escape(sink, (const uint8_t*)entry->rdata, entry->rdata_len);
  ok += bs_cat(sink, "\",\"count\":", 10);
  snprintf(buf, 63, "%u", entry->count);
  ok += bs_cat(sink, buf, strlen(buf));
  ok += bs_cat(sink, ",\"first_seen\":", 14);
  snprintf(buf, 63, "%u", entry->first_seen);
  ok += bs_cat(sink, buf, strlen(buf));
  ok += bs_cat(sink, ",\"last_seen\":", 13);
  snprintf(buf, 63, "%u", entry->last_seen);
  ok += bs_cat(sink, buf, strlen(buf));
  ok += bs_append1(sink, '}');
  ok += bs_append1(sink, '\n');
  if(ok == 0) {
    fwrite(sink->p, sink->index, 1, state->os);
    return (0);
  } else {
    fputs("{\"error\":\"buffer-out-of-space\"}", state->os);
    return (-1);
  }
  return (0);
}

static int dump_entry_replay_cb(state_t* state, protocol_entry_t* entry) {
  ASSERT(state->sock != -1);

  protocol_input_request_t input = {.entry = *entry};
  ssize_t rc = blb_protocol_encode_input_request(
      &input, (char*)state->scrtch0, state->scrtch0_sz);
  if(rc <= 0) {
    L(log_error("unable to encode input request"));
    return (-1);
  }

  uint8_t* p = state->scrtch0;
  ssize_t r = rc;
  while(r > 0) {
    ssize_t rc = write(state->sock, p, r);
    if(rc < 0) {
      L(log_error("write() failed with `%s`", strerror(errno)));
      return (-1);
    } else if(rc == 0 && errno == EINTR) {
      continue;
    }
    r -= rc;
    p += rc;
  }
  return (0);
}

static int main_query(int argc, char** argv) {
  engine_config_t engine_config = blb_engine_client_config_init();
  trace_config_t trace_config = {.stream = stderr,
                                 .host = "pdns",
                                 .app = "balboa-backend-console",
                                 // leaking process number ...
                                 .procid = getpid()};
  protocol_query_request_t __query = {0}, *query = &__query;
  ketopt_t opt = KETOPT_INIT;
  int c;
  while((c = ketopt(&opt, argc, argv, 1, "h:p:r:d:s:l:vSR", NULL)) >= 0) {
    switch(c) {
    case 'v': trace_config.verbosity += 1; break;
    case 'h': engine_config.host = opt.arg; break;
    case 'p': engine_config.port = atoi(opt.arg); break;
    default: break;
    }
  }

  theTrace_stream_use(&trace_config);

  conn_t* conn = blb_engine_client_new(&engine_config);
  engine_t* engine = conn->engine;

  ssize_t used = blb_protocol_encode_query_request(
      query, conn->scrtch, ENGINE_CONN_SCRTCH_SZ);
  if(used <= 0) {
    L(log_error("unable to encode query"));
    blb_engine_teardown(engine);
    blb_engine_conn_teardown(conn);
    return (-1);
  }

  int rc = blb_conn_write_all(conn, conn->scrtch, used);
  if(rc != 0) {
    L(log_debug("blb_conn_write_all() failed"));
    blb_engine_teardown(engine);
    blb_engine_conn_teardown(conn);
    return (-1);
  }

  protocol_stream_t* stream = blb_engine_stream_new(conn);
  if(stream == NULL) {
    L(log_error("blb_engine_stream_new() failed"));
    blb_engine_teardown(engine);
    blb_engine_conn_teardown(conn);
    return (-1);
  }

  while(1) {
    protocol_message_t msg;
    int rc = blb_protocol_stream_decode(stream, &msg);
    if(rc < 0) {
      L(log_error("blb_protocol_stream_decode() failed"));
      blb_protocol_stream_teardown(stream);
      blb_engine_teardown(engine);
      blb_engine_conn_teardown(conn);
      return (-1);
    } else if(rc == 0) {
      break;
    }
  }

  blb_protocol_stream_teardown(stream);
  blb_engine_teardown(engine);
  blb_engine_conn_teardown(conn);
  return (0);
}

static int main_jsonize(int argc, char** argv) {
  const char* dump_file = "-";
  int verbosity = 0;
  trace_config_t trace_config = {.stream = stderr,
                                 .host = "pdns",
                                 .app = "balboa-backend-console",
                                 // leaking process number ...
                                 .procid = getpid()};

  ketopt_t opt = KETOPT_INIT;
  int c;
  while((c = ketopt(&opt, argc, argv, 1, "d:v", NULL)) >= 0) {
    switch(c) {
    case 'd': dump_file = opt.arg; break;
    case 'v': verbosity += 1; break;
    default: break;
    }
  }

  theTrace_stream_use(&trace_config);
  theTrace_set_verbosity(verbosity);

  V(log_info("dump file is `%s`", dump_file));

  state_t __state = {0}, *state = &__state;
  int state_ok = dump_state_init(state);
  if(state_ok != 0) {
    L(log_error("unable to initialize the dump state"));
    return (-1);
  }
  state->os = stdout;
  state->dump_entry_cb = dump_entry_json_cb;
  int rc = dump(state, dump_file);
  return (rc);
}

static int dump_connect(const char* host, const char* _port) {
  int port = atoi(_port);
  struct sockaddr_in addr;
  int addr_ok = inet_pton(AF_INET, host, &addr.sin_addr);
  if(addr_ok != 1) { return (-1); }
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  int fd = socket(addr.sin_family, SOCK_STREAM, 0);
  if(fd < 0) { return (-1); }
  int rc = connect(fd, &addr, sizeof(struct sockaddr_in));
  if(rc < 0) {
    close(fd);
    return (-1);
  }
  return (fd);
}

__attribute__((noreturn)) void version(void) {
  fprintf(stderr, "balboa-backend-console v2.0.0\n");
  exit(1);
}

__attribute__((noreturn)) void usage(void) {
  fprintf(
      stderr,
      "\
`balboa-backend-console` is a management tool for `balboa-backends`\n\
\n\
Usage: balboa-backend-console <--version|help|jsonize|dump|replay> [options]\n\
\n\
Command help:\n\
    show help\n\
\n\
Command jsonize:\n\
    read a dump file and print all entries as json\n\
\n\
    -d <path> path to the dump file to read\n\
\n\
Command dump:\n\
    connect to a `balboa-backend` and request a dump of all data to local stdout\n\
\n\
    -h <host> ip address of the `balboa-backend` (default: 127.0.0.1)\n\
    -p <port> port of the `balboa-backend` (default: 4242)\n\
    -v increase verbosity; can be passed multiple times\n\
    -d <remote-dump-path> unused/ignored (default: -)\n\
\n\
Command replay:\n\
    replay a previously generated database dump\n\
\n\
    -d <path> database dump file or `-` for stdin (default: -)\n\
    -h <host> ip address of the `balboa-backend` (default: 127.0.0.1)\n\
    -p <port> port of the `balboa-backend` (default: 4242)\n\
    -v increase verbosity; can be passed multiple times\n\
\n\
Examples:\n\
\n\
balboa-backend-console jsonize -r /tmp/pdns.dmp\n\
lz4cat /tmp/pdns.dmp.lz4 | balboa-backend-console jsonize\n\
\n");
  exit(1);
}

static int main_dump(int argc, char** argv) {
  const char* host = "127.0.0.1";
  const char* port = "4242";
  const char* dump_path_hint = "-";
  int verbosity = 0;
  trace_config_t trace_config = {.stream = stderr,
                                 .host = "pdns",
                                 .app = "balboa-backend-console",
                                 // leaking process number ...
                                 .procid = getpid()};
  ketopt_t opt = KETOPT_INIT;
  int c;
  while((c = ketopt(&opt, argc, argv, 1, "h:p:d:v", NULL)) >= 0) {
    switch(c) {
    case 'h': host = opt.arg; break;
    case 'p': port = opt.arg; break;
    case 'v': verbosity += 1; break;
    case 'd': dump_path_hint = opt.arg; break;
    default: break;
    }
  }

  theTrace_stream_use(&trace_config);
  theTrace_set_verbosity(verbosity);

  V(log_info(
      "host `%s` port `%s` dump_path_hint `%s`", host, port, dump_path_hint));
  int sock = dump_connect(host, port);
  if(sock < 0) {
    L(log_error("unable to connect to backend"));
    return (-1);
  }

  char scrtch[1024];
  size_t scrtch_sz = sizeof(scrtch);
  protocol_dump_request_t req = {.path = dump_path_hint};
  ssize_t used = blb_protocol_encode_dump_request(&req, scrtch, scrtch_sz);
  if(used <= 0) {
    L(log_error("blb_protocol_encode_dump_request() failed `%zd`", used));
    close(sock);
    return (-1);
  }
  char* p = scrtch;
  ssize_t r = used;
  while(r > 0) {
    ssize_t rc = write(sock, p, r);
    if(rc < 0) {
      L(log_error("write() failed with `%s`", strerror(errno)));
      close(sock);
      return (-1);
    } else if(rc == 0 && errno == EINTR) {
      continue;
    }
    r -= rc;
    p += rc;
  }

  while(1) {
    ssize_t rc = read(sock, scrtch, scrtch_sz);
    if(rc == 0) {
      fflush(stdout);
      close(sock);
      return (0);
    } else if(rc < 0) {
      L(log_error("read() failed with `%s`", strerror(errno)));
      close(sock);
      return (-1);
    }
    rc = fwrite(scrtch, rc, 1, stdout);
    if(rc != 1) {
      L(log_error("fwrite() failed with `%s`", strerror(errno)));
      close(sock);
      return (-1);
    }
  }
}

static int main_replay(int argc, char** argv) {
  const char* host = "127.0.0.1";
  const char* port = "4242";
  const char* dump_file = "-";
  int verbosity = 0;
  trace_config_t trace_config = {.stream = stderr,
                                 .host = "pdns",
                                 .app = "balboa-backend-console",
                                 // leaking process number ...
                                 .procid = getpid()};

  ketopt_t opt = KETOPT_INIT;
  int c;
  while((c = ketopt(&opt, argc, argv, 1, "d:h:p:v", NULL)) >= 0) {
    switch(c) {
    case 'd': dump_file = opt.arg; break;
    case 'h': host = opt.arg; break;
    case 'p': port = opt.arg; break;
    case 'v': verbosity += 1; break;
    default: break;
    }
  }

  theTrace_stream_use(&trace_config);
  theTrace_set_verbosity(verbosity);

  V(log_info("host `%s` port `%s` dump_file `%s`", host, port, dump_file));

  int sock = dump_connect(host, port);
  if(sock < 0) {
    L(log_error("unable to connect to backend"));
    return (-1);
  }
  state_t __state = {0}, *state = &__state;
  int state_ok = dump_state_init(state);
  if(state_ok != 0) {
    L(log_error("unable to initialize the dump state"));
    return (-1);
  }
  state->sock = sock;
  state->dump_entry_cb = dump_entry_replay_cb;
  int rc = dump(state, dump_file);
  return (rc);
}

int main(int argc, char** argv) {
  int res = -1;
  if(argc < 2) {
    usage();
  } else if(strcmp(argv[1], "jsonize") == 0) {
    argc--;
    argv++;
    res = main_jsonize(argc, argv);
  } else if(strcmp(argv[1], "replay") == 0) {
    argc--;
    argv++;
    res = main_replay(argc, argv);
  } else if(strcmp(argv[1], "dump") == 0) {
    argc--;
    argv++;
    res = main_dump(argc, argv);
  } else if(strcmp(argv[1], "--version") == 0) {
    version();
  } else {
    usage();
  }

  return (res);
}