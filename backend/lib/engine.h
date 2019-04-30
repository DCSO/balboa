// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#ifndef __ENGINE_H
#define __ENGINE_H

#include <alloc.h>
#include <inttypes.h>
#include <protocol.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include <trace.h>

#define ENGINE_CONN_SCRTCH_SZ (1024 * 10)
#define ENGINE_CONN_SCRTCH_BUFFERS (10)

typedef int socket_t;

typedef struct dbi_t dbi_t;
typedef struct db_t db_t;
typedef struct engine_t engine_t;
typedef struct conn_t conn_t;
typedef struct engine_stats_t engine_stats_t;

#define QUERY_TYPE_DEFAULT 0

struct dbi_t {
  db_t* (*thread_init)(conn_t*, db_t*);
  void (*thread_deinit)(conn_t*, db_t*);
  void (*teardown)(db_t* db);
  int (*query)(conn_t* th, const protocol_query_request_t* query);
  int (*input)(conn_t* th, const protocol_input_request_t* input);
  void (*backup)(conn_t* th, const protocol_backup_request_t* backup);
  void (*dump)(conn_t* th, const protocol_dump_request_t* dump);
};

struct db_t {
  const dbi_t* dbi;
};

enum engine_stats_counter_t {
  ENGINE_STATS_QUERIES = 0,
  ENGINE_STATS_INPUTS = 1,
  ENGINE_STATS_BACKUPS = 2,
  ENGINE_STATS_DUMPS = 3,
  ENGINE_STATS_BYTES_RECV = 4,
  ENGINE_STATS_BYTES_SEND = 5,
  ENGINE_STATS_CONNECTIONS = 6,
  ENGINE_STATS_ERRORS = 7,
  ENGINE_STATS_N = 8
};

struct engine_stats_t {
  unsigned long interval;
  struct timespec last;
  atomic_ullong counters[ENGINE_STATS_N];
};

struct engine_t {
  engine_stats_t stats;
  int conn_throttle_limit;
  db_t* db;
  socket_t listen_fd;
  bool enable_stats_reporter;
  bool enable_signal_consumer;
  pthread_t stats_reporter;
  pthread_t signal_consumer;
};

struct conn_t {
  pthread_t thread;
  engine_t* engine;
  db_t* db;
  void* usr_ctx;
  size_t usr_ctx_sz;
  socket_t fd;
  char scrtch[ENGINE_CONN_SCRTCH_SZ];
};

static inline void blb_engine_sleep(long seconds) {
  pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t c = PTHREAD_COND_INITIALIZER;
  (void)pthread_mutex_lock(&m);
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += seconds;
  (void)pthread_cond_timedwait(&c, &m, &ts);
  (void)pthread_mutex_unlock(&m);
}

static inline db_t* blb_dbi_conn_init(conn_t* th, db_t* db) {
  return (db->dbi->thread_init(th, db));
}

static inline void blb_dbi_conn_deinit(conn_t* th, db_t* db) {
  db->dbi->thread_deinit(th, db);
}

static inline void blb_dbi_teardown(db_t* db) {
  db->dbi->teardown(db);
}

static inline int blb_dbi_query(conn_t* th, const protocol_query_request_t* q) {
  return (th->db->dbi->query(th, q));
}

static inline int blb_dbi_input(conn_t* th, const protocol_input_request_t* i) {
  return (th->db->dbi->input(th, i));
}

static inline void blb_dbi_backup(
    conn_t* th, const protocol_backup_request_t* b) {
  th->db->dbi->backup(th, b);
}

static inline void blb_dbi_dump(conn_t* th, const protocol_dump_request_t* d) {
  th->db->dbi->dump(th, d);
}

static inline void blb_engine_stats_bump(
    engine_t* engine, enum engine_stats_counter_t counter) {
  if(counter < 0 || counter >= ENGINE_STATS_N) { return; }
  atomic_fetch_add(&engine->stats.counters[counter], 1);
}

static inline void blb_engine_stats_add(
    engine_t* engine,
    enum engine_stats_counter_t counter,
    unsigned long long x) {
  if(counter < 0 || counter >= ENGINE_STATS_N) { return; }
  atomic_fetch_add(&engine->stats.counters[counter], x);
}

typedef struct engine_config_t engine_config_t;
struct engine_config_t {
  int conn_throttle_limit;
  bool is_server;
  bool enable_signal_consumer;
  bool enable_stats_reporter;
  db_t* db;
  const char* host;
  int port;
};

static inline engine_config_t blb_engine_server_config_init() {
  return ((engine_config_t){.db = NULL,
                            .conn_throttle_limit = 64,
                            .is_server = true,
                            .enable_stats_reporter = true,
                            .enable_signal_consumer = true,
                            .host = "127.0.0.1",
                            .port = 4242});
}

static inline engine_config_t blb_engine_client_config_init() {
  return ((engine_config_t){.db = NULL,
                            .conn_throttle_limit = 64,
                            .is_server = false,
                            .enable_stats_reporter = true,
                            .enable_signal_consumer = true,
                            .host = "127.0.0.1",
                            .port = 4242});
}

void blb_engine_signals_init(void);
engine_t* blb_engine_server_new(const engine_config_t* config);
conn_t* blb_engine_client_new(const engine_config_t* config);
void blb_engine_teardown(engine_t* e);
void blb_engine_run(engine_t* e);
void blb_engine_request_stop(void);
protocol_stream_t* blb_engine_stream_new(conn_t* c);
int blb_conn_write_all(conn_t* th, char* _p, size_t _p_sz);
void blb_engine_conn_teardown(conn_t* th);

int blb_conn_query_stream_start_response(conn_t* th);
int blb_conn_query_stream_push_response(conn_t*, const protocol_entry_t* entry);
int blb_conn_query_stream_end_response(conn_t* th);
int blb_conn_dump_entry(conn_t* th, const protocol_entry_t* entry);

#endif