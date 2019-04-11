// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#ifndef __ENGINE_H
#define __ENGINE_H

#include <alloc.h>
#include <inttypes.h>
#include <protocol.h>
#include <pthread.h>
#include <trace.h>

#define ENGINE_CONN_SCRTCH_SZ (1024 * 10)
#define ENGINE_CONN_SCRTCH_BUFFERS (10)

typedef int socket_t;

typedef struct dbi_t dbi_t;
typedef struct db_t db_t;
typedef struct engine_t engine_t;
typedef struct conn_t conn_t;

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

struct engine_t {
  int conn_throttle_limit;
  db_t* db;
  socket_t listen_fd;
};

struct conn_t {
  pthread_t thread;
  engine_t* engine;
  db_t* db;
  void* usr_ctx;
  socket_t fd;
  char scrtch_response[ENGINE_CONN_SCRTCH_SZ];
};

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

void blb_engine_signals_init(void);
engine_t* blb_engine_new(
    db_t* db, const char* name, int port, int conn_throttle_limit);
void blb_engine_teardown(engine_t* e);
void blb_engine_run(engine_t* e);

int blb_conn_query_stream_start_response(conn_t* th);
int blb_conn_query_stream_push_response(conn_t*, const protocol_entry_t* entry);
int blb_conn_query_stream_end_response(conn_t* th);
int blb_conn_dump_entry(conn_t* th, const protocol_entry_t* entry);

#endif