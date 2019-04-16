// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <rocksdb-impl.h>
#include <rocksdb/c.h>

#define ROCKSDB_CONN_SCRTCH_SZ (1024 * 10)

static void blb_rocksdb_teardown(db_t* _db);
static db_t* blb_rocksdb_conn_init(conn_t* th, db_t* db);
static void blb_rocksdb_conn_deinit(conn_t* th, db_t* db);
static int blb_rocksdb_query(conn_t* th, const protocol_query_request_t* q);
static int blb_rocksdb_input(conn_t* th, const protocol_input_request_t* i);
static void blb_rocksdb_backup(conn_t* th, const protocol_backup_request_t* b);
static void blb_rocksdb_dump(conn_t* th, const protocol_dump_request_t* d);

static const dbi_t blb_rocksdb_dbi = {.thread_init = blb_rocksdb_conn_init,
                                      .thread_deinit = blb_rocksdb_conn_deinit,
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
  char scrtch_key[ROCKSDB_CONN_SCRTCH_SZ];
  char scrtch_inv[ROCKSDB_CONN_SCRTCH_SZ];
};

rocksdb_t* blb_rocksdb_handle(db_t* db);
rocksdb_readoptions_t* blb_rocksdb_readoptions(db_t* db);

typedef struct value_t value_t;
struct value_t {
  uint32_t count;
  uint32_t first_seen;
  uint32_t last_seen;
};

static inline value_t blb_rocksdb_val_init() {
  return ((value_t){.count = 0, .first_seen = UINT32_MAX, .last_seen = 0});
}

#define blb_rocksdb_max(a, b) \
  ({                          \
    __typeof__(a) _a = (a);   \
    __typeof__(b) _b = (b);   \
    _a > _b ? _a : _b;        \
  })

#define blb_rocksdb_min(a, b) \
  ({                          \
    __typeof__(a) _a = (a);   \
    __typeof__(b) _b = (b);   \
    _a < _b ? _a : _b;        \
  })

static inline void _write_u32_le(unsigned char* p, uint32_t v) {
  p[0] = v >> 0;
  p[1] = v >> 8;
  p[2] = v >> 16;
  p[3] = v >> 24;
}

static inline uint32_t _read_u32_le(const unsigned char* p) {
  return (
      (((uint32_t)p[0]) << 0) | (((uint32_t)p[1]) << 8)
      | (((uint32_t)p[2]) << 16) | (((uint32_t)p[3]) << 24));
}

static inline int blb_rocksdb_val_encode(
    const struct value_t* o, char* buf, size_t buflen) {
  size_t minlen = sizeof(uint32_t) * 3;
  if(buflen < minlen) { return (-1); }

  unsigned char* p = (unsigned char*)buf;
  _write_u32_le(p + 0, o->count);
  _write_u32_le(p + 4, o->last_seen);
  _write_u32_le(p + 8, o->first_seen);

  return (0);
}

static inline int blb_rocksdb_val_decode(
    value_t* o, const char* buf, size_t buflen) {
  size_t minlen = sizeof(uint32_t) * 3;
  if(buflen < minlen) { return (-1); };

  const unsigned char* p = (const unsigned char*)buf;
  o->count = _read_u32_le(p + 0);
  o->last_seen = _read_u32_le(p + 4);
  o->first_seen = _read_u32_le(p + 8);

  return (0);
}

static inline void blb_rocksdb_val_merge(value_t* lhs, const value_t* rhs) {
  lhs->count += rhs->count;
  lhs->last_seen = blb_rocksdb_max(lhs->last_seen, rhs->last_seen);
  lhs->first_seen = blb_rocksdb_min(lhs->first_seen, rhs->first_seen);
}

static char* blb_rocksdb_merge_fully(
    void* state,
    const char* key,
    size_t key_len,
    value_t* obs,
    const char* const* opnds,
    const size_t* opnds_len,
    int n_opnds,
    unsigned char* success,
    size_t* new_len) {
  (void)state;
  if(key_len < 5) {
    V(log_warn(
        "merge called on unknown key `%p` `%s` `%.*s` opnds `%d`",
        key,
        key,
        (int)key_len,
        key,
        n_opnds));
  }
  // this is an observation value
  size_t buf_length = sizeof(uint32_t) * 3;
  char* buf = malloc(buf_length);
  if(buf == NULL) {
    *success = (unsigned char)0;
    *new_len = 0;
    return (NULL);
  }
  for(int i = 0; i < n_opnds; i++) {
    value_t nobs = {0, 0, 0};
    int rc = blb_rocksdb_val_decode(&nobs, opnds[i], opnds_len[i]);
    if(rc != 0) {
      L(log_error(
          "blb_rocksdb_val_decode() failed (key `%.*s` opnd `%d`)",
          (int)key_len,
          key,
          i));
      continue;
    }
    blb_rocksdb_val_merge(obs, &nobs);
  }
  blb_rocksdb_val_encode(obs, buf, buf_length);
  *new_len = buf_length;
  *success = (unsigned char)1;
  return (buf);
}

static char* blb_rocksdb_mergeop_full_merge(
    void* state,
    const char* key,
    size_t key_len,
    const char* existing_value,
    size_t existing_value_length,
    const char* const* opnds,
    const size_t* opnds_len,
    int n_opnds,
    unsigned char* success,
    size_t* new_len) {
  value_t obs = blb_rocksdb_val_init();
  if(key[0] == 'o' && existing_value != NULL) {
    int rc =
        blb_rocksdb_val_decode(&obs, existing_value, existing_value_length);
    if(rc != 0) {
      L(log_error("blb_rocksdb_val_decode() failed"));
      *success = 1;
      return (NULL);
    }
  }
  char* result = blb_rocksdb_merge_fully(
      state, key, key_len, &obs, opnds, opnds_len, n_opnds, success, new_len);
  return (result);
}

static char* blb_rocksdb_mergeop_partial_merge(
    void* state,
    const char* key,
    size_t key_len,
    const char* const* opnds,
    const size_t* opnds_len,
    int n_opnds,
    unsigned char* success,
    size_t* new_len) {
  value_t obs = blb_rocksdb_val_init();
  char* result = blb_rocksdb_merge_fully(
      state, key, key_len, &obs, opnds, opnds_len, n_opnds, success, new_len);
  return (result);
}

static void blb_rocksdb_mergeop_destructor(void* state) {
  (void)state;
}

static const char* blb_rocksdb_mergeop_name(void* state) {
  (void)state;
  return ("observation-mergeop");
}

static inline rocksdb_mergeoperator_t* blb_rocksdb_mergeoperator_create() {
  return (rocksdb_mergeoperator_create(
      NULL,
      blb_rocksdb_mergeop_destructor,
      blb_rocksdb_mergeop_full_merge,
      blb_rocksdb_mergeop_partial_merge,
      NULL,
      blb_rocksdb_mergeop_name));
}

db_t* blb_rocksdb_conn_init(conn_t* th, db_t* db) {
  (void)th;
  return (db);
}

void blb_rocksdb_conn_deinit(conn_t* th, db_t* db) {
  (void)th;
  (void)db;
}

void blb_rocksdb_teardown(db_t* _db) {
  ASSERT(_db->dbi == &blb_rocksdb_dbi);
  blb_rocksdb_t* db = (blb_rocksdb_t*)_db;
  L(log_notice("teardown"));
  rocksdb_mergeoperator_destroy(db->mergeop);
  rocksdb_writeoptions_destroy(db->writeoptions);
  rocksdb_readoptions_destroy(db->readoptions);
  // keeping this causes segfault
  // rocksdb_options_destroy(db->options);
  rocksdb_close(db->db);
  blb_free(db);
}

static int blb_rocksdb_query_by_o(
    conn_t* th, const protocol_query_request_t* q) {
  ASSERT(th->db->dbi == &blb_rocksdb_dbi);
  blb_rocksdb_t* db = (blb_rocksdb_t*)th->db;
  size_t prefix_len = 0;
  if(q->qsensorid_len > 0) {
    prefix_len = q->qsensorid_len + q->qrrname_len + 4;
    (void)snprintf(
        db->scrtch_key,
        ROCKSDB_CONN_SCRTCH_SZ,
        "o\x1f%.*s\x1f%.*s\x1f",
        (int)q->qrrname_len,
        q->qrrname,
        (int)q->qsensorid_len,
        q->qsensorid);
  } else {
    prefix_len = q->qrrname_len + 3;
    (void)snprintf(
        db->scrtch_key,
        ROCKSDB_CONN_SCRTCH_SZ,
        "o\x1f%.*s\x1f",
        (int)q->qrrname_len,
        q->qrrname);
  }

  X(log_debug("prefix key `%.*s`", (int)prefix_len, db->scrtch_key));

  int start_ok = blb_conn_query_stream_start_response(th);
  if(start_ok != 0) {
    L(log_error("unable to start query stream response"));
    return (-1);
  }

  rocksdb_iterator_t* it = rocksdb_create_iterator(db->db, db->readoptions);
  rocksdb_iter_seek(it, db->scrtch_key, prefix_len);
  size_t keys_visited = 0;
  size_t keys_hit = 0;
  for(;
      rocksdb_iter_valid(it) != (unsigned char)0 && keys_hit < (size_t)q->limit;
      rocksdb_iter_next(it)) {
    keys_visited += 1;
    size_t key_len = 0;
    const char* key = rocksdb_iter_key(it, &key_len);
    if(key == NULL) {
      L(log_error("impossible: unable to extract key from rocksdb iterator"));
      goto stream_error;
    }

    enum TokIdx { RRNAME = 0, SENSORID = 1, RRTYPE = 2, RDATA = 3, FIELDS = 4 };

    struct Tok {
      const char* tok;
      int tok_len;
    };

    struct Tok toks[FIELDS];
    memset(toks, 0, sizeof(toks));

    enum TokIdx j = RRNAME;
    size_t last = 1;
    for(size_t i = 2; i < key_len; i++) {
      if(key[i] == '\x1f') {
        // we fixup the RDATA and skip extra \x1f's
        if(j < RDATA) {
          toks[j].tok = &key[last + 1];
          toks[j].tok_len = i - last - 1;
          last = i;
          j++;
        }
      }
    }
    toks[RDATA].tok = &key[last + 1];
    toks[RDATA].tok_len = key_len - last - 1;

    X(log_debug(
        "o %.*s %.*s %.*s %.*s",
        toks[RRNAME].tok_len,
        toks[RRNAME].tok,
        toks[SENSORID].tok_len,
        toks[SENSORID].tok,
        toks[RRTYPE].tok_len,
        toks[RRTYPE].tok,
        toks[RDATA].tok_len,
        toks[RDATA].tok));

    size_t qrrname_len = q->qrrname_len;
    if(toks[RRNAME].tok_len <= 0
       || memcmp(
              toks[RRNAME].tok,
              q->qrrname,
              blb_rocksdb_min((size_t)toks[RRNAME].tok_len, qrrname_len))
              != 0) {
      break;
    }
    if((size_t)toks[RRNAME].tok_len != qrrname_len) { continue; }

    if(toks[SENSORID].tok_len == 0
       || (q->qsensorid_len > 0
           && (size_t)toks[SENSORID].tok_len != q->qsensorid_len)
       || (q->qsensorid_len > 0
           && memcmp(toks[SENSORID].tok, q->qsensorid, toks[SENSORID].tok_len)
                  != 0)) {
      continue;
    }

    if(toks[RDATA].tok_len == 0
       || (q->qrdata_len > 0 && (size_t)toks[RDATA].tok_len != q->qrdata_len)
       || (q->qrdata_len > 0
           && memcmp(toks[RDATA].tok, q->qrdata, toks[RDATA].tok_len) != 0)) {
      continue;
    }

    if(toks[RRTYPE].tok_len == 0
       || (q->qrrtype_len > 0 && (size_t)toks[RRTYPE].tok_len != q->qrrtype_len)
       || (q->qrrtype_len > 0
           && memcmp(toks[RRTYPE].tok, q->qrrtype, toks[RRTYPE].tok_len)
                  != 0)) {
      continue;
    }

    size_t val_size = 0;
    value_t v;
    const char* val = rocksdb_iter_value(it, &val_size);
    int ret = blb_rocksdb_val_decode(&v, val, val_size);
    if(ret != 0) {
      L(log_error("blb_rocksdb_val_decode() failed"));
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
    int push_ok = blb_conn_query_stream_push_response(th, e);
    if(push_ok != 0) {
      L(log_error("unable to push query response entry"));
      goto stream_error;
    }
  }
  rocksdb_iter_destroy(it);
  (void)blb_conn_query_stream_end_response(th);
  return (0);

stream_error:
  rocksdb_iter_destroy(it);
  return (-1);
}

static int blb_rocksdb_query_by_i(
    conn_t* th, const protocol_query_request_t* q) {
  ASSERT(th->db->dbi == &blb_rocksdb_dbi);
  blb_rocksdb_t* db = (blb_rocksdb_t*)th->db;
  size_t prefix_len = 0;
  if(q->qsensorid_len > 0) {
    prefix_len = q->qrdata_len + q->qsensorid_len + 4;
    (void)snprintf(
        db->scrtch_inv,
        ROCKSDB_CONN_SCRTCH_SZ,
        "i\x1f%.*s\x1f%.*s\x1f",
        (int)q->qrdata_len,
        q->qrdata,
        (int)q->qsensorid_len,
        q->qsensorid);
  } else {
    prefix_len = q->qrdata_len + 3;
    (void)snprintf(
        db->scrtch_inv,
        ROCKSDB_CONN_SCRTCH_SZ,
        "i\x1f%.*s\x1f",
        (int)q->qrdata_len,
        q->qrdata);
  }
  ASSERT(db->scrtch_inv[prefix_len] == '\0');

  X(log_debug("prefix key `%.*s`", (int)prefix_len, db->scrtch_inv));

  int start_ok = blb_conn_query_stream_start_response(th);
  if(start_ok != 0) {
    L(log_error("unable to start query stream response"));
    return (-1);
  }

  rocksdb_iterator_t* it = rocksdb_create_iterator(db->db, db->readoptions);
  rocksdb_iter_seek(it, db->scrtch_inv, prefix_len);
  size_t keys_visited = 0;
  int keys_hit = 0;
  for(; rocksdb_iter_valid(it) != (unsigned char)0 && keys_hit < q->limit;
      rocksdb_iter_next(it)) {
    keys_visited += 1;
    size_t key_len = 0;
    const char* key = rocksdb_iter_key(it, &key_len);
    char* err = NULL;

    enum TokIdx { RDATA = 3, SENSORID = 2, RRNAME = 1, RRTYPE = 0, FIELDS = 4 };

    struct Tok {
      const char* tok;
      int tok_len;
    };

    struct Tok toks[FIELDS];
    memset(toks, 0, sizeof(toks));

    enum TokIdx j = RRTYPE;
    size_t last = key_len;
    for(ssize_t i = key_len - 1; i > 0; i--) {
      if(key[i] == '\x1f') {
        if(j < FIELDS) {
          toks[j].tok = &key[i + 1];
          toks[j].tok_len = last - i - 1;
          last = i;
          j++;
        }
      }
    }
    toks[RDATA].tok = key + 2;
    toks[RDATA].tok_len = toks[RDATA].tok_len + last - 1;

    X(log_debug("k `%zu` `%.*s`", key_len, (int)key_len, key));
    X(log_debug(
        "i `%.*s` | `%.*s` `%.*s` `%.*s`",
        toks[RDATA].tok_len,
        toks[RDATA].tok,
        toks[SENSORID].tok_len,
        toks[SENSORID].tok,
        toks[RRTYPE].tok_len,
        toks[RRTYPE].tok,
        toks[RRNAME].tok_len,
        toks[RRNAME].tok));

    if(j < FIELDS) {
      L(log_error("found invalid key `%.*s`; skipping ...", (int)key_len, key));
      continue;
    }

    size_t qrdata_len = q->qrdata_len;
    if(toks[RDATA].tok_len <= 0
       || memcmp(
              toks[RDATA].tok,
              q->qrdata,
              blb_rocksdb_min((size_t)toks[RDATA].tok_len, qrdata_len))
              != 0) {
      break;
    }
    if((size_t)toks[RDATA].tok_len != qrdata_len) { continue; }

    if(toks[SENSORID].tok_len == 0
       || (q->qsensorid_len > 0
           && (size_t)toks[SENSORID].tok_len != q->qsensorid_len)
       || (q->qsensorid_len > 0
           && memcmp(toks[SENSORID].tok, q->qsensorid, toks[SENSORID].tok_len)
                  != 0)) {
      continue;
    }

    if(toks[RRTYPE].tok_len == 0
       || (q->qrrtype_len > 0 && (size_t)toks[RRTYPE].tok_len != q->qrrtype_len)
       || (q->qrrtype_len > 0
           && memcmp(toks[RRTYPE].tok, q->qrrtype, toks[RRTYPE].tok_len)
                  != 0)) {
      continue;
    }

    memset(db->scrtch_key, '\0', ROCKSDB_CONN_SCRTCH_SZ);
    ssize_t fullkey_len = snprintf(
        db->scrtch_key,
        ROCKSDB_CONN_SCRTCH_SZ,
        "o\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s",
        toks[RRNAME].tok_len,
        toks[RRNAME].tok,
        toks[SENSORID].tok_len,
        toks[SENSORID].tok,
        toks[RRTYPE].tok_len,
        toks[RRTYPE].tok,
        toks[RDATA].tok_len,
        toks[RDATA].tok);

    if(fullkey_len <= 0 || fullkey_len >= ROCKSDB_CONN_SCRTCH_SZ) {
      L(log_error("invalid key `%.*s`", (int)fullkey_len, db->scrtch_key));
      continue;
    }

    X(log_debug("full key `%.*s`", (int)fullkey_len, db->scrtch_key));

    size_t val_size = 0;
    char* val = rocksdb_get(
        db->db, db->readoptions, db->scrtch_key, fullkey_len, &val_size, &err);
    if(val == NULL || err != NULL) {
      X(log_debug("rocksdb_get() failed with `%s`", err));
      free(err);
      continue;
    }

    value_t v;
    int ret = blb_rocksdb_val_decode(&v, val, val_size);
    if(ret != 0) {
      L(log_error(
          "blb_rocksdb_val_decode() failed (key `%.*s` val_ptr `%p` val_sz "
          "`%zu`)",
          (int)fullkey_len,
          db->scrtch_key,
          val,
          val_size));
      free(val);
      continue;
    }
    free(val);

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
    int push_ok = blb_conn_query_stream_push_response(th, e);
    if(push_ok != 0) {
      L(log_error("unable to push query response entry"));
      goto stream_error;
    }
  }
  rocksdb_iter_destroy(it);
  (void)blb_conn_query_stream_end_response(th);
  return (0);

stream_error:
  rocksdb_iter_destroy(it);
  return (-1);
}

static int blb_rocksdb_query(conn_t* th, const protocol_query_request_t* q) {
  int rc = -1;
  if(q->qrrname_len > 0) {
    rc = blb_rocksdb_query_by_o(th, q);
  } else {
    rc = blb_rocksdb_query_by_i(th, q);
  }
  return (rc);
}

static void blb_rocksdb_backup(conn_t* th, const protocol_backup_request_t* b) {
  ASSERT(th->db->dbi == &blb_rocksdb_dbi);
  blb_rocksdb_t* db = (blb_rocksdb_t*)th->db;

  X(log_info("backup `%.*s`", (int)b->path_len, b->path));

  if(b->path_len >= 256) {
    L(log_error("invalid path"));
    return;
  }

  char path[256];
  snprintf(path, sizeof(path), "%.*s", (int)b->path_len, b->path);

  char* err = NULL;
  rocksdb_backup_engine_t* be =
      rocksdb_backup_engine_open(db->options, path, &err);
  if(err != NULL) {
    L(log_error("rocksdb_backup_engine_open() failed `%s`", err));
    free(err);
    return;
  }

  rocksdb_backup_engine_create_new_backup(be, db->db, &err);
  if(err != NULL) {
    L(log_error("rocksdb_backup_engine_create_new_backup() failed `%s`", err));
    free(err);
    rocksdb_backup_engine_close(be);
    return;
  }
}

static void blb_rocksdb_dump(conn_t* th, const protocol_dump_request_t* d) {
  ASSERT(th->db->dbi == &blb_rocksdb_dbi);
  blb_rocksdb_t* db = (blb_rocksdb_t*)th->db;

  X(log_info("dump `%.*s`", (int)d->path_len, d->path));

  uint64_t cnt = 0;
  rocksdb_iterator_t* it = rocksdb_create_iterator(db->db, db->readoptions);
  // rocksdb_iter_seek_to_first(it);
  rocksdb_iter_seek(it, "o", 1);
  for(; rocksdb_iter_valid(it) != (unsigned char)0; rocksdb_iter_next(it)) {
    size_t key_len = 0;
    const char* key = rocksdb_iter_key(it, &key_len);
    if(key == NULL) {
      L(log_error("impossible: unable to extract key from rocksdb iterator"));
      break;
    }

    if(key[0] == 'i') { continue; }

    enum TokIdx { RRNAME = 0, SENSORID = 1, RRTYPE = 2, RDATA = 3, FIELDS = 4 };

    struct Tok {
      const char* tok;
      int tok_len;
    };

    struct Tok toks[FIELDS];
    memset(toks, 0, sizeof(toks));

    enum TokIdx j = RRNAME;
    size_t last = 1;
    for(size_t i = 2; i < key_len; i++) {
      if(key[i] == '\x1f') {
        // we fixup the RDATA and skip extra \x1f's
        if(j < RDATA) {
          toks[j].tok = &key[last + 1];
          toks[j].tok_len = i - last - 1;
          last = i;
          j++;
        }
      }
    }
    toks[RDATA].tok = &key[last + 1];
    toks[RDATA].tok_len = key_len - last - 1;

    X(log_debug(
        "o %.*s %.*s %.*s %.*s",
        toks[RRNAME].tok_len,
        toks[RRNAME].tok,
        toks[SENSORID].tok_len,
        toks[SENSORID].tok,
        toks[RRTYPE].tok_len,
        toks[RRTYPE].tok,
        toks[RDATA].tok_len,
        toks[RDATA].tok));

    size_t val_size = 0;
    value_t v;
    const char* val = rocksdb_iter_value(it, &val_size);
    int ret = blb_rocksdb_val_decode(&v, val, val_size);
    if(ret != 0) {
      L(log_error("blb_rocksdb_val_decode() failed"));
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

    int rc = blb_conn_dump_entry(th, e);
    if(rc != 0) {
      L(log_error("blb_conn_dump_entry() failed"));
      break;
    }
  }

  char* err = NULL;
  rocksdb_iter_get_error(it, &err);
  if(err != NULL) { L(log_error("iterator error `%s`", err)); }
  rocksdb_iter_destroy(it);
  L(log_notice("dumped `%" PRIu64 "` entries", cnt));
}

static int blb_rocksdb_input(conn_t* th, const protocol_input_request_t* i) {
  ASSERT(th->db->dbi == &blb_rocksdb_dbi);
  blb_rocksdb_t* db = (blb_rocksdb_t*)th->db;

  value_t v = {.count = i->entry.count,
               .first_seen = i->entry.first_seen,
               .last_seen = i->entry.last_seen};
  char val[sizeof(uint32_t) * 3];
  size_t val_len = sizeof(val);
  (void)blb_rocksdb_val_encode(&v, val, val_len);

  int key_sz = snprintf(
      db->scrtch_key,
      ROCKSDB_CONN_SCRTCH_SZ,
      "o\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s",
      (int)i->entry.rrname_len,
      i->entry.rrname,
      (int)i->entry.sensorid_len,
      i->entry.sensorid,
      (int)i->entry.rrtype_len,
      i->entry.rrtype,
      (int)i->entry.rdata_len,
      i->entry.rdata);
  if(key_sz <= 0 || key_sz >= ROCKSDB_CONN_SCRTCH_SZ) {
    L(log_error("truncated key"));
    return (-1);
  }

  int inv_sz = snprintf(
      db->scrtch_inv,
      ROCKSDB_CONN_SCRTCH_SZ,
      "i\x1f%.*s\x1f%.*s\x1f%.*s\x1f%.*s",
      (int)i->entry.rdata_len,
      i->entry.rdata,
      (int)i->entry.sensorid_len,
      i->entry.sensorid,
      (int)i->entry.rrname_len,
      i->entry.rrname,
      (int)i->entry.rrtype_len,
      i->entry.rrtype);
  if(inv_sz <= 0 || inv_sz >= ROCKSDB_CONN_SCRTCH_SZ) {
    L(log_error("truncated inverted key"));
    return (-1);
  }

  if(inv_sz < 5 || key_sz < 5) {
    L(log_error(
        "derived invalid input keys: inv_sz `%d` key_sz `%d`", inv_sz, key_sz));
    return (-1);
  }

  char* err = NULL;
  rocksdb_merge(
      db->db, db->writeoptions, db->scrtch_key, key_sz, val, val_len, &err);
  if(err != NULL) {
    L(log_error("rocksdb_merge() failed: `%s`", err));
    free(err);
    return (-1);
  }

  // XXX: put vs merge
  rocksdb_put(db->db, db->writeoptions, db->scrtch_inv, inv_sz, "", 0, &err);
  if(err != NULL) {
    L(log_error("rocksdb_put() failed: `%s`", err));
    free(err);
    return (-1);
  }

  return (0);
}

rocksdb_t* blb_rocksdb_handle(db_t* _db) {
  ASSERT(_db->dbi == &blb_rocksdb_dbi);
  blb_rocksdb_t* db = (blb_rocksdb_t*)_db;
  return (db->db);
}

db_t* blb_rocksdb_open(const blb_rocksdb_config_t* c) {
  V(log_info("rocksdb database at `%s`", c->path));
  V(log_info(
      "parallelism `%d` membudget `%zu` max_log_file_size `%zu` "
      "keep_log_file_num `%d`",
      c->parallelism,
      c->membudget,
      c->max_log_file_size,
      c->keep_log_file_num));

  blb_rocksdb_t* db = blb_new(blb_rocksdb_t);
  if(db == NULL) { return (NULL); }
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

  rocksdb_options_increase_parallelism(db->options, c->parallelism);
  rocksdb_options_optimize_level_style_compaction(db->options, c->membudget);
  rocksdb_options_set_create_if_missing(db->options, 1);
  rocksdb_options_set_max_log_file_size(db->options, c->max_log_file_size);
  rocksdb_options_set_keep_log_file_num(db->options, c->keep_log_file_num);
  rocksdb_options_set_max_open_files(db->options, c->max_open_files);
  rocksdb_options_set_merge_operator(db->options, db->mergeop);
  rocksdb_options_set_compression_per_level(db->options, level_compression, 5);

  db->db = rocksdb_open(db->options, c->path, &err);
  if(err != NULL) {
    L(log_error("rocksdb_open() failed: `%s`", err));
    rocksdb_options_destroy(db->options);
    rocksdb_mergeoperator_destroy(db->mergeop);
    rocksdb_writeoptions_destroy(db->writeoptions);
    rocksdb_readoptions_destroy(db->readoptions);
    free(err);
    blb_free(db);
    return (NULL);
  }

  V(log_debug("rocksdb at %p", db));

  return ((db_t*)db);
}
