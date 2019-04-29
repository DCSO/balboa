// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#include <bs.h>
#include <mpack.h>
#include <protocol.h>
#include <trace.h>

enum {
  OBS_RRNAME_IDX = 0,
  OBS_RRTYPE_IDX = 1,
  OBS_RDATA_IDX = 2,
  OBS_SENSOR_IDX = 3,
  OBS_COUNT_IDX = 4,
  OBS_FIRST_SEEN_IDX = 5,
  OBS_LAST_SEEN_IDX = 6,
  OBS_FIELDS = 7
};

#define PROTOCOL_TYPED_MESSAGE_TYPE_KEY ("T")
#define PROTOCOL_TYPED_MESSAGE_ENCODED_KEY ("M")

#define PROTOCOL_BACKUP_REQUEST_PATH_KEY ("P")

#define PROTOCOL_DUMP_REQUEST_PATH_KEY ("P")

#define PROTOCOL_QUERY_REQUEST_QRDATA_KEY ("Qrdata")
#define PROTOCOL_QUERY_REQUEST_QRRNAME_KEY ("Qrrname")
#define PROTOCOL_QUERY_REQUEST_QRRTYPE_KEY ("Qrrtype")
#define PROTOCOL_QUERY_REQUEST_QSENSORID_KEY ("QsensorID")
#define PROTOCOL_QUERY_REQUEST_HRDATA_KEY ("Hrdata")
#define PROTOCOL_QUERY_REQUEST_HRRNAME_KEY ("Hrrname")
#define PROTOCOL_QUERY_REQUEST_HRRTYPE_KEY ("Hrrtype")
#define PROTOCOL_QUERY_REQUEST_HSENSORID_KEY ("HsensorID")
#define PROTOCOL_QUERY_REQUEST_LIMIT_KEY ("Limit")

#define PROTOCOL_INPUT_REQUEST_OBSERVATION_KEY0 ('O')

#define PROTOCOL_PDNS_ENTRY_RRNAME_KEY0 ('N')
#define PROTOCOL_PDNS_ENTRY_RRTYPE_KEY0 ('T')
#define PROTOCOL_PDNS_ENTRY_RDATA_KEY0 ('D')
#define PROTOCOL_PDNS_ENTRY_SENSORID_KEY0 ('I')
#define PROTOCOL_PDNS_ENTRY_COUNT_KEY0 ('C')
#define PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY0 ('F')
#define PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY0 ('L')

#define PROTOCOL_PDNS_ENTRY_RRNAME_KEY ("N")
#define PROTOCOL_PDNS_ENTRY_RRTYPE_KEY ("T")
#define PROTOCOL_PDNS_ENTRY_RDATA_KEY ("D")
#define PROTOCOL_PDNS_ENTRY_SENSORID_KEY ("I")
#define PROTOCOL_PDNS_ENTRY_COUNT_KEY ("C")
#define PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY ("F")
#define PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY ("L")

#define PROTOCOL_SCRTCH_SZ (1024 * 10)
#define PROTOCOL_SCRTCH_BUFFERS (10)

struct protocol_stream_t {
  mpack_tree_t tree;
  void* usr;
  ssize_t (*read_cb)(void* usr, char* p, size_t p_sz);
  char scrtch[PROTOCOL_SCRTCH_BUFFERS][PROTOCOL_SCRTCH_SZ];
};

struct protocol_dump_stream_t {
  mpack_reader_t reader;
  unsigned char scrtch[PROTOCOL_SCRTCH_SZ];
};

static ssize_t blb_protocol_encode_outer_request(
    int type, char* p, size_t p_sz, size_t used_inner) {
  ASSERT(used_inner < p_sz);
  mpack_writer_t __wr = {0}, *wr = &__wr;

  // encode outer message
  mpack_writer_init(wr, p + used_inner, p_sz - used_inner);
  mpack_start_map(wr, 2);
  mpack_write_cstr(wr, PROTOCOL_TYPED_MESSAGE_TYPE_KEY);
  mpack_write_int(wr, type);
  mpack_write_cstr(wr, PROTOCOL_TYPED_MESSAGE_ENCODED_KEY);
  mpack_write_bin(wr, p, used_inner);
  mpack_finish_map(wr);
  mpack_error_t outer_err = mpack_writer_error(wr);
  if(outer_err != mpack_ok) {
    L(log_debug("encoding outer message failed `%d`", outer_err));
    mpack_writer_destroy(wr);
    return (-1);
  }

  size_t used_outer = mpack_writer_buffer_used(wr);
  X(log_debug("encoded outer message size `%zu`", used_outer));
  mpack_writer_destroy(wr);
  ASSERT((used_inner + used_outer) < p_sz);
  memmove(p, p + used_inner, used_outer);
  return (used_outer);
}

ssize_t blb_protocol_encode_dump_request(
    const protocol_dump_request_t* r, char* p, size_t p_sz) {
  mpack_writer_t __wr = {0}, *wr = &__wr;

  // encode inner message
  mpack_writer_init(wr, p, p_sz);
  mpack_start_map(wr, 1);
  mpack_write_cstr(wr, PROTOCOL_DUMP_REQUEST_PATH_KEY);
  mpack_write_str(wr, r->path, r->path_len);
  mpack_finish_map(wr);
  mpack_error_t err = mpack_writer_error(wr);
  if(err != mpack_ok) {
    L(log_error("encoding inner msgpack data failed `%d`", err));
    mpack_writer_destroy(wr);
    return (-1);
  }

  size_t used_inner = mpack_writer_buffer_used(wr);
  X(log_debug("encoded inner message size `%zu`", used_inner));
  ASSERT(used_inner < p_sz);
  mpack_writer_destroy(wr);

  return (blb_protocol_encode_outer_request(
      PROTOCOL_DUMP_REQUEST, p, p_sz, used_inner));
}

ssize_t blb_protocol_encode_backup_request(
    const protocol_backup_request_t* r, char* p, size_t p_sz) {
  mpack_writer_t __wr = {0}, *wr = &__wr;

  // encode inner message
  mpack_writer_init(wr, p, p_sz);
  mpack_start_map(wr, 1);
  mpack_write_cstr(wr, PROTOCOL_BACKUP_REQUEST_PATH_KEY);
  mpack_write_str(wr, r->path, r->path_len);
  mpack_finish_map(wr);
  mpack_error_t err = mpack_writer_error(wr);
  if(err != mpack_ok) {
    L(log_error("encoding inner msgpack data failed `%d`", err));
    mpack_writer_destroy(wr);
    return (-1);
  }

  size_t used_inner = mpack_writer_buffer_used(wr);
  X(log_debug("encoded inner message size `%zu`", used_inner));
  ASSERT(used_inner < p_sz);
  mpack_writer_destroy(wr);

  return (blb_protocol_encode_outer_request(
      PROTOCOL_BACKUP_REQUEST, p, p_sz, used_inner));
}

ssize_t blb_protocol_encode_dump_entry(
    const protocol_entry_t* entry, char* p, size_t p_sz) {
  mpack_writer_t __wr = {0}, *wr = &__wr;
  mpack_writer_init(wr, p, p_sz);

  mpack_start_map(wr, OBS_FIELDS);
  mpack_write_uint(wr, OBS_RRNAME_IDX);
  mpack_write_bin(wr, entry->rrname, entry->rrname_len);
  mpack_write_uint(wr, OBS_RRTYPE_IDX);
  mpack_write_bin(wr, entry->rrtype, entry->rrtype_len);
  mpack_write_uint(wr, OBS_RDATA_IDX);
  mpack_write_bin(wr, entry->rdata, entry->rdata_len);
  mpack_write_uint(wr, OBS_SENSOR_IDX);
  mpack_write_bin(wr, entry->sensorid, entry->sensorid_len);
  mpack_write_uint(wr, OBS_COUNT_IDX);
  mpack_write_uint(wr, entry->count);
  mpack_write_uint(wr, OBS_FIRST_SEEN_IDX);
  mpack_write_uint(wr, entry->first_seen);
  mpack_write_uint(wr, OBS_LAST_SEEN_IDX);
  mpack_write_uint(wr, entry->last_seen);
  mpack_finish_map(wr);

  mpack_error_t err = mpack_writer_error(wr);
  if(err != mpack_ok) {
    L(log_error("encoding dump entry failed with mpack_error_t `%d`", err));
    mpack_writer_destroy(wr);
    return (-1);
  }

  size_t used = mpack_writer_buffer_used(wr);
  X(log_debug("encoded dump entry size `%zu`", used));

  ASSERT(used < p_sz);

  mpack_writer_destroy(wr);

  return (used);
}

ssize_t blb_protocol_encode_entry(
    const protocol_entry_t* entry, char* p, size_t p_sz) {
  mpack_writer_t __wr = {0}, *wr = &__wr;
  mpack_writer_init(wr, p, p_sz);

  mpack_start_map(wr, 7);
  mpack_write_cstr(wr, PROTOCOL_PDNS_ENTRY_COUNT_KEY);
  mpack_write_uint(wr, entry->count);
  mpack_write_cstr(wr, PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY);
  mpack_write_timestamp_seconds(wr, entry->first_seen);
  // mpack_write_uint( wr, entry->first_seen );
  mpack_write_cstr(wr, PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY);
  mpack_write_timestamp_seconds(wr, entry->last_seen);
  // mpack_write_uint( wr, entry->last_seen );
  mpack_write_cstr(wr, PROTOCOL_PDNS_ENTRY_RDATA_KEY);
  mpack_write_str(wr, entry->rdata, entry->rdata_len);
  mpack_write_cstr(wr, PROTOCOL_PDNS_ENTRY_RRNAME_KEY);
  mpack_write_str(wr, entry->rrname, entry->rrname_len);
  mpack_write_cstr(wr, PROTOCOL_PDNS_ENTRY_RRTYPE_KEY);
  mpack_write_str(wr, entry->rrtype, entry->rrtype_len);
  mpack_write_cstr(wr, PROTOCOL_PDNS_ENTRY_SENSORID_KEY);
  mpack_write_str(wr, entry->sensorid, entry->sensorid_len);
  mpack_finish_map(wr);

  size_t used_inner = mpack_writer_buffer_used(wr);

  mpack_error_t err = mpack_writer_error(wr);
  if(err != mpack_ok) {
    L(log_error("encoding inpot failed with mpack_error_t `%d`", err));
    mpack_writer_destroy(wr);
    return (-1);
  }

  mpack_writer_destroy(wr);

  return (used_inner);
}

ssize_t blb_protocol_encode_input_request(
    const protocol_input_request_t* input, char* p, size_t p_sz) {
  ssize_t used_inner = blb_protocol_encode_entry(&input->entry, p, p_sz);
  if(used_inner <= 0) {
    L(log_error("blb_protocol_encode_entry() failed"));
    return (-1);
  }
  return (blb_protocol_encode_outer_request(
      PROTOCOL_INPUT_REQUEST, p, p_sz, used_inner));
}

ssize_t blb_protocol_encode_stream_start_response(char* p, size_t p_sz) {
  return (blb_protocol_encode_outer_request(
      PROTOCOL_QUERY_STREAM_START_RESPONSE, p, p_sz, 0));
}

ssize_t blb_protocol_encode_stream_end_response(char* p, size_t p_sz) {
  return (blb_protocol_encode_outer_request(
      PROTOCOL_QUERY_STREAM_END_RESPONSE, p, p_sz, 0));
}

ssize_t blb_protocol_encode_stream_entry(
    const protocol_entry_t* entry, char* p, size_t p_sz) {
  ssize_t rc = blb_protocol_encode_entry(entry, p, p_sz);
  if(rc <= 0) { return (-1); }

  return (blb_protocol_encode_outer_request(
      PROTOCOL_QUERY_STREAM_DATA_RESPONSE, p, p_sz, rc));
}

// read-from-stream api

#define PROTOCOL_POLL_READ_TIMEOUT (60)

static size_t blb_protocol_stream_cb(mpack_tree_t* tree, char* p, size_t p_sz) {
  protocol_stream_t* s = mpack_tree_context(tree);

  ssize_t rc = s->read_cb(s->usr, p, p_sz);
  if(rc < 0) {
    L(log_error("read() failed: `%s`", strerror(errno)));
    mpack_tree_flag_error(tree, mpack_error_io);
  } else if(rc == 0) {
    X(log_debug("read() eof"));
    mpack_tree_flag_error(tree, mpack_error_eof);
  }
  return (rc);
}

protocol_stream_t* blb_protocol_stream_new(
    void* usr,
    ssize_t (*read_cb)(void* usr, char* p, size_t p_sz),
    size_t max_sz,
    size_t max_nodes) {
  protocol_stream_t* s = blb_new(protocol_stream_t);
  if(s == NULL) {
    L(log_error("blb_new() failed"));
    return (NULL);
  }
  s->read_cb = read_cb;
  s->usr = usr;
  mpack_tree_init_stream(
      &s->tree, blb_protocol_stream_cb, s, max_sz, max_nodes);
  return (s);
}

void blb_protocol_stream_teardown(protocol_stream_t* stream) {
  if(stream == NULL) { return; }
  mpack_tree_destroy(&stream->tree);
  blb_free(stream);
}

static int blb_protocol_decode_input(
    protocol_stream_t* stream, mpack_node_t payload, protocol_message_t* out) {
  const char* p = mpack_node_bin_data(payload);
  size_t p_sz = mpack_node_bin_size(payload);
  X(log_debug("encoded message len %zu", p_sz));
  if(p == NULL || p_sz == 0) {
    L(log_error("invalid message"));
    return (-1);
  }

  WHEN_X {
    theTrace_lock();
    for(size_t i = 0; i < p_sz; i++) {
      log_inject("%02x ", (int)(unsigned char)p[i]);
    }
    log_inject("\n");
    theTrace_release();
  }

  mpack_reader_t __rd = {0}, *rd = &__rd;
  mpack_reader_init(rd, (char*)p, p_sz, p_sz);

  uint32_t cnt = mpack_expect_map(rd);
  mpack_error_t map_ok = mpack_reader_error(rd);
  if(cnt != 7 || map_ok != mpack_ok) {
    L(log_error(
        "invalid inner message: map with `7` elements expected got `%u`", cnt));
    goto decode_error;
  }

  ASSERT(cnt < PROTOCOL_SCRTCH_BUFFERS);

  protocol_input_request_t* i = &out->u.input;
  out->ty = PROTOCOL_INPUT_REQUEST;
  uint32_t w = 0;
  for(uint32_t j = 0; j < cnt; j++) {
    char key[1] = {'\0'};
    (void)mpack_expect_str_buf(rd, key, 1);
    switch(key[0]) {
    case PROTOCOL_PDNS_ENTRY_COUNT_KEY0: {
      X(log_debug("got input request count"));
      i->entry.count = mpack_expect_uint(rd);
      break;
    }
    case PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY0: {
      X(log_debug("got input request first seen"));
      mpack_timestamp_t ts = mpack_expect_timestamp(rd);
      i->entry.first_seen = ts.seconds;
      break;
    }
    case PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY0: {
      X(log_debug("got input request last seen"));
      mpack_timestamp_t ts = mpack_expect_timestamp(rd);
      i->entry.last_seen = ts.seconds;
      break;
    }
    case PROTOCOL_PDNS_ENTRY_RRNAME_KEY0: {
      X(log_debug("got input request rrname"));
      i->entry.rrname_len =
          mpack_expect_str_buf(rd, stream->scrtch[w], PROTOCOL_SCRTCH_SZ);
      i->entry.rrname = stream->scrtch[w];
      w++;
      break;
    }
    case PROTOCOL_PDNS_ENTRY_RRTYPE_KEY0: {
      X(log_debug("got input request rrtype"));
      i->entry.rrtype_len =
          mpack_expect_str_buf(rd, stream->scrtch[w], PROTOCOL_SCRTCH_SZ);
      i->entry.rrtype = stream->scrtch[w];
      w++;
      break;
    }
    case PROTOCOL_PDNS_ENTRY_RDATA_KEY0: {
      X(log_debug("got input request rdata"));
      i->entry.rdata_len =
          mpack_expect_str_buf(rd, stream->scrtch[w], PROTOCOL_SCRTCH_SZ);
      i->entry.rdata = stream->scrtch[w];
      w++;
      break;
    }
    case PROTOCOL_PDNS_ENTRY_SENSORID_KEY0: {
      X(log_debug("got input request sensorid"));
      i->entry.sensorid_len =
          mpack_expect_str_buf(rd, stream->scrtch[w], PROTOCOL_SCRTCH_SZ);
      i->entry.sensorid = stream->scrtch[w];
      w++;
      break;
    }
    default:
      L(log_error(
          "invalid inner message: invalid key: %02x",
          (int)(unsigned char)key[0]));
      return (-1);
    }
  }

  mpack_done_map(rd);
  if(mpack_reader_error(rd) != mpack_ok) {
    L(log_error("invalid inner message; map decode failed"));
    goto decode_error;
  }

  mpack_reader_destroy(rd);
  return (0);

decode_error:
  mpack_reader_destroy(rd);
  return (-1);
}

static int blb_protocol_decode_query(
    protocol_stream_t* stream, mpack_node_t payload, protocol_message_t* out) {
  const char* p = mpack_node_bin_data(payload);
  size_t p_sz = mpack_node_bin_size(payload);
  X(log_debug("encoded message len %zu", p_sz));
  if(p == NULL || p_sz == 0) {
    L(log_error("invalid message"));
    return (-1);
  }

  WHEN_X {
    theTrace_lock();
    for(size_t i = 0; i < p_sz; i++) {
      log_inject("%02x ", (int)(unsigned char)p[i]);
    }
    log_inject("\n");
    theTrace_release();
  }

  mpack_reader_t __rd = {0}, *rd = &__rd;
  mpack_reader_init(rd, (char*)p, p_sz, p_sz);

  uint32_t cnt = mpack_expect_map(rd);
  if(cnt != 9 || mpack_reader_error(rd) != mpack_ok) {
    L(log_error("invalid inner message: query map expected"));
    goto decode_error;
  }

  ASSERT(cnt < PROTOCOL_SCRTCH_BUFFERS);

  struct have_t {
    bool hrrname;
    bool hrdata;
    bool hrrtype;
    bool hsensorid;
  };

  struct have_t __h = {0}, *h = &__h;
  protocol_query_request_t* q = &out->u.query;
  out->ty = PROTOCOL_QUERY_REQUEST;
  uint32_t w = 0;
  for(uint32_t j = 0; j < cnt; j++) {
    char key[64] = {'\0'};
    size_t key_len = mpack_expect_str_buf(rd, key, sizeof(key));
    if(key_len <= 0) {
      L(log_error("invalid inner message: invalid key"));
      goto decode_error;
    }
    if(strncmp(key, PROTOCOL_QUERY_REQUEST_LIMIT_KEY, key_len) == 0) {
      X(log_debug("got query request limit"));
      q->limit = mpack_expect_int(rd);
    } else if(strncmp(key, PROTOCOL_QUERY_REQUEST_QRRNAME_KEY, key_len) == 0) {
      X(log_debug("got input request rrname"));
      q->qrrname_len =
          mpack_expect_str_buf(rd, stream->scrtch[w], PROTOCOL_SCRTCH_SZ);
      q->qrrname = stream->scrtch[w];
      w++;
    } else if(strncmp(key, PROTOCOL_QUERY_REQUEST_HRRNAME_KEY, key_len) == 0) {
      X(log_debug("got input request have rrname"));
      h->hrrname = mpack_expect_bool(rd);
    } else if(strncmp(key, PROTOCOL_QUERY_REQUEST_QRRTYPE_KEY, key_len) == 0) {
      X(log_debug("got input request rrtype"));
      q->qrrtype_len =
          mpack_expect_str_buf(rd, stream->scrtch[w], PROTOCOL_SCRTCH_SZ);
      q->qrrtype = stream->scrtch[w];
      w++;
    } else if(strncmp(key, PROTOCOL_QUERY_REQUEST_HRRTYPE_KEY, key_len) == 0) {
      X(log_debug("got input request have rrtype"));
      h->hrrtype = mpack_expect_bool(rd);
    } else if(strncmp(key, PROTOCOL_QUERY_REQUEST_QRDATA_KEY, key_len) == 0) {
      X(log_debug("got input request rdata"));
      q->qrdata_len =
          mpack_expect_str_buf(rd, stream->scrtch[w], PROTOCOL_SCRTCH_SZ);
      q->qrdata = stream->scrtch[w];
      w++;
    } else if(strncmp(key, PROTOCOL_QUERY_REQUEST_HRDATA_KEY, key_len) == 0) {
      X(log_debug("got input request have rdata"));
      h->hrdata = mpack_expect_bool(rd);
    } else if(
        strncmp(key, PROTOCOL_QUERY_REQUEST_QSENSORID_KEY, key_len) == 0) {
      X(log_debug("got input request sensorid"));
      q->qsensorid_len =
          mpack_expect_str_buf(rd, stream->scrtch[w], PROTOCOL_SCRTCH_SZ);
      q->qsensorid = stream->scrtch[w];
      w++;
    } else if(
        strncmp(key, PROTOCOL_QUERY_REQUEST_HSENSORID_KEY, key_len) == 0) {
      X(log_debug("got input request have sensorid"));
      h->hsensorid = mpack_expect_bool(rd);
    } else {
      L(log_error(
          "invalid inner message: unkown key: `%.*s`", (int)key_len, key));
      goto decode_error;
    }
  }
  mpack_done_map(rd);
  if(mpack_reader_error(rd) != mpack_ok) {
    L(log_error("invalid inner message; decode query request failed"));
    goto decode_error;
  }

  if(!h->hsensorid) { q->qsensorid_len = 0; }
  if(!h->hrrname) { q->qrrname_len = 0; }
  if(!h->hrrtype) { q->qrrtype_len = 0; }
  if(!h->hrdata) { q->qrdata_len = 0; }

  mpack_reader_destroy(rd);
  return (0);

decode_error:
  mpack_reader_destroy(rd);
  return (-1);
}

static int blb_protocol_decode_backup(
    protocol_stream_t* stream, mpack_node_t payload, protocol_message_t* out) {
  const char* p = mpack_node_bin_data(payload);
  size_t p_sz = mpack_node_bin_size(payload);
  X(log_debug("encoded message len %zu", p_sz));
  if(p == NULL || p_sz == 0) {
    L(log_error("invalid message"));
    return (-1);
  }

  WHEN_X {
    theTrace_lock();
    for(size_t i = 0; i < p_sz; i++) {
      log_inject("%02x ", (int)(unsigned char)p[i]);
    }
    log_inject("\n");
    theTrace_release();
  }

  mpack_reader_t __rd = {0}, *rd = &__rd;
  mpack_reader_init(rd, (char*)p, p_sz, p_sz);

  uint32_t cnt = mpack_expect_map(rd);
  if(cnt != 1 || mpack_reader_error(rd) != mpack_ok) {
    L(log_error("invalid inner message: backup map expected"));
    goto decode_error;
  }

  ASSERT(cnt < PROTOCOL_SCRTCH_BUFFERS);

  char key[1] = {'\0'};
  (void)mpack_expect_str_buf(rd, key, 1);
  if(key[0] != PROTOCOL_BACKUP_REQUEST_PATH_KEY[0]) {
    L(log_error("invalid inner message: path key expected"));
    goto decode_error;
  }

  protocol_backup_request_t* b = &out->u.backup;
  out->ty = PROTOCOL_BACKUP_REQUEST;
  b->path_len = mpack_expect_str_buf(rd, stream->scrtch[0], PROTOCOL_SCRTCH_SZ);
  b->path = stream->scrtch[0];

  mpack_done_map(rd);
  if(mpack_reader_error(rd) != mpack_ok) {
    L(log_error("invalid inner message; decode backup request failed"));
    goto decode_error;
  }

  mpack_reader_destroy(rd);
  return (0);

decode_error:
  mpack_reader_destroy(rd);
  return (-1);
}

static int blb_protocol_decode_dump(
    protocol_stream_t* stream, mpack_node_t payload, protocol_message_t* out) {
  const char* p = mpack_node_bin_data(payload);
  size_t p_sz = mpack_node_bin_size(payload);
  X(log_debug("encoded message len %zu", p_sz));
  if(p == NULL || p_sz == 0) {
    L(log_error("invalid message"));
    return (-1);
  }

  WHEN_X {
    theTrace_lock();
    for(size_t i = 0; i < p_sz; i++) {
      log_inject("%02x ", (int)(unsigned char)p[i]);
    }
    log_inject("\n");
    theTrace_release();
  }

  mpack_reader_t __rd = {0}, *rd = &__rd;
  mpack_reader_init(rd, (char*)p, p_sz, p_sz);

  uint32_t cnt = mpack_expect_map(rd);
  if(cnt != 1 || mpack_reader_error(rd) != mpack_ok) {
    L(log_error("invalid inner message: dump map expected"));
    goto decode_error;
  }

  ASSERT(cnt < PROTOCOL_SCRTCH_BUFFERS);

  char key[1] = {'\0'};
  (void)mpack_expect_str_buf(rd, key, 1);
  if(key[0] != PROTOCOL_DUMP_REQUEST_PATH_KEY[0]) {
    L(log_error("invalid inner message: path key expected"));
    goto decode_error;
  }

  protocol_dump_request_t* d = &out->u.dump;
  out->ty = PROTOCOL_DUMP_REQUEST;
  d->path_len = mpack_expect_str_buf(rd, stream->scrtch[0], PROTOCOL_SCRTCH_SZ);
  d->path = stream->scrtch[0];

  mpack_done_map(rd);
  if(mpack_reader_error(rd) != mpack_ok) {
    L(log_error("invalid inner message; decode dump request failed"));
    goto decode_error;
  }

  mpack_reader_destroy(rd);
  return (0);

decode_error:
  mpack_reader_destroy(rd);
  return (-1);
}

static int blb_protocol_decode_stream_start(
    protocol_stream_t* stream, mpack_node_t payload, protocol_message_t* out) {
  const char* p = mpack_node_bin_data(payload);
  size_t p_sz = mpack_node_bin_size(payload);
  X(log_debug("encoded message len %zu", p_sz));
  if(p != NULL || p_sz != 0) {
    L(log_error("invalid message"));
    return (-1);
  }
  (void)stream;
  out->ty = PROTOCOL_QUERY_STREAM_START_RESPONSE;
  return (0);
}

static int blb_protocol_decode_stream_end(
    protocol_stream_t* stream, mpack_node_t payload, protocol_message_t* out) {
  const char* p = mpack_node_bin_data(payload);
  size_t p_sz = mpack_node_bin_size(payload);
  X(log_debug("encoded message len %zu", p_sz));
  if(p != NULL || p_sz != 0) {
    L(log_error("invalid message"));
    return (-1);
  }
  (void)stream;
  out->ty = PROTOCOL_QUERY_STREAM_END_RESPONSE;
  return (0);
}

static int blb_protocol_decode_stream_data(
    protocol_stream_t* stream, mpack_node_t payload, protocol_message_t* out) {
  int rc = blb_protocol_decode_input(stream, payload, out);
  if(rc != 0) { return (rc); }
  out->ty = PROTOCOL_QUERY_STREAM_DATA_RESPONSE;
  return (0);
}

int blb_protocol_stream_decode(
    protocol_stream_t* stream, protocol_message_t* out) {
  mpack_tree_t* tree = &stream->tree;
  mpack_tree_parse(tree);
  switch(mpack_tree_error(tree)) {
  case mpack_ok: break;
  case mpack_error_eof: return (-1);
  default: return (-2);
  }

  mpack_node_t root = mpack_tree_root(tree);
  WHEN_X {
    theTrace_lock();
    log_debug(
        "got message kv-pairs=%zu tree-size=%zu",
        mpack_node_map_count(root),
        mpack_tree_size(tree));
    for(size_t i = 0; i < mpack_node_map_count(root); i++) {
      mpack_node_t key = mpack_node_map_key_at(root, i);
      log_debug("key[%zu]=%.*s", i, 1, mpack_node_str(key));
    }
    theTrace_release();
  }

  mpack_node_t type =
      mpack_node_map_cstr(root, PROTOCOL_TYPED_MESSAGE_TYPE_KEY);
  mpack_node_t payload =
      mpack_node_map_cstr(root, PROTOCOL_TYPED_MESSAGE_ENCODED_KEY);
  if(mpack_node_is_nil(type) || mpack_node_is_nil(payload)) {
    L(log_error("invalid message received"));
    return (-1);
  }
  switch(mpack_node_int(type)) {
  case PROTOCOL_INPUT_REQUEST:
    X(log_debug("got input request"));
    return (blb_protocol_decode_input(stream, payload, out));
  case PROTOCOL_QUERY_REQUEST:
    X(log_debug("got query request"));
    return (blb_protocol_decode_query(stream, payload, out));
  case PROTOCOL_BACKUP_REQUEST:
    X(log_debug("got backup request"));
    return (blb_protocol_decode_backup(stream, payload, out));
  case PROTOCOL_DUMP_REQUEST:
    X(log_debug("got dump request"));
    return (blb_protocol_decode_dump(stream, payload, out));
  case PROTOCOL_QUERY_STREAM_START_RESPONSE:
    X(log_debug("got stream start response"));
    return (blb_protocol_decode_stream_start(stream, payload, out));
  case PROTOCOL_QUERY_STREAM_END_RESPONSE:
    X(log_debug("got stream end response"));
    return (blb_protocol_decode_stream_end(stream, payload, out));
  case PROTOCOL_QUERY_STREAM_DATA_RESPONSE:
    X(log_debug("got stream data response"));
    return (blb_protocol_decode_stream_data(stream, payload, out));
  default: L(log_error("invalid message type")); return (-1);
  }
}

protocol_dump_stream_t* blb_protocol_dump_stream_new(FILE* file) {
  protocol_dump_stream_t* stream = blb_new(protocol_dump_stream_t);
  if(stream == NULL) { return (NULL); }
  mpack_reader_init_stdfile(&stream->reader, file, 0);
  return (stream);
}

void blb_protocol_dump_stream_teardown(protocol_dump_stream_t* stream) {
  mpack_reader_destroy(&stream->reader);
  blb_free(stream);
}

int blb_protocol_dump_stream_decode(
    protocol_dump_stream_t* stream, protocol_entry_t* entry) {
  mpack_reader_t* rd = &stream->reader;
  uint32_t cnt = mpack_expect_map(rd);
  mpack_error_t map_ok = mpack_reader_error(rd);
  if(map_ok != mpack_ok) {
    if(map_ok == mpack_error_eof) {
      V(log_debug("dump finished; eof reached"));
      return (-1);
    } else {
      L(log_error("mpack decode error `%d`", map_ok));
      return (-2);
    }
  }
  if(cnt != OBS_FIELDS) {
    L(log_error(
        "expected map with `%u` entries but got `%u` entries",
        OBS_FIELDS,
        cnt));
    return (-2);
  }
  bytestring_sink_t sink = bs_sink(stream->scrtch, PROTOCOL_SCRTCH_SZ);
  for(uint32_t i = 0; i < cnt; i++) {
    uint32_t field = mpack_expect_uint(rd);
    switch(field) {
    case OBS_RRTYPE_IDX: {
      size_t sz = mpack_expect_bin_buf(rd, (char*)sink.p, sink.available);
      entry->rrtype = (const char*)sink.p;
      entry->rrtype_len = sz;
      sink = bs_sink_slice0(&sink, sz);
      if(sink.p == 0) { return (-2); }
      break;
    }
    case OBS_RDATA_IDX: {
      size_t sz = mpack_expect_bin_buf(rd, (char*)sink.p, sink.available);
      entry->rdata = (const char*)sink.p;
      entry->rdata_len = sz;
      sink = bs_sink_slice0(&sink, sz);
      if(sink.p == 0) { return (-2); }
      break;
    }
    case OBS_SENSOR_IDX: {
      size_t sz = mpack_expect_bin_buf(rd, (char*)sink.p, sink.available);
      entry->sensorid = (const char*)sink.p;
      entry->sensorid_len = sz;
      sink = bs_sink_slice0(&sink, sz);
      if(sink.p == 0) { return (-2); }
      break;
    }
    case OBS_RRNAME_IDX: {
      size_t sz = mpack_expect_bin_buf(rd, (char*)sink.p, sink.available);
      entry->rrname = (const char*)sink.p;
      entry->rrname_len = sz;
      sink = bs_sink_slice0(&sink, sz);
      if(sink.p == 0) { return (-2); }
      break;
    }
    case OBS_COUNT_IDX: entry->count = mpack_expect_uint(rd); break;
    case OBS_LAST_SEEN_IDX: entry->last_seen = mpack_expect_uint(rd); break;
    case OBS_FIRST_SEEN_IDX: entry->first_seen = mpack_expect_uint(rd); break;
    default: L(log_error("unknown field index `%u`", field)); return (-1);
    }
  }

  mpack_done_map(rd);
  mpack_error_t err = mpack_reader_error(rd);
  switch(err) {
  case mpack_ok: return (0);
  case mpack_error_eof: return (-1);
  default: return (-2);
  }
}

void blb_protocol_log_entry(const protocol_entry_t* entry) {
  log_debug(
      "entry{ `%.*s` `%.*s` `%.*s` `%.*s` `%u` `%u` `%u` }",
      (int)entry->rrname_len,
      entry->rrname,
      (int)entry->rrtype_len,
      entry->rrtype,
      (int)entry->rdata_len,
      entry->rdata,
      (int)entry->sensorid_len,
      entry->sensorid,
      entry->count,
      entry->first_seen,
      entry->last_seen);
}