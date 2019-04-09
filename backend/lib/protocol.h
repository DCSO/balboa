// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>

#define PROTOCOL_INPUT_REQUEST 1
#define PROTOCOL_QUERY_REQUEST 2
#define PROTOCOL_BACKUP_REQUEST 3
#define PROTOCOL_DUMP_REQUEST 4
#define PROTOCOL_ERROR_RESPONSE 128
#define PROTOCOL_QUERY_RESPONSE 129
#define PROTOCOL_QUERY_STREAM_START_RESPONSE 130
#define PROTOCOL_QUERY_STREAM_DATA_RESPONSE 131
#define PROTOCOL_QUERY_STREAM_END_RESPONSE 132

typedef struct protocol_dump_request_t protocol_dump_request_t;
struct protocol_dump_request_t {
  const char* path;
  size_t path_len;
};

ssize_t blb_protocol_encode_dump_request(
    const protocol_dump_request_t* r, char* p, size_t p_sz );

typedef struct protocol_backup_request_t protocol_backup_request_t;
struct protocol_backup_request_t {
  const char* path;
  size_t path_len;
};

ssize_t blb_protocol_encode_backup_request(
    const protocol_backup_request_t* r, char* p, size_t p_sz );

typedef struct protocol_entry_t protocol_entry_t;
struct protocol_entry_t {
  const char* rdata;
  size_t rdata_len;
  const char* rrname;
  size_t rrname_len;
  const char* rrtype;
  size_t rrtype_len;
  const char* sensorid;
  size_t sensorid_len;
  uint32_t count;
  uint32_t first_seen;
  uint32_t last_seen;
};

typedef struct protocol_input_request_t protocol_input_request_t;
struct protocol_input_request_t {
  protocol_entry_t entry;
};

ssize_t blb_protocol_encode_input_request(
    const protocol_input_request_t* i, char* p, size_t p_sz );
ssize_t blb_protocol_encode_entry(
    const protocol_entry_t* i, char* p, size_t p_sz );

typedef struct protocol_query_request_t protocol_query_request_t;
struct protocol_query_request_t {
  int ty;
  const char* qrdata;
  size_t qrdata_len;
  const char* qrrname;
  size_t qrrname_len;
  const char* qrrtype;
  size_t qrrtype_len;
  const char* qsensorid;
  size_t qsensorid_len;
  int limit;
};

ssize_t blb_protocol_encode_query_request(
    const protocol_query_request_t* q, char* p, size_t p_sz );

ssize_t blb_protocol_encode_stream_start_response( char* p, size_t p_sz );
ssize_t blb_protocol_encode_stream_end_response( char* p, size_t p_sz );
ssize_t blb_protocol_encode_stream_entry(
    const protocol_entry_t* entry, char* p, size_t p_sz );
ssize_t blb_protocol_encode_dump_entry(
    const protocol_entry_t* entry, char* p, size_t p_sz );

typedef struct protocol_stream_t protocol_stream_t;
protocol_stream_t* blb_protocol_stream_new(
    void* usr,
    ssize_t ( *read_cb )( void* usr, char* p, size_t p_sz ),
    size_t max_sz,
    size_t max_nodes );

typedef struct protocol_message_t protocol_message_t;
struct protocol_message_t {
  int ty;
  union {
    protocol_input_request_t input;
    protocol_query_request_t query;
    protocol_backup_request_t backup;
    protocol_dump_request_t dump;
    protocol_entry_t entry;
  } u;
};
int blb_protocol_stream_decode(
    protocol_stream_t* stream, protocol_message_t* out );

void blb_protocol_stream_teardown( protocol_stream_t* stream );

typedef struct protocol_dump_stream_t protocol_dump_stream_t;
protocol_dump_stream_t* blb_protocol_dump_stream_new( FILE* f );

void blb_protocol_dump_stream_teardown( protocol_dump_stream_t* stream );

int blb_protocol_dump_stream_decode(
    protocol_dump_stream_t* stream, protocol_entry_t* entry );

#endif