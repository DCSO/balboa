
#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include <alloc.h>
#include <inttypes.h>
#include <stdlib.h>

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

#define PROTOCOL_INPUT_REQUEST 1
#define PROTOCOL_QUERY_REQUEST 2
#define PROTOCOL_BACKUP_REQUEST 3
#define PROTOCOL_DUMP_REQUEST 4
#define PROTOCOL_ERROR_RESPONSE 128
#define PROTOCOL_QUERY_RESPONSE 129
#define PROTOCOL_QUERY_STREAM_START_RESPONSE 130
#define PROTOCOL_QUERY_STREAM_DATA_RESPONSE 131
#define PROTOCOL_QUERY_STREAM_END_RESPONSE 132

#define PROTOCOL_TYPED_MESSAGE_TYPE_KEY ( "T" )
#define PROTOCOL_TYPED_MESSAGE_ENCODED_KEY ( "M" )

#define PROTOCOL_BACKUP_REQUEST_PATH_KEY ( "P" )

#define PROTOCOL_DUMP_REQUEST_PATH_KEY ( "P" )

#define PROTOCOL_QUERY_REQUEST_QRDATA_KEY ( "Qrdata" )
#define PROTOCOL_QUERY_REQUEST_QRRNAME_KEY ( "Qrrname" )
#define PROTOCOL_QUERY_REQUEST_QRRTYPE_KEY ( "Qrrtype" )
#define PROTOCOL_QUERY_REQUEST_QSENSORID_KEY ( "QsensorID" )
#define PROTOCOL_QUERY_REQUEST_HRDATA_KEY ( "Hrdata" )
#define PROTOCOL_QUERY_REQUEST_HRRNAME_KEY ( "Hrrname" )
#define PROTOCOL_QUERY_REQUEST_HRRTYPE_KEY ( "Hrrtype" )
#define PROTOCOL_QUERY_REQUEST_HSENSORID_KEY ( "HsensorID" )
#define PROTOCOL_QUERY_REQUEST_LIMIT_KEY ( "Limit" )

#define PROTOCOL_INPUT_REQUEST_OBSERVATION_KEY0 ( 'O' )

#define PROTOCOL_PDNS_ENTRY_RRNAME_KEY0 ( 'N' )
#define PROTOCOL_PDNS_ENTRY_RRTYPE_KEY0 ( 'T' )
#define PROTOCOL_PDNS_ENTRY_RDATA_KEY0 ( 'D' )
#define PROTOCOL_PDNS_ENTRY_SENSORID_KEY0 ( 'I' )
#define PROTOCOL_PDNS_ENTRY_COUNT_KEY0 ( 'C' )
#define PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY0 ( 'F' )
#define PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY0 ( 'L' )

#define PROTOCOL_PDNS_ENTRY_RRNAME_KEY ( "N" )
#define PROTOCOL_PDNS_ENTRY_RRTYPE_KEY ( "T" )
#define PROTOCOL_PDNS_ENTRY_RDATA_KEY ( "D" )
#define PROTOCOL_PDNS_ENTRY_SENSORID_KEY ( "I" )
#define PROTOCOL_PDNS_ENTRY_COUNT_KEY ( "C" )
#define PROTOCOL_PDNS_ENTRY_FIRSTSEEN_KEY ( "F" )
#define PROTOCOL_PDNS_ENTRY_LASTSEEN_KEY ( "L" )

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
  int count;
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

// static size_t blb_thread_read_stream_cb(
//    mpack_tree_t* tree, char* p, size_t p_sz ) {

typedef struct protocol_stream_t protocol_stream_t;
protocol_stream_t* blb_protocol_stream_new(
    void* usr,
    ssize_t ( *read_cb )( void* usr, char* p, size_t p_sz ),
    size_t max_sz,
    size_t max_nodes );

#endif