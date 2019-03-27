
# `balboa` Frontend <-> Backend (ad-hoc) Protocol

All messges between the frontend and backend are encoded using the [msgpack][1]
format. We use a *double* encoding of *outer* and *inner* messages to be
able to add compression and authentication later on.

```text
enum typed_message_id(int){
    PROTOCOL_INPUT_REQUEST=1
    PROTOCOL_QUERY_REQUEST=2
    PROTOCOL_BACKUP_REQUEST=3
    PROTOCOL_DUMP_REQUEST=4
    PROTOCOL_ERROR_RESPONSE=128
    PROTOCOL_QUERY_RESPONSE=129
    PROTOCOL_QUERY_STREAM_START_RESPONSE=130
    PROTOCOL_QUERY_STREAM_DATA_RESPONSE=131
    PROTOCOL_QUERY_STREAM_END_RESPONSE=132
}
// the typed outer message
struct typed_message{
    type: typed_message_id where field="T"
    encoded_message: bytestring where field="M"
}
```

Depending on the value of the `type` field, the `encoded_message` field contains
the actual *inner* [msgpack][1] encoded message (using msgpack encoded data in
`encoded_message` is not mandatory, could be plain text json as well).

All messages are delivered in an asynchronous fashion and are not explicitly ack'ed.

[1]: https://msgpack.org/

# Inner Messages

## Input Request Message

```text
struct pdns_entry{
    rrname: bytestring where field="N"
    rdata: bytestring where field="D"
    rrtype: bytestring where field="T"
    sensorid: bytestring where field="I"
    count: uint32 where field="C"
    first_seen: timestamp_in_seconds where field="F"
    last_seen: timestamp_in_seconds where field="L"
}
struct input_message{
    observations: array(pdns_entry) where field="O"
}
```

## Query Request Message

```text
struct query_request{
    qrrname: bytestring where field="Qrrname"
    have_rrname: bool where field="Hrrname"
    qrdata: bytestring where field="Qrdata"
    have_qrdata: bool where field="Hrdata"
    qrrtype: bytestring where field="Qrrtype"
    have_rrtype: bool where field="Hrrtype"
    qsensorid: bytestring where field="Qsensorid"
    have_sensorid: bool where field="Hsensorid"
    limit: int where field="Limit"
}
```

## Query Response Message

```text
struct query_response{
    observations: array(pdns_entry) where field="O"
}
```

## Query Stream Response Start

```text
struct query_stream_start_response{
    // empty
}
```

## Query Stream Response Data

```text
struct query_stream_data_response{
    embed pdns_entry
}
```

## Query Stream Response End

```text
struct query_stream_end_response{
    // empty
}
```

## Dump Request Message

```text
struct dump_request{
    // currently unused
    path: bytestring where field="P"
}
```

## Backup Request Message

```text
struct backup_request{
    // currently unused
    path: bytestring where field="P"
}
```
