
# `balboa` Frontend <-> Backend (ad-hoc) Protocol

all messges between the frontend and backend are encoded using the [msgpack][1]
format. We use a *double* encoding of *outer* and *inner* messages to be
able to add compression and authentication later on.

```
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

dependent on the type field the `payload` field contains the actual *inner*
[msgpack][1] encoded message (using msgpack encoded data in `encoded_message`
is not mandatory, could be plain text json as well).

all messages are delivered asynchronous and are not explicitly ack'ed.

[1]: https://msgpack.org/

# Inner Messages

## Input Request Message

```
struct observation{
    rrname: bytestring where field="N"
    rdata: bytestring where field="D"
    rrtype: bytestring where field="T"
    sensorid: bytestring where field="I"
    count: uint32 where field="C"
    first_seen: timestamp_in_seconds where field="F"
    last_seen: timestamp_in_seconds where field="L"
}
struct input_message{
    observations: array(observation) where field="O"
}
```

## Query Request Message

## Query Response Message

## Query Stream Response Start

## Query Stream Response Data

## Query Stream Response End

## Dump Request Message

## Backup Request Message

