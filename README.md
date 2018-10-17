# balboa [![Build Status](https://travis-ci.org/DCSO/balboa.svg?branch=master)](https://travis-ci.org/DCSO/balboa)

balboa is the BAsic Little Book Of Answers. It consumes and indexes observations from [passive DNS](https://www.farsightsecurity.com/technical/passive-dns/) collection, providing a [GraphQL](https://graphql.org/) interface to access the aggregated contents of the observations database. We built balboa to handle passive DNS data aggregated from metadata gathered by [Suricata](https://suricata-ids.org).

The API should be suitable for integration into existing multi-source observable integration frameworks. It is possible to produce results in a [Common Output Format](https://datatracker.ietf.org/doc/draft-dulaunoy-dnsop-passive-dns-cof/) compatible schema using the GraphQL API. In fact, the GraphQL schema is modelled after the COF fields.

The balboa software...

- is fast for queries and input/updates
- implements persistent, compressed storage of observations
  - as local storage
  - in a [Cassandra](https://cassandra.apache.org) cluster
- supports tracking and specifically querying multiple sensors
- makes use of multi-core systems
- can accept input from multiple sources simultaneously
  - HTTP (POST)
  - AMQP
  - GraphQL
  - Unix socket
- accepts various (text-based) input formats
  - JSON-based
      - FEVER
      - [gopassivedns](https://github.com/Phillipmartin/gopassivedns)
      - [Packetbeat](https://www.elastic.co/guide/en/beats/packetbeat/master/packetbeat-dns-options.html) (via Logstash)
      - [Suricata EVE DNS v1 and v2](http://suricata.readthedocs.io/en/latest/output/eve/eve-json-format.html#event-type-dns)
  - flat file
      - Edward Fjellskål's [PassiveDNS](https://github.com/gamelinux/passivedns) tabular format (default order `-f SMcsCQTAtn`)

## Building and Installation

```
$ go get github.com/DCSO/balboa
```

Dependencies:

- Go 1.7 or later
- [RocksDB](https://rocksdb.org/) 5.0 or later (shared lib)
- [tpl](https://troydhanson.github.io/tpl/index.html) (shared lib)

On Debian (testing), one can satisfy these dependencies with:

```
apt install librocksdb5.14 libtpl0
```

## Usage

### Configuring feeders

Feeders are used to get observations into the database. They run concurrently and process inputs in the background, making results accessible via the query interface as soon as the resulting upsert transactions have been completed in the database. What feeders are to be created is defined in a YAML configuration file (to be passed via the `-f` parameter to `balboa serve`). Example:

```yaml
feeder:
    - name: AMQP Input
      type: amqp
      url: amqp://guest:guest@localhost:5672
      exchange: [ tdh.pdns ]
      input_format: fever_aggregate
    - name: HTTP Input
      type: http
      listen_host: 127.0.0.1
      listen_port: 8081
      input_format: fever_aggregate
    - name: Socket Input
      type: socket
      path: /tmp/balboa.sock
      input_format: gopassivedns
```

A balboa instance given this feeder configuration would support the following input options:

- JSON in FEVER's aggregate format delivered via AMQP from a temporary queue attached to the exchange `tdh.pdns` on `localhost` port 5762, authenticated with user `guest` and password `guest`
- JSON in FEVER's aggregate format parsed from HTTP POST requests on port 8081 on the local system
- JSON in gopassivedns's format, fed into the UNIX socket `/tmp/balboa.sock` created by balboa

All of these feeders accept input simultaneously, there is no distinction made as to where an observation has come from. It is possible to specify multiple feeders of the same type but with different settings as long as their `name`s are unique.

### Configuring the database backend

Multiple database backends are supported to store pDNS observations persistently. The one to use in a particular instance can be configured separately in another YAML file (to be passed via the `-d` parameter to `balboa serve`). For example, to use the RocksDB backend storing data in /tmp/balboa, one would specify:

```yaml
database:
    name: Local RocksDB
    type: rocksdb
    db_path: /tmp/balboa
```

To alternatively, for instance, send all data to a Cassandra cluster, use the following configuration:

```yaml
database:
    name: Cassandra cluster
    type: cassandra
    hosts: [ "127.0.0.1", "127.0.0.2", "127.0.0.3" ]
```

which would use the specified nodes to access the cluster. Only one database can be configured at a time.

### Running the server and consuming input

All interaction with the service on the command line takes place via the `balboa` executable. The server can be started using:

```
$ balboa serve -l ''
INFO[0000] Local RocksDB: memory budget empty, using default of 128MB
INFO[0000] starting database Local RocksDB
INFO[0000] opening database...
INFO[0000] database ready
INFO[0000] starting feeder AMQPInput2
INFO[0000] starting feeder HTTP Input
INFO[0000] accepting submissions on port 8081
INFO[0000] starting feeder Socket Input
INFO[0000] starting feeder Suricata Socket Input
INFO[0000] serving GraphQL on port 8080
```

Depending on the size of the existing database, the start-up time ("opening database...") might be rather long if there is a lot of log to go through.

After startup, the feeders are free to be used for data ingest. For example, one might do some of the following to test data consumption (assuming the feeders above are used):

- for AMQP:
 
    ```
    $ scripts/mkjson.py | rabbitmqadmin publish routing_key="" exchange=tdh.pdns
    ```
    
- for HTTP:
    ```
    $ scripts/mkjson.py | curl -d@- -qs --header "X-Sensor-ID: abcde" http://localhost:8081/submit
    ```
    
- for socket:
    ```
    $ sudo gopassivedns -dev eth0 | socat /tmp/balboa.sock STDIN
    ```

Besides these asynchronous updates, it is always possible to use the `announceObservation` mutation in the GraphQL interface to explicitly add an observation, returning the updated data entry immediately:

```graphql
mutation {
  announceObservation(observation: {
    rrname: "test.foobar.de",
    rrtype: A,
    rdata: "1.2.3.4",
    time_first: 1531943211,
    time_last: 1531949570,
    sensor_id: "abcde",
    count: 3  
  }) {
    count
  }
}
```

This request would synchronously add a new observation with the given input data to the database and then return the new, updated `count` value.

### Querying the server

The intended main interface for interacting with the server is via GraphQL. For example, the query

```graphql
query {
  entries(rrname: "test.foobar.de", sensor_id: "abcde") {
    rrname
    rrtype
    rdata
    time_first
    time_last
    sensor_id
    count  
  }
}
```

would return something like

```json
{
  "data": {
    "entries": [
      {
        "rrname": "test.foobar.de",
        "rrtype": "A",
        "rdata": "1.2.3.4",
        "time_first": 1531943211,
        "time_last": 1531949570,
        "sensor_id": "abcde",
        "count": 3
      }
    ]
  }
}
```

This also works with `rdata` as the query parameter, but at least one of `rrname` or `rdata` must be stated. If there is no `sensor_id` parameter, then all results will be returned regardless of where the DNS answer was observed. 

There is also some shortcut tool to make 'bulk' querying easier. For example, to get all the information on the hosts in range 1.2.0.0/16 as observed by sensor `abcde`, one can use:

```
$ balboa query --sensor abcde 1.2.0.0/16
{"count":6,"time_first":1531943211,"time_last":1531949570,"rrtype":"A","rrname":"test.foobar.de","rdata":"1.2.3.4","sensor_id":"abcde"}
{"count":1,"time_first":1531943215,"time_last":1531949530,"rrtype":"A","rrname":"baz.foobar.de","rdata":"1.2.3.7","sensor_id":"abcde"}
```
Note that this tool currently only does a lot of concurrent individual queries! To improve performance in these cases it might be worthwhile to allow for range queries on the server side as well in the future.

## Author/Contact

Sascha Steinbiss

## License

BSD-3-clause
