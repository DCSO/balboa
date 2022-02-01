# 📑 balboa

![Build Status](https://github.com/DCSO/balboa/actions/workflows/go.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/DCSO/balboa)](https://goreportcard.com/report/github.com/DCSO/balboa)


balboa is the BAsic Little Book Of Answers. It consumes and indexes
observations from [passive DNS](https://www.farsightsecurity.com/technical/passive-dns/)
collection, providing a [GraphQL](https://graphql.org/) interface to access
the aggregated contents of the observations database. We built balboa to handle
passive DNS data aggregated from metadata gathered by
[Suricata](https://suricata-ids.org).

The API should be suitable for integration into existing multi-source
observable integration frameworks. It is possible to produce results in a
[Common Output Format](https://datatracker.ietf.org/doc/draft-dulaunoy-dnsop-passive-dns-cof/)
compatible schema using either a GraphQL API (see below) or a REST API compatible with
[CIRCL's](https://www.circl.lu/services/passive-dns/).

The balboa software...

- is fast for queries and input/updates
- implements storage using pluggable backends, potentially on separate machines
- supports tracking and specifically querying multiple sensors
- makes use of multiple cores for query and ingest
- accepts input from multiple sources simultaneously
  - HTTP (POST)
  - AMQP
  - Unix socket
  - network socket (NMSG format only)
- can tag and filter observations based on various properties
- can store observations to one or multiple backends based on matched selectors
- accepts various input formats
  - JSON-based
    - [FEVER](https://github.com/DCSO/fever)
    - [gopassivedns](https://github.com/Phillipmartin/gopassivedns)
    - [Packetbeat](https://www.elastic.co/guide/en/beats/packetbeat/master/packetbeat-dns-options.html) (via
      Logstash)
    - [Suricata EVE DNS v1 and v2](http://suricata.readthedocs.io/en/latest/output/eve/eve-json-format.html#event-type-dns)
  - flat text file
    - Edward Fjellskål's [PassiveDNS](https://github.com/gamelinux/passivedns) tabular format (default order `-f SMcsCQTAtn`)
  - binary
    - Farsight Security [NMSG format](https://www.farsightsecurity.com/txt-record/2015/01/28/nmsg-intro/) via network socket

## Building and Installation

```text
$ go get github.com/DCSO/balboa/cmd/balboa
...
```
This will drop a `balboa` executable in your Go bin path.

To build the backends:

```text
$ cd $GOPATH/src/github.com/DCSO/balboa/backend
$ make
...
```

This will create a binary executable in the `build/` subdirectories of each backends directory.

### Dependencies

- Go 1.10 or later
- For the bundled RocksDB backend: [RocksDB](https://rocksdb.org/) 5.0 or later (shared lib, with LZ4 support)

On Debian, for example, one can satisfy these dependencies with:

```text
% apt install golang-go librocksdb-dev
...
```

## Usage

### Configuring feeders

Feeders are used to get observations into the database. They run concurrently
and process inputs in the background, making results accessible via the query
interface as soon as the resulting upsert transactions have been completed in
the database. What feeders are to be created is defined in a YAML configuration
file (to be passed via the `-f` parameter to `balboa serve`). Example:

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

A balboa instance given this feeder configuration would support the following
input options:

- JSON in FEVER's aggregate format delivered via AMQP from a temporary queue
  attached to the exchange `tdh.pdns` on `localhost` port 5762, authenticated
  with user `guest` and password `guest`
- JSON in FEVER's aggregate format parsed from HTTP POST requests on port 8081 on the local system
- JSON in gopassivedns's format, fed into the UNIX socket `/tmp/balboa.sock` created by balboa

All of these feeders accept input simultaneously, there is no distinction made
as to where an observation has come from. It is possible to specify multiple
feeders of the same type but with different settings as long as their `name`s
are unique.

### Configuring selectors

Balboa provides a selector engine which can be used to select or filter observations.
The selector engine is configured in a YAML file which is provided via the `-s` parameter to balboa.

Available selector implementations:

* regex: match the `RRNAME` field of the observation with one or multiple selectors
* lua: process observations with lua scripts, see *selector.lua* for an example

Example:

```yaml
selectors:
  - name: Filter Unwanted TLDs
    type: regex
    mode: filter
    regexp:
      - unwanted_regex.txt
    tags:
      - filtered_tlds
  - name: CobaltStrike Regex
    type: regex
    mode: select
    regexp:
      - cobaltstrike_regex.txt
    ingest:
      - filtered_tlds
    tags:
      - possible_cobaltstrike
```

This configuration will tag all observations which are **not** matched by the regular expressions in `unwanted_regex.txt` with the tag `filtered_tlds`.
All observations which are tagged with `filtered_tlds` and which match one or more regular expressions in `cobaltstrike_regex.txt` are tagged with `possible_cobaltstrike`.

### Configuring the database backend

Multiple database backends are supported to store pDNS observations
persistently. Each database backend is provided as a self-contained binary
(executable). The frontend connects to exactly one database backend. The
backend, however, supports multiple client or frontend connections.
Each backend can either configure all observations (no `tags` parameter) or a list of tags (conditional or).

The backend configuration is defined in a YAML file (to be passed via the `-b` parameter to `balboa server`). Example:

```yaml
- name: cobaltstrike
  host: "localhost:4242"
  tags:
    - possible_cobaltstrike
- name: all filtered observations
  host: "localhost:4343"
  tags:
    - filtered_tlds
```

A balboa instance with this backend configuration will store all events tagged with `possible_cobaltstrike` to the backend
listening on port `localhost:4242` and all events tagged with `filtered_tlds` to the backend on `localhost:4343`.

### Running the backend and frontend services, consuming input

All interaction with the frontend on the command line takes place via the
`balboa` frontend executable. The frontend depends on a backend service,
which is usually its own executable.
For instance, the RocksDB backend can be started using:

```text
$ balboa-rocksdb -h
`balboa-rocksdb` provides a pdns database backend for `balboa`

Usage: balboa-rocksdb [options]

    -h display help
    -D daemonize (default: off)
    -d <path> path to rocksdb database (default: `/tmp/balboa-rocksdb`)
    -l listen address (default: 127.0.0.1)
    -p listen port (default: 4242)
    -v increase verbosity; can be passed multiple times
    -j thread throttle limit, maximum concurrent connections (default: 64)
    --membudget <memory-in-bytes> rocksdb membudget option (value: 134217728)
    --parallelism <number-of-threads> rocksdb parallelism option (value: 8)
    --max_log_file_size <size> rocksdb log file size option (value: 10485760)
    --max_open_files <number> rocksdb max number of open files (value: 300)
    --keep_log_file_num <number> rocksdb max number of log files (value: 2)
    --database_path <path> same as `-d`
    --version show version thenp exit

$ balboa-rocksdb --database_path /data/pdns -l 127.0.0.1 -p 4242
```

After starting the backend the `balboa` frontend can be started as follows:

```text
$ balboa serve -l ''
INFO[0000] starting feeder AMQPInput2
INFO[0000] starting feeder HTTP Input
INFO[0000] accepting submissions on port 8081
INFO[0000] starting feeder Socket Input
INFO[0000] starting feeder Suricata Socket Input
INFO[0000] ConsumeFeed() starting
INFO[0000] serving GraphQL on port 8080
...
```

After startup, the feeders are free to be used for data ingest. For example,
one might do some of the following to test data consumption (assuming the
feeders above are used):

- for AMQP:

    ```text
    $ scripts/mkjson.py | rabbitmqadmin publish routing_key="" exchange=tdh.pdns
    ...
    ```

- for HTTP:

    ```text
    $ scripts/mkjson.py | curl -d@- -qs --header "X-Sensor-ID: abcde" http://localhost:8081/submit
    ...
    ```

- for socket:

    ```text
    $ sudo gopassivedns -dev eth0 | socat /tmp/balboa.sock STDIN
    ...
    ```

### Querying the server

The intended main interface for interacting with the server is via GraphQL. For
example, the query

```graphql
query {
  entries(rrname: "test.foobar.de", sensor_id: "abcde", limit: 1) {
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

This also works with `rdata` as the query parameter, but at least one of
`rrname` or `rdata` must be stated. If there is no `sensor_id` parameter, then
all results will be returned regardless of where the DNS answer was observed.
Use the `time_first_rfc3339` and `time_last_rfc3339` instead of `time_first`
and `time_last`, respectively, to get human-readable timestamps.

When multiple backends are configured a query will be dispatched to every backend.
Accordingly, when an observation is stored in multiple backends, the result to the query
will contain duplicates.

### Aliases

Sometimes it is interesting to ask for all the domain names that resolve to the
same IP address. For this reason, the GraphQL API supports a virtual `aliases`
field that returns all Entries with RRType `A` or `AAAA` that share the same
address in the Rdata field.

Example:

```graphql
{
  entries(rrname: "heise.de", rrtype: A) {
    rrname
    rdata
    rrtype
    time_first_rfc3339
    time_last_rfc3339
    aliases {
      rrname
    }
  }
}
```

```json
{
  "data": {
    "entries": [
      {
        "rrname": "heise.de",
        "rdata": "193.99.144.80",
        "rrtype": "A",
        "time_first_rfc3339": "2018-07-10T08:05:45Z",
        "time_last_rfc3339": "2018-10-18T09:24:38Z",
        "aliases": [
          {
            "rrname": "ct.de"
          },
          {
            "rrname": "ix.de"
          },
          {
            "rrname": "redirector.heise.de"
          },
          {
            "rrname": "www.ix.de"
          }
        ]
      }
    ]
  }
}
```

### Bulk queries

There is also a shortcut tool to make 'bulk' querying easier. For example, to
get all the information on the hosts in range 1.2.0.0/16 as observed by sensor
`abcde`, one can use:

```text
$ balboa query --sensor abcde 1.2.0.0/16
{"count":6,"time_first":1531943211,"time_last":1531949570,"rrtype":"A","rrname":"test.foobar.de","rdata":"1.2.3.4","sensor_id":"abcde"}
{"count":1,"time_first":1531943215,"time_last":1531949530,"rrtype":"A","rrname":"baz.foobar.de","rdata":"1.2.3.7","sensor_id":"abcde"}
```

Note that this tool currently only does a lot of concurrent individual queries!
To improve performance in these cases it might be worthwhile to allow for range
queries on the server side as well in the future.

### Other tools

Run `balboa` without arguments to list available subcommands and get a short
description of what they do.

See also `README.md` in the `backend` directory.

## Author/Contact

Sascha Steinbiss

## License

BSD-3-clause
