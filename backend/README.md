# balboa-backends [![CircleCI](https://circleci.com/gh/DCSO/balboa-backends.svg?style=svg)](https://circleci.com/gh/DCSO/balboa-backends)

`balboa-backends` is the central repository for database backends leveraged by
[balboa](https://www.github.colm/DCSO/balboa)

## Building and Installation

```
$ go get github.com/DCSO/balboa-backends
```

All backends reside self contained in separate directories

```
├── backend
│   └── backend.go
├── backend-mock
│   └── main.go
├── backend-rocksdb
│   ├── lib
│   │   ├── obs_rocksdb.c
│   │   ├── obs_rocksdb.h
│   │   ├── rocksdb.go
│   │   └── rocksdb_test.go
│   └── main.go
├── LICENSE
└── README.md
```

Building the RocksDB backend:

```
cd balboa-backends/backend-rocksdb
go build
```

On Debian (testing and stretch-backports), one can satisfy these dependencies
with:

```
apt install librocksdb-dev
```

## Usage

Build and install `balboa`. Start a backend and configure `balboa` to use a
remote backend:

```yaml
# database.yaml
database:
    name: Remote Backend
    type: remote-backend
    host: "127.0.0.1:4242"
```

Fist start build and start the backend, in this case we use the RocksDB backend

```
cd balboa-backends/backend-rocksdb
go build
./backend-rocksdb -h
./backend-rocksdb
INFO[0000] opening database path=/tmp/balboa-rocksdb membudget=134217728
INFO[0000] alloc=7145568 tot=7212080 sys=71956728 lookups=0
INFO[0000] database opened successfully
INFO[0000] start listening on host=:4242
```

Now start *balboa* and feed pDNS observations into the backend

```
balboa serve -l '' -d database.yml -f my-feeders.yaml
```

### Other tools

You need the [balboa](https://www.github.colm/DCSO/balboa) frontend to be able
to communicate with the various backends.


## Migrating from balboa/rocksdb v1

`balboa` v1 uses a different format to store data in the `rocksdb` backend. In
order not to loose all of our precicous pDNS observations we need a migration
strategy. The suggested approach looks a bit cumbersome but is in part due to
the reason `rocksdb` does not yet support hot backup.

Let's assume you have a moderatly large pDNS observation database stored in
`/data/balboa-rocksdb-v1`.

```
du -s /data/balboa-rocksdb-v1
42GB
```

Now, we want to migrate this database to balboa v2. First you prepare the new
balboa frontend to speak to a remote backend:

database.yaml:
```yaml
database:
    name: Remote Backend
    type: remote-backend
    host: "127.0.0.1:4242"
```

The feeder configuration remains the same. Second, start the new balboa backend
matching the `database.yaml` configuration.

```
backend-rocksdb -path /data/balboa-rocksdb-v2 -host :4242
```

Stop the old monolithic balboa v1 service and immediately start the new one

```
balboa serve -l '' -f my-feeders.yaml
```

Now we get all current observations inserted into the database. But we also
want all the old observations be migrated to the new one. We can do this using
the `dump-rocksdb-v1` and `dump` tools:

```
dump-rocksdb-v1 dump /data/balboa-rocksdb-v2 | dump replay -h 127.0.0.1 -p 4242 -d -
```

`dump-rocksdb-v1` will dump the observation entries to stdout which in turn
gets redirected to the `dump` tool in `replay` mode (`-d -` tells `dump` to
read from stdin instead of a file).

Wait some time. Done.

## Author/Contact

- Sascha Steinbiss
- Alexander Wamack

## License

BSD-3-clause
