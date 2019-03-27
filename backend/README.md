# balboa backends

This directory contains the code for database backends leveraged by
[balboa](https://www.github.colm/DCSO/balboa)

## Building and Installation

### RocksDB Backend

First, install the required depencencies. Aside from `make` and a working `gcc`
installation, `rocksdb` development files are necessary.

On Debian (testing and stretch-backports), one can satisfy these dependencies
with:

```text
% apt install librocksdb-dev
...
```

On Void Linux

```text
% xbps-install rocksdb-devel
...
```

Building the RocksDB backend:

```text
$ cd backend/balboa/balboa-rocksdb
$ make
...
```

This yields the backend binary `build/linux/balboa-rocksdb`.

### Usage

Show available parameters to the RocksDB backend:

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
    --version show version then exit
```

Now start *balboa* and the backend to feed pDNS observations into it:

```text
$ balboa-rocksdb -p 4242 -h 127.0.0.1 -d /tmp/balboa-rocksdb -D
$ balboa serve -l '' -host 127.0.0.1:4242 -f my-feeders.yaml
...
```

### Other tools

#### balboa-backend-console

`balboa-backend-console` is a small utility managing balboa backends. It speaks
the *backend protocol* directly. You can `backup`, `dump` and `replay`
databases. Building is as easy as:

```text
$ cd backend/balboa-backend-console
$ make
...
```

assuming `make` and `gcc` are working on your system. It drops the self
contained binary `build/linux/balboa-backend-console`.

```text
$ balboa-backend-console -h
`balboa-backend-console` is a management tool for `balboa-backends`

Usage: balboa-backend-console <--version|help|jsonize|dump|replay> [options]

Command help:
    show help

Command jsonize:
    read a dump file and print all entries as json

    -d <path> path to the dump file to read

Command dump:
    connect to a `balboa-backend` and request a dump of all data to local stdout

    -h <host> ip address of the `balboa-backend` (default: 127.0.0.1)
    -p <port> port of the `balboa-backend` (default: 4242)
    -v increase verbosity; can be passed multiple times
    -d <remote-dump-path> unused/ignored (default: -)

Command replay:
    replay a previously generated database dump

    -d <path> database dump file or `-` for stdin (default: -)
    -h <host> ip address of the `balboa-backend` (default: 127.0.0.1)
    -p <port> port of the `balboa-backend` (default: 4242)
    -v increase verbosity; can be passed multiple times

Examples:

balboa-backend-console jsonize -r /tmp/pdns.dmp
lz4cat /tmp/pdns.dmp.lz4 | balboa-backend-console jsonize
```

#### balboa-rocksdb-v1-dump

`balboa-rocksdb-v1-dump` is a small utility dealing with migration from
*balboa* v1 databases to the new and internal format of v2.

```
$ cd backend/balboa-rocksdb-v1-dump
$ make
...
```

After a successful build, the binary is located at
`build/linux/balboa-rocksdb-v1-dump`.

## Migrating from balboa/rocksdb v1

`balboa` v1 uses a different format to store data in the `rocksdb` backend. In
order not to loose all of our precicous pDNS observations we need a migration
strategy. The suggested approach looks a bit cumbersome but is in part due to
the reason `rocksdb` does not yet support hot backup.

Let's assume you have a moderatly large pDNS observation database stored in
`/data/balboa-rocksdb-v1`.

```text
$ du -s /data/balboa-rocksdb-v1
42GB
```

Now, we want to migrate this database to balboa v2. The feeder configuration
remains the same. Start the new balboa backend and frontend.

```text
$ balboa-rocksdb -d /data/balboa-rocksdb-v2 -h 127.0.0.1 -p 4242
$ balboa serve -l '' -f my-feeders.yaml -host 127.0.0.1:4242
...
```

Stop the old balboa v1 service and dump the database:

```text
$ balboa-rocksdb-v1-dump dump /data/balboa-rocksdb | lz4 > /data/pdns-backup.dmp.lz4
$ lz4cat /data/pdns-backup.dmp.lz4 | balboa-backend-console replay -h 127.0.0.1 -p 4242
...
```

`balboa-rocksdb-v1-dump` will dump the observation entries to stdout which in
turn gets redirected to the `balboa-backend-console` tool in `replay` mode
(reading from stdin in this case, use `-d <path>` to read from dump file)

Wait some time. Done.

## Author/Contact

- Sascha Steinbiss
- Alexander Wamack

## License

BSD-3-clause
