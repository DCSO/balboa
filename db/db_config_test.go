package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestDBConfigLoad(t *testing.T) {
	_, err := LoadSetup([]byte("foo["))
	if err == nil {
		t.Fatal("did not error with invalid data")
	}

	_, err = LoadSetup([]byte(""))
	if err == nil {
		t.Fatal("did not error with empty config")
	}

	_, err = LoadSetup([]byte(`
database:
    name: Whatever
`))
	if err == nil {
		t.Fatal("did not error with missing db type")
	}

	_, err = LoadSetup([]byte(`
database:
    name: Local RocksDB
    type: rocksdb
`))
	if err == nil {
		t.Fatal("did not error with missing path")
	}

	_, err = LoadSetup([]byte(`
database:
    name: Local RocksDB
    type: rocksdb
    db_path: /tmp/balboa
`))
	if err != nil {
		t.Fatal("does not accept regular config: ", err)
	}

	_, err = LoadSetup([]byte(`
database:
    name: Local RocksDB
    type: rocksdb
    db_path: /tmp/balboa
    membudget: 1000000
`))
	if err != nil {
		t.Fatal("does not accept regular config: ", err)
	}

	_, err = LoadSetup([]byte(`
database:
    name: Local Cassandra
    type: cassandra
    hosts: [ "127.0.0.1", "127.0.0.2", "127.0.0.3" ]
`))
	if err != nil {
		t.Fatal("does not accept regular config: ", err)
	}

	_, err = LoadSetup([]byte(`
database:
    name: Local Cassandra
    type: cassandra
`))
	if err == nil {
		t.Fatal("did not error with missing hosts")
	}
}

func TestDBConfigRunRocks(t *testing.T) {
	dbdir, err := ioutil.TempDir("", "example")
	if err != nil {
		t.Fatal(err)
	}
	dbs, err := LoadSetup([]byte(fmt.Sprintf(`
database:
    name: Local RocksDB
    type: rocksdb
    membudget: 1000000
    db_path: %s
`, dbdir)))
	if err != nil {
		t.Fatal("does not accept regular config: ", err)
	}

	_, err = dbs.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dbdir)

	stopChan := make(chan bool)
	dbs.Stop(stopChan)
	<-stopChan

}
