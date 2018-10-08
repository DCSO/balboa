package db

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/DCSO/balboa/observation"
)

func createRocksTmpDB(t *testing.T) (*RocksDB, string) {
	dbdir, err := ioutil.TempDir("", "example")
	if err != nil {
		t.Fatal(err)
	}
	db, err := MakeRocksDB(dbdir, 8000000)
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Fatal("no db created")
	}
	return db, dbdir
}

func TestRocksCreate(t *testing.T) {
	db, dbdir := createRocksTmpDB(t)
	defer os.RemoveAll(dbdir)
	defer db.Shutdown()
}

func TestRocksCreateFail(t *testing.T) {
	// skip this test if run as root
	if syscall.Getuid() == 0 {
		t.Skip()
	}
	_, err := MakeRocksDB("/nonexistent", 8000000)
	if err == nil {
		t.Fatal(err)
	}
}

func TestRocksSimpleStore(t *testing.T) {
	db, dbdir := createRocksTmpDB(t)
	defer os.RemoveAll(dbdir)
	defer db.Shutdown()

	a := db.AddObservation(observation.InputObservation{
		Rrname:         "foo.bar",
		Rrtype:         "A",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.78",
		TimestampEnd:   time.Now(),
		TimestampStart: time.Now(),
		Count:          1,
	})
	if a.RData != "12.34.56.78" || a.RRName != "foo.bar" || a.RRType != "A" ||
		a.Count != 1 {
		t.Fatal("invalid return")
	}
	a = db.AddObservation(observation.InputObservation{
		Rrname:         "foo.bar",
		Rrtype:         "MX",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.77",
		TimestampEnd:   time.Now(),
		TimestampStart: time.Now(),
		Count:          1,
	})
	if a.RData != "12.34.56.77" || a.RRName != "foo.bar" || a.RRType != "MX" ||
		a.Count != 1 {
		t.Fatal("invalid return")
	}
	a = db.AddObservation(observation.InputObservation{
		Rrname:         "foo.bar",
		Rrtype:         "NS",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.79",
		TimestampEnd:   time.Now(),
		TimestampStart: time.Now(),
		Count:          1,
	})
	if a.RData != "12.34.56.79" || a.RRName != "foo.bar" || a.RRType != "NS" ||
		a.Count != 1 {
		t.Fatal("invalid return")
	}

	str := "foo.bar"
	obs, err := db.Search(nil, &str, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 3 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}

	a = db.AddObservation(observation.InputObservation{
		Rrname:         "foo.bar",
		Rrtype:         "NS",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.79",
		TimestampEnd:   time.Now().Add(77 * time.Second),
		TimestampStart: time.Now().Add(77 * time.Second),
		Count:          4,
	})
	if a.RData != "12.34.56.79" || a.RRName != "foo.bar" || a.RRType != "NS" ||
		a.Count != 5 {
		t.Fatalf("invalid return %v", a)
	}

	obs, err = db.Search(nil, &str, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 3 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}
}

func TestRocksSimpleConsume(t *testing.T) {
	db, dbdir := createRocksTmpDB(t)
	defer os.RemoveAll(dbdir)
	defer db.Shutdown()

	inChan := make(chan observation.InputObservation)
	stopChan := make(chan bool)

	go db.ConsumeFeed(inChan)

	inChan <- observation.InputObservation{
		Rrname:         "foo.bar",
		Rrtype:         "A",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.78",
		TimestampEnd:   time.Now(),
		TimestampStart: time.Now(),
		Count:          1,
	}
	inChan <- observation.InputObservation{
		Rrname:         "foo.bar",
		Rrtype:         "MX",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.77",
		TimestampEnd:   time.Now(),
		TimestampStart: time.Now(),
		Count:          1,
	}
	for i := 0; i < 10000; i++ {
		inChan <- observation.InputObservation{
			Rrname:         "foo.bar",
			Rrtype:         "NS",
			SensorID:       "deadcafe",
			Rdata:          "12.34.56.79",
			TimestampEnd:   time.Now(),
			TimestampStart: time.Now(),
			Count:          2,
		}
	}
	close(stopChan)

	str := "foo.bar"
	obs, err := db.Search(nil, &str, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 3 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}

	str = "12.34.56.79"
	obs, err = db.Search(&str, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}

	str = "12.34.56.79"
	obs, err = db.Search(&str, nil, nil, &str)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 0 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}

}
