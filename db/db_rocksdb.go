// balboa
// Copyright (c) 2018, DCSO GmbH

package db

// #cgo LDFLAGS: -lrocksdb -ltpl
// #cgo CFLAGS: -O3 -Wno-implicit-function-declaration -Wall -Wextra -Wno-unused-parameter
// #include "obs_rocksdb.h"
// #include <stdlib.h>
import "C"

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
)

const (
	rocksTxSize = 50
	keySepChar  = "\x1f" // we use ASCII Unit separators
)

// RocksDB is a DB implementation based on Facebook's RocksDB engine.
type RocksDB struct {
	db       *C.ObsDB
	stopChan chan bool
}

func rocksMakeKey(sensor string, rrname string, rrtype string, rdata string) string {
	k := fmt.Sprintf("o%s%s%s%s%s%s%s%s", keySepChar, rrname, keySepChar, sensor, keySepChar, rrtype, keySepChar, rdata)
	return k
}

func rocksMakeInvKey(sensor string, rrname string, rrtype string, rdata string) string {
	k := fmt.Sprintf("i%s%s%s%s%s%s%s%s", keySepChar, rdata, keySepChar, sensor, keySepChar, rrname, keySepChar, rrtype)
	return k
}

// MakeRocksDB returns a new RocksDB instance, storing data at the given dbPath.
// The second parameter specifies a memory budget which determines the size of
// memtables and caches.
func MakeRocksDB(dbPath string, membudget uint64) (*RocksDB, error) {
	log.Info("opening database...")

	e := C.error_new()
	defer C.error_delete(e)
	cdbPath := C.CString(dbPath)
	cdb := C.obsdb_open(cdbPath, C.size_t(membudget), e)
	defer C.free(unsafe.Pointer(cdbPath))

	if C.error_is_set(e) {
		return nil, fmt.Errorf("%s", C.GoString(C.error_get(e)))
	}

	db := &RocksDB{
		db:       cdb,
		stopChan: make(chan bool),
	}

	log.Info("database ready")
	return db, nil
}

// MakeRocksDBReadonly returns a new read-only RocksDB instance.
func MakeRocksDBReadonly(dbPath string) (*RocksDB, error) {
	log.Info("opening database...")

	e := C.error_new()
	defer C.error_delete(e)
	cdbPath := C.CString(dbPath)
	cdb := C.obsdb_open_readonly(cdbPath, e)
	defer C.free(unsafe.Pointer(cdbPath))

	if C.error_is_set(e) {
		return nil, fmt.Errorf("%s", C.GoString(C.error_get(e)))
	}

	db := &RocksDB{
		db:       cdb,
		stopChan: make(chan bool),
	}

	log.Info("database ready")
	return db, nil
}

func rocksTxDedup(in []observation.InputObservation) []observation.InputObservation {
	cache := make(map[string]*observation.InputObservation)
	for i, inObs := range in {
		key := rocksMakeKey(inObs.SensorID, inObs.Rrname, inObs.Rrtype, inObs.Rdata)
		_, ok := cache[key]
		if ok {
			cache[key].Count += inObs.Count
			cache[key].TimestampEnd = inObs.TimestampEnd
		} else {
			cache[key] = &in[i]
		}
	}
	out := make([]observation.InputObservation, 0)
	for _, v := range cache {
		out = append(out, *v)
	}
	if len(in) != len(out) {
		log.Debugf("TX dedup: %d -> %d", len(in), len(out))
	}
	return out
}

// AddObservation adds a single observation synchronously to the database.
func (db *RocksDB) AddObservation(obs observation.InputObservation) observation.Observation {
	var cobs C.Observation
	e := C.error_new()
	defer C.error_delete(e)

	key := rocksMakeKey(obs.SensorID, obs.Rrname, obs.Rrtype,
		obs.Rdata)
	invKey := rocksMakeInvKey(obs.SensorID, obs.Rrname,
		obs.Rrtype, obs.Rdata)

	cobs.key = C.CString(key)
	cobs.inv_key = C.CString(invKey)
	cobs.count = C.uint(obs.Count)
	cobs.last_seen = C.uint(obs.TimestampEnd.Unix())
	cobs.first_seen = C.uint(obs.TimestampStart.Unix())

	C.obsdb_put(db.db, &cobs, e)
	if C.error_is_set(e) {
		log.Errorf("%s", C.GoString(C.error_get(e)))
	}

	C.free(unsafe.Pointer(cobs.key))
	C.free(unsafe.Pointer(cobs.inv_key))

	r, err := db.Search(&obs.Rdata, &obs.Rrname, &obs.Rrtype, &obs.SensorID)
	if err != nil {
		log.Error(err)
	}
	return r[0]
}

type rocksObservation struct {
	Count     int
	LastSeen  int64
	FirstSeen int64
}

// ConsumeFeed accepts observations from a channel and queues them for
// database insertion.
func (db *RocksDB) ConsumeFeed(inChan chan observation.InputObservation) {
	var i = 0
	buf := make([]observation.InputObservation, rocksTxSize)
	var cobs C.Observation
	e := C.error_new()
	defer C.error_delete(e)

	for {
		select {
		case <-db.stopChan:
			log.Info("database ingest terminated")
			return
		default:
			obs := <-inChan
			if i < rocksTxSize {
				buf[i] = obs
				i++
			} else {
				i = 0
				startTime := time.Now()
				for _, obs := range rocksTxDedup(buf) {
					if obs.Rrtype == "" {
						continue
					}

					key := rocksMakeKey(obs.SensorID, obs.Rrname, obs.Rrtype,
						obs.Rdata)
					invKey := rocksMakeInvKey(obs.SensorID, obs.Rrname,
						obs.Rrtype, obs.Rdata)

					cobs.key = C.CString(key)
					cobs.inv_key = C.CString(invKey)
					cobs.count = C.uint(obs.Count)
					cobs.last_seen = C.uint(obs.TimestampEnd.Unix())
					cobs.first_seen = C.uint(obs.TimestampStart.Unix())

					C.obsdb_put(db.db, &cobs, e)
					if C.error_is_set(e) {
						log.Errorf("%s", C.GoString(C.error_get(e)))
					}

					C.free(unsafe.Pointer(cobs.key))
					C.free(unsafe.Pointer(cobs.inv_key))
				}
				log.Debugf("insert Tx took %v", time.Since(startTime))
			}
		}
	}
}

//export cgoLogInfo
func cgoLogInfo(str *C.char) {
	log.Info(C.GoString(str))
}

//export cgoLogDebug
func cgoLogDebug(str *C.char) {
	log.Debug(C.GoString(str))
}

// Search returns a slice of observations matching one or more criteria such
// as rdata, rrname, rrtype or sensor ID.
func (db *RocksDB) Search(qrdata, qrrname, qrrtype, qsensorID *string) ([]observation.Observation, error) {
	outs := make([]observation.Observation, 0)
	var cqrdata, cqrrname, cqrrtype, cqsensorID *C.char
	var i uint

	if qrdata == nil {
		cqrdata = nil
	} else {
		cqrdata = C.CString(*qrdata)
		defer C.free(unsafe.Pointer(cqrdata))
	}
	if qrrname == nil {
		cqrrname = nil
	} else {
		cqrrname = C.CString(*qrrname)
		defer C.free(unsafe.Pointer(cqrrname))
	}
	if qrrtype == nil {
		cqrrtype = nil
	} else {
		cqrrtype = C.CString(*qrrtype)
		defer C.free(unsafe.Pointer(cqrrtype))
	}
	if qsensorID == nil {
		cqsensorID = nil
	} else {
		cqsensorID = C.CString(*qsensorID)
		defer C.free(unsafe.Pointer(cqsensorID))
	}

	r := C.obsdb_search(db.db, cqrdata, cqrrname, cqrrtype, cqsensorID)
	defer C.obs_set_delete(r)

	for i = 0; i < uint(C.obs_set_size(r)); i++ {
		o := C.obs_set_get(r, C.ulong(i))
		valArr := strings.Split(C.GoString(o.key), keySepChar)
		outs = append(outs, observation.Observation{
			SensorID:  valArr[2],
			Count:     int(o.count),
			RData:     valArr[4],
			RRName:    valArr[1],
			RRType:    valArr[3],
			FirstSeen: int(o.first_seen),
			LastSeen:  int(o.last_seen),
		})
	}

	return outs, nil
}

//export cgoObsDump
func cgoObsDump(o *C.Observation) {
	valArr := strings.Split(C.GoString(o.key), keySepChar)
	myObs := observation.Observation{
		SensorID:  valArr[2],
		Count:     int(o.count),
		RData:     valArr[4],
		RRName:    valArr[1],
		RRType:    valArr[3],
		FirstSeen: int(o.first_seen),
		LastSeen:  int(o.last_seen),
	}
	js, err := json.Marshal(myObs)
	if err == nil {
		fmt.Println(string(js))
	}
}

// Dump prints all aggregated observations in the database to stdout, in JSON format.
func (db *RocksDB) Dump() error {
	e := C.error_new()
	defer C.error_delete(e)

	C.obsdb_dump(db.db, e)

	if C.error_is_set(e) {
		return fmt.Errorf("%s", C.GoString(C.error_get(e)))
	}

	return nil
}

// TotalCount returns the overall number of observations across all sensors.
func (db *RocksDB) TotalCount() (int, error) {
	var val int
	val = int(C.obsdb_num_keys(db.db))
	return val, nil
}

// Shutdown closes the database connection, leaving the database unable to
// process both reads and writes.
func (db *RocksDB) Shutdown() {
	close(db.stopChan)
	C.obsdb_close(db.db)
	log.Info("database closed")
}
