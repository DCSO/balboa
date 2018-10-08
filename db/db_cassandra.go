// balboa
// Copyright (c) 2018, DCSO GmbH

package db

import (
	"fmt"
	"time"

	"github.com/DCSO/balboa/observation"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/willfaught/gockle"
)

// CassandraDB is a DB implementation based on Apache Cassandra.
type CassandraDB struct {
	Session  gockle.Session
	StopChan chan bool
}

const (
	cassbufsize = 500
)

// MakeCassandraDB returns a new CassandraDB instance connecting to the
// provided hosts.
func MakeCassandraDB(hosts []string) (*CassandraDB, error) {
	var err error
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "balboa"
	cluster.Consistency = gocql.One
	log.Infof("connecting to hosts %v", hosts)
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	db := &CassandraDB{
		Session:  gockle.NewSession(session),
		StopChan: make(chan bool),
	}
	return db, nil
}

func makeCassandraDBMock() (*CassandraDB, *gockle.SessionMock) {
	mock := &gockle.SessionMock{}
	db := &CassandraDB{
		Session:  mock,
		StopChan: make(chan bool),
	}
	return db, mock
}

// AddObservation adds a single observation synchronously to the database.
func (db *CassandraDB) AddObservation(obs observation.InputObservation) observation.Observation {
	//Not implemented
	log.Warn("AddObservation() not yet implemented on Cassandra backend")
	return observation.Observation{}
}

func cassMakeKey(sensor string, rrname string, rrtype string, rdata string) string {
	k := fmt.Sprintf("%s%s%s%s%s%s%s", rrname, keySepChar, sensor, keySepChar, rrtype, keySepChar, rdata)
	return k
}

func cassTxDedup(in []observation.InputObservation) []observation.InputObservation {
	cache := make(map[string]*observation.InputObservation)
	for i, inObs := range in {
		key := cassMakeKey(inObs.SensorID, inObs.Rrname, inObs.Rrtype, inObs.Rdata)
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

// ConsumeFeed accepts observations from a channel and queues them for
// database insertion.
func (db *CassandraDB) ConsumeFeed(inChan chan observation.InputObservation) {
	buf := make([]observation.InputObservation, cassbufsize)
	i := 0
	errMap := make(map[string]uint64)
	for {
		select {
		case <-db.StopChan:
			log.Info("database ingest terminated")
			return
		default:
			if i < cassbufsize {
				buf[i] = <-inChan
				i++
			} else {
				i = 0
				startTime := time.Now()
				for _, obs := range cassTxDedup(buf) {
					select {
					case <-db.StopChan:
						log.Info("database ingest terminated")
						return
					default:
						if obs.Rdata == "" {
							obs.Rdata = "-"
						}
						iter := db.Session.ScanIterator(`SELECT rrname FROM observations_by_rrname WHERE rrname = ? and rdata = ? and rrtype = ? and sensor_id = ?;`,
							obs.Rrname, obs.Rdata, obs.Rrtype, obs.SensorID)
						defer iter.Close()
						row := make(map[string]interface{})
						if !iter.ScanMap(row) {
							if err := db.Session.Exec(`UPDATE observations_by_rdata SET first_seen = ?, last_seen = ? where rdata = ? and rrname = ?  and rrtype = ? and sensor_id = ?;`,
								obs.TimestampStart, obs.TimestampEnd, obs.Rdata, obs.Rrname, obs.Rrtype, obs.SensorID); err != nil {
								errMap[err.Error()]++
								continue
							}
							if err := db.Session.Exec(`UPDATE observations_by_rrname SET first_seen = ?, last_seen = ? where rrname = ? and rdata = ? and rrtype = ? and sensor_id = ?;`,
								obs.TimestampStart, obs.TimestampEnd, obs.Rrname, obs.Rdata, obs.Rrtype, obs.SensorID); err != nil {
								errMap[err.Error()]++
								continue
							}
						} else {
							if err := db.Session.Exec(`UPDATE observations_by_rdata SET last_seen = ? where rdata = ? and rrname = ?  and rrtype = ? and sensor_id = ?;`,
								obs.TimestampEnd, obs.Rdata, obs.Rrname, obs.Rrtype, obs.SensorID); err != nil {
								errMap[err.Error()]++
								continue
							}
							if err := db.Session.Exec(`UPDATE observations_by_rrname SET last_seen = ? where rrname = ? and rdata = ? and rrtype = ? and sensor_id = ?;`,
								obs.TimestampEnd, obs.Rrname, obs.Rdata, obs.Rrtype, obs.SensorID); err != nil {
								errMap[err.Error()]++
								continue
							}
						}
						if err := db.Session.Exec(`UPDATE observations_counts SET count = count + ? where rdata = ? and rrname = ? and rrtype = ? and sensor_id = ?;`,
							obs.Count, obs.Rdata, obs.Rrname, obs.Rrtype, obs.SensorID); err != nil {
							errMap[err.Error()]++
							continue
						}
					}
				}
				log.Infof("insert Tx took %v", time.Since(startTime))
				if len(errMap) > 0 {
					errString := "errors: "
					for k, v := range errMap {
						errString += fmt.Sprintf("%s (%d instances) ", k, v)
					}
					log.Error(errString)
					errMap = make(map[string]uint64)
				}
			}
		}
	}
}

// Search returns a slice of observations matching one or more criteria such
// as rdata, rrname, rrtype or sensor ID.
func (db *CassandraDB) Search(qrdata, qrrname, qrrtype, qsensorID *string) ([]observation.Observation, error) {
	outs := make([]observation.Observation, 0)
	var getQueryString string
	var rdataFirst, hasSecond bool

	// determine appropriate table and parameterisation for query
	if qrdata != nil {
		rdataFirst = true
		if qrrname != nil {
			hasSecond = true
			getQueryString = "SELECT * FROM observations_by_rdata WHERE rdata = ? and rrtype = ?"
		} else {
			hasSecond = false
			getQueryString = "SELECT * FROM observations_by_rdata WHERE rdata = ?"
		}
	} else {
		rdataFirst = false
		if qrdata != nil {
			hasSecond = true
			getQueryString = "SELECT * FROM observations_by_rrname WHERE rrname = ? and rdata = ?"
		} else {
			hasSecond = false
			getQueryString = "SELECT * FROM observations_by_rrname WHERE rrname = ?"
		}
	}

	// do parameterised search
	var iter gockle.Iterator
	if rdataFirst {
		if hasSecond {
			iter = db.Session.ScanIterator(getQueryString, *qrdata, *qrrname)
		} else {
			iter = db.Session.ScanIterator(getQueryString, *qrdata)
		}
	} else {
		if hasSecond {
			iter = db.Session.ScanIterator(getQueryString, *qrrname, *qrdata)
		} else {
			iter = db.Session.ScanIterator(getQueryString, *qrrname)
		}
	}
	defer iter.Close()

	// retrieve hits for initial queries
	for {
		row := make(map[string]interface{})
		more := iter.ScanMap(row)
		if !more {
			break
		}

		if rrnameV, ok := row["rrname"]; ok {
			var rdata, rrname, rrtype, sensorID string
			var lastSeen, firstSeen int
			rrname = rrnameV.(string)
			if rdataV, ok := row["rdata"]; ok {
				rdata = rdataV.(string)

				// secondary filtering by sensor ID and RRType
				if sensorIDV, ok := row["sensor_id"]; ok {
					sensorID = sensorIDV.(string)
					if qsensorID != nil && *qsensorID != sensorID {
						continue
					}
				}
				if rrtypeV, ok := row["rrtype"]; ok {
					rrtype = rrtypeV.(string)
					if qrrtype != nil && *qrrtype != rrtype {
						continue
					}
				}
				if lastSeenV, ok := row["last_seen"]; ok {
					lastSeen = int(lastSeenV.(time.Time).Unix())
				}
				if firstSeenV, ok := row["first_seen"]; ok {
					firstSeen = int(firstSeenV.(time.Time).Unix())
				}

				// we now have a result item
				out := observation.Observation{
					RRName:    rrname,
					RData:     rdata,
					RRType:    rrtype,
					SensorID:  sensorID,
					LastSeen:  lastSeen,
					FirstSeen: firstSeen,
				}

				// manual joins -> get additional data from counts table
				tmpMap := make(map[string]interface{})
				cntIter := db.Session.ScanIterator(`SELECT count FROM observations_counts WHERE rrname = ? AND rdata = ? AND rrtype = ? AND sensor_id = ?`,
					rrname, rdata, rrtype, sensorID)
				cntIter.ScanMap(tmpMap)
				iter.Close()
				out.Count = int(tmpMap["count"].(int64))

				outs = append(outs, out)
			} else {
				log.Warn("result is missing rdata column, something is very wrong")
			}
		} else {
			log.Warn("result is missing rrname column, something is very wrong")
		}

	}

	return outs, nil
}

// TotalCount returns the overall number of observations across all sensors.
func (db *CassandraDB) TotalCount() (int, error) {
	// TODO
	return 0, nil
}

// Shutdown closes the database connection, leaving the database unable to
// process both reads and writes.
func (db *CassandraDB) Shutdown() {
	close(db.StopChan)
	if db.Session != nil {
		db.Session.Close()
	}
}
