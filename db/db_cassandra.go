// balboa
// Copyright (c) 2018, DCSO GmbH

package db

import (
	"time"

	"github.com/DCSO/balboa/observation"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// CassandraDB is a DB implementation based on Apache Cassandra.
type CassandraDB struct {
	Cluster  *gocql.ClusterConfig
	Session  *gocql.Session
	StopChan chan bool
	Nworkers uint
}

// MakeCassandraDB returns a new CassandraDB instance connecting to the
// provided hosts.
func MakeCassandraDB(hosts []string, username, password string, nofWorkers uint) (*CassandraDB, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "balboa"
	cluster.ProtoVersion = 4
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 5}
	cluster.Consistency = gocql.Two
	if len(username) > 0 && len(password) > 0 {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}
	gsession, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	//session := gockle.NewSession(gsession)
	db := &CassandraDB{
		Cluster:  cluster,
		Session:  gsession,
		StopChan: make(chan bool),
		Nworkers: nofWorkers,
	}
	return db, nil
}

//func makeCassandraDBMock() (*CassandraDB, *gockle.SessionMock) {
//	mock := &gockle.SessionMock{}
//	db := &CassandraDB{
//		//Session:  mock,
//		StopChan: make(chan bool),
//	}
//	return db, mock
//}

// AddObservation adds a single observation synchronously to the database.
func (db *CassandraDB) AddObservation(obs observation.InputObservation) observation.Observation {
	//Not implemented
	log.Warn("AddObservation() not yet implemented on Cassandra backend")
	return observation.Observation{}
}

func (db *CassandraDB) runChunkWorker(inChan chan observation.InputObservation) {
	rdataUpd := db.Session.Query(`UPDATE observations_by_rdata SET last_seen = ? where rdata = ? and rrname = ?  and rrtype = ? and sensor_id = ?;`)
	rrnameUpd := db.Session.Query(`UPDATE observations_by_rrname SET last_seen = ? where rrname = ? and rdata = ? and rrtype = ? and sensor_id = ?;`)
	firstseenUpd := db.Session.Query(`INSERT INTO observations_firstseen (first_seen, rrname, rdata, rrtype, sensor_id) values (?, ?, ?, ?, ?) IF NOT EXISTS;`)
	countsUpd := db.Session.Query(`UPDATE observations_counts SET count = count + ? where rdata = ? and rrname = ? and rrtype = ? and sensor_id = ?;`)
	for obs := range inChan {
		select {
		case <-db.StopChan:
			log.Info("database ingest terminated")
			return
		default:
			if obs.Rdata == "" {
				obs.Rdata = "-"
			}
			if err := rdataUpd.Bind(obs.TimestampEnd, obs.Rdata, obs.Rrname, obs.Rrtype, obs.SensorID).Exec(); err != nil {
				log.Error(err)
				continue
			}
			if err := rrnameUpd.Bind(obs.TimestampEnd, obs.Rrname, obs.Rdata, obs.Rrtype, obs.SensorID).Exec(); err != nil {
				log.Error(err)
				continue
			}
			if err := firstseenUpd.Bind(obs.TimestampStart, obs.Rrname, obs.Rdata, obs.Rrtype, obs.SensorID).Exec(); err != nil {
				log.Error(err)
				continue
			}
			if err := countsUpd.Bind(obs.Count, obs.Rdata, obs.Rrname, obs.Rrtype, obs.SensorID).Exec(); err != nil {
				log.Error(err)
				continue
			}
		}
	}
}

// ConsumeFeed accepts observations from a channel and queues them for
// database insertion.
func (db *CassandraDB) ConsumeFeed(inChan chan observation.InputObservation) {
	var w uint
	sendChan := make(chan observation.InputObservation, 500)
	log.Debugf("Firing up %d workers", db.Nworkers)
	for w = 1; w <= db.Nworkers; w++ {
		go db.runChunkWorker(sendChan)
	}
	for {
		select {
		case <-db.StopChan:
			log.Info("database ingest terminated")
			return
		case obs := <-inChan:
			sendChan <- obs
		}
	}
}

// Search returns a slice of observations matching one or more criteria such
// as rdata, rrname, rrtype or sensor ID.
func (db *CassandraDB) Search(qrdata, qrrname, qrrtype, qsensorID *string) ([]observation.Observation, error) {
	outs := make([]observation.Observation, 0)
	var getQueryString string
	var rdataFirst, hasSecond bool
	var getQuery *gocql.Query

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
	log.Debug(getQueryString)
	getQuery = db.Session.Query(getQueryString)
	getQuery.Consistency(gocql.One)

	// do parameterised search
	if rdataFirst {
		if hasSecond {
			getQuery.Bind(*qrdata, *qrrname)
		} else {
			getQuery.Bind(*qrdata)
		}
	} else {
		if hasSecond {
			getQuery.Bind(*qrrname, *qrdata)
		} else {
			getQuery.Bind(*qrrname)
		}
	}

	// retrieve hits for initial queries
	iter := getQuery.Iter()
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}

		if rrnameV, ok := row["rrname"]; ok {
			var rdata, rrname, rrtype, sensorID string
			var lastSeen int
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

				// we now have a result item
				out := observation.Observation{
					RRName:   rrname,
					RData:    rdata,
					RRType:   rrtype,
					SensorID: sensorID,
					LastSeen: lastSeen,
				}

				// manual joins -> get additional data from counts table
				tmpMap := make(map[string]interface{})
				getCounters := db.Session.Query(`SELECT count FROM observations_counts WHERE rrname = ? AND rdata = ? AND rrtype = ? AND sensor_id = ?`).Bind(rrname, rdata, rrtype, sensorID)
				err := getCounters.MapScan(tmpMap)
				if err != nil {
					log.Errorf("getCount: %s", err.Error())
					continue
				}
				out.Count = int(tmpMap["count"].(int64))

				tmpMap = make(map[string]interface{})
				getFirstSeen := db.Session.Query(`SELECT first_seen FROM observations_firstseen WHERE rrname = ? AND rdata = ? AND rrtype = ? AND sensor_id = ?`).Bind(rrname, rdata, rrtype, sensorID)
				err = getFirstSeen.MapScan(tmpMap)
				if err != nil {
					log.Errorf("getFirstSeen: %s", err.Error())
					continue
				}
				out.FirstSeen = int(tmpMap["first_seen"].(int64))

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
