// balboa
// Copyright (c) 2018, DCSO GmbH

package db

import (
	"database/sql"
	"time"

	// imported for side effects
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/DCSO/balboa/observation"
)

// create table observations (
// 	id bigint AUTO_INCREMENT,
// 	sensorID varchar(255) NOT NULL,
// 	rrname varchar(255) NOT NULL,
// 	rdata varchar(255) NOT NULL,
// 	rrcode varchar(64) NULL,
// 	rrtype varchar(64) NULL,
// 	count int NOT NULL,
// 	time_first timestamp NOT NULL,
// 	time_last timestamp NOT NULL,
// 	PRIMARY KEY (id),
// 	UNIQUE KEY (rdata, rrname, sensorID),
// 	INDEX rrname (rrname)
// );

// MySQLDB is a DB implementation based on MySQL.
type MySQLDB struct {
	DB *sql.DB
}

const (
	mysqlbufsize = 50
	upsertSQL    = `
	INSERT INTO observations
		(sensorID, rrname, rdata, rrcode, rrtype, count, time_first, time_last)
    VALUES 
	    (?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		count = count + VALUES(count),
		time_last = VALUES(time_last);`
	qrySQL = `
	SELECT 
		sensorID, rrname, rdata, rrtype, count, time_first, time_last
	FROM 
		observations 
	WHERE 
		rdata like ? AND rrname like ? AND rrtype like ? AND sensorID like ?;`
)

// MakeMySQLDB returns a new MySQLDB instance based on the given sql.DB
// pointer.
func MakeMySQLDB(pdb *sql.DB) *MySQLDB {
	db := &MySQLDB{
		DB: pdb,
	}
	return db
}

// AddObservation adds a single observation synchronously to the database.
func (db *MySQLDB) AddObservation(obs observation.InputObservation) observation.Observation {
	//Not implemented
	log.Warn("AddObservation() not implemented on MySQL DB backend")
	return observation.Observation{}
}

// ConsumeFeed accepts observations from a channel and queues them for
// database insertion.
func (db *MySQLDB) ConsumeFeed(inChan chan observation.InputObservation, stopChan chan bool) {
	buf := make([]observation.InputObservation, mysqlbufsize)
	i := 0
	for {
		select {
		case <-stopChan:
			log.Info("database ingest terminated")
			return
		default:
			if i < mysqlbufsize {
				log.Debug("buffering ", i)
				buf[i] = <-inChan
				i++
			} else {
				// flushing buffer in one transaction
				i = 0
				startTime := time.Now()
				tx, err := db.DB.Begin()
				if err != nil {
					log.Warn(err)
				}
				var upsert *sql.Stmt
				upsert, _ = tx.Prepare(upsertSQL)
				for _, obs := range buf {
					_, err := upsert.Exec(obs.SensorID, obs.Rrname,
						obs.Rdata, nil, obs.Rrtype, obs.Count,
						obs.TimestampStart, obs.TimestampEnd)
					if err != nil {
						log.Warn(err)
						tx.Rollback()
					}
				}
				tx.Commit()
				log.Infof("insert Tx took %v", time.Since(startTime))
			}
		}
	}
}

// Search returns a slice of observations matching one or more criteria such
// as rdata, rrname, rrtype or sensor ID.
func (db *MySQLDB) Search(qrdata, qrrname, qrrtype, qsensorID *string) ([]observation.Observation, error) {
	outs := make([]observation.Observation, 0)
	stmt, err := db.DB.Prepare(qrySQL)
	if err != nil {
		return nil, err
	}

	rdata := "%"
	if qrdata != nil {
		rdata = *qrdata
	}
	sensorID := "%"
	if qsensorID != nil {
		sensorID = *qsensorID
	}
	rrname := "%"
	if qrrname != nil {
		rrname = *qrrname
	}
	rrtype := "%"
	if qrrtype != nil {
		rrtype = *qrrtype
	}

	rows, err := stmt.Query(rdata, rrname, rrtype, sensorID)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		out := observation.Observation{}
		var lastseen, firstseen time.Time
		err := rows.Scan(&out.SensorID, &out.RRName, &out.RData, &out.RRType,
			&out.Count, &firstseen, &lastseen)
		if err != nil {
			return nil, err
		}
		out.FirstSeen = int(firstseen.Unix())
		out.LastSeen = int(lastseen.Unix())
		outs = append(outs, out)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return outs, nil
}

// TotalCount returns the overall number of observations across all sensors.
func (db *MySQLDB) TotalCount() (int, error) {
	var val int
	var err error
	rows, err := db.DB.Query("SELECT count(*) FROM observations")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&val)
		if err != nil {
			return 0, err
		}
	}
	err = rows.Err()
	if err != nil {
		return 0, err
	}
	return val, nil
}

// Shutdown closes the database connection, leaving the database unable to
// process both reads and writes.
func (db *MySQLDB) Shutdown() {
	db.DB.Close()
}
