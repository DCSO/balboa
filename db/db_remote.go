// balboa
// Copyright (c) 2019, DCSO GmbH

package db

import (
	"errors"
	"net"

	obs "github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
)

type RemoteBackend struct {
	host     string
	StopChan chan bool
}

func MakeRemoteBackend(host string, refill bool) (*RemoteBackend, error) {
	return &RemoteBackend{StopChan: make(chan bool), host: host}, nil
}

func (db *RemoteBackend) AddObservation(o obs.InputObservation) obs.Observation {
	log.Warn("AddObservation() not implemented")
	return obs.Observation{}
}

func (db *RemoteBackend) Backup(path string) {
	log.Warnf("backup request unimplemented")
}

func (db *RemoteBackend) Dump(path string) {
	log.Warnf("dump request unimplemented")
}

func (db *RemoteBackend) ConsumeFeed(inChan chan obs.InputObservation) {
	enc := MakeEncoder()
	defer enc.Release()
	conn, conn_err := net.Dial("tcp", db.host)
	if conn_err != nil {
		log.Warnf("connecting to backend failed: %v", conn_err)
		return
	}
	defer conn.Close()
	for {
		select {
		case <-db.StopChan:
			log.Info("stop request received")
			return
		case obs := <-inChan:
			log.Debug("received observation")
			w, err := enc.EncodeInputRequest(obs)
			if err != nil {
				log.Warnf("encoding observation failed: %s", err)
				continue
			}
			wanted := w.Len()
			n, err := w.WriteTo(conn)
			if err != nil {
				log.Warnf("sending observation failed: %s", err)
				return
			}
			if n != int64(wanted) {
				log.Warnf("short write")
				return
			}
		}
	}
}

func sanitize(s *string) string {
	if s == nil {
		return ""
	} else {
		return *s
	}
}

func (db *RemoteBackend) Search(qrdata, qrrname, qrrtype, qsensorID *string, limit int) ([]obs.Observation, error) {
	qry := QueryRequest{
		Qrdata:    sanitize(qrdata),
		Hrdata:    qrdata != nil,
		Qrrname:   sanitize(qrrname),
		Hrrname:   qrrname != nil,
		Qrrtype:   sanitize(qrrtype),
		Hrrtype:   qrrtype != nil,
		QsensorID: sanitize(qsensorID),
		HsensorID: qsensorID != nil,
		Limit:     limit,
	}

	conn, conn_err := net.Dial("tcp", db.host)
	if conn_err != nil {
		log.Warnf("connecting to backend failed %v", conn_err)
		return []obs.Observation{}, conn_err
	}
	defer conn.Close()

	enc := MakeEncoder()
	defer enc.Release()

	w, enc_err := enc.EncodeQueryRequest(qry)
	if enc_err != nil {
		log.Warnf("unable to encode query")
		return []obs.Observation{}, enc_err
	}

	wanted := w.Len()

	n, err_wr := w.WriteTo(conn)
	if err_wr != nil {
		log.Infof("sending query failed; closing connection")
		conn.Close()
		return []obs.Observation{}, err_wr
	}

	if n != int64(wanted) {
		log.Infof("sending query failed; short write; closing connection")
		conn.Close()
		return []obs.Observation{}, errors.New("sending query failed")
	}

	log.Debugf("sent query (%d bytes)", n)

	dec := MakeDecoder(conn)
	defer dec.Release()

	result, err := dec.ExpectQueryResponse()

	log.Debugf("received answer")

	if err != nil {
		log.Warnf("ExpectQueryResponse() failed with `%v`", err)
		conn.Close()
		return []obs.Observation{}, err
	}

	return result.Obs, nil
}

func (db *RemoteBackend) TotalCount() (int, error) {
	return -1, nil
}

func (db *RemoteBackend) Shutdown() {
	close(db.StopChan)
}
