// balboa
// Copyright (c) 2019, DCSO GmbH

package db

import (
	"gopkg.in/yaml.v2"
	"net"

	obs "github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
)

type Backend struct {
	Name string   `yaml:"name"`
	Host string   `yaml:"host"`
	Tags []string `yaml:"tags"`
}

type RemoteBackend struct {
	stopChan chan bool
	backends []*Backend
}

func MakeRemoteBackend(config []byte, refill bool) (*RemoteBackend, error) {
	var backends []*Backend
	err := yaml.Unmarshal(config, &backends)
	if err != nil {
		log.Fatalf("could not read backend configuration due to %v", err)
	}
	if len(backends) < 1 {
		log.Fatalf("no or malformed backend configuration provided")
	}
	db := &RemoteBackend{stopChan: make(chan bool), backends: backends}

	return db, nil
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
	// hosts maps tags to connections
	hosts := make(map[string][]*net.Conn)
	for _, backend := range db.backends {
		conn, err := net.Dial("tcp", backend.Host)
		if err != nil {
			log.Fatalf("could not connect to backend %v due to %v", backend.Name, err)
		}
		if len(backend.Tags) != 0 {
			for _, tag := range backend.Tags {
				if _, ok := hosts[tag]; ok {
					hosts[tag] = append(hosts[tag], &conn)
				} else {
					hosts[tag] = make([]*net.Conn, 1)
					hosts[tag][0] = &conn
				}
			}
		} else {
			// the backend has no tags specified, consume everything
			tag := ""
			if _, ok := hosts[tag]; ok {
				hosts[tag] = append(hosts[tag], &conn)
			} else {
				hosts[tag] = make([]*net.Conn, 1)
				hosts[tag][0] = &conn
			}
		}
	}
	for {
		select {
		case <-db.stopChan:
			log.Info("stop request received")
			return
		case obs := <-inChan:
			for tag, connections := range hosts {
				match := false
				for obTag := range obs.Tags {
					if obTag == tag {
						match = true
						break
					}
				}
				if tag != "" && !match {
					continue
				}

				for _, conn := range connections {
					// since the backend does not support storage of tags yet remove tags from the observation
					obs.Tags = nil
					w, err := enc.EncodeInputRequest(obs)
					if err != nil {
						log.Warnf("encoding observation failed: %s", err)
						continue
					}
					wanted := w.Len()
					n, err := w.WriteTo(*conn)
					if err != nil {
						log.Warnf("sending observation failed: %s", err)
						continue
					}
					if n != int64(wanted) {
						log.Warnf("short write")
						continue
					}
				}
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

	var result []obs.Observation

	for _, backend := range db.backends {
		conn, connErr := net.Dial("tcp", backend.Host)
		if connErr != nil {
			log.Warnf("connecting to backend failed %v", connErr)
			conn.Close()
			continue
		}

		enc := MakeEncoder()
		w, encErr := enc.EncodeQueryRequest(qry)
		if encErr != nil {
			log.Warnf("unable to encode query %v", encErr)
			conn.Close()
			enc.Release()
			continue
		}

		wanted := w.Len()
		n, errWr := w.WriteTo(conn)
		if errWr != nil {
			log.Infof("sending query failed; closing connection %v", errWr)
			conn.Close()
			enc.Release()
			continue
		}

		if n != int64(wanted) {
			log.Infof("sending query failed; short write; closing connection")
			conn.Close()
			enc.Release()
			continue
		}

		log.Debugf("sent query (%d bytes)", n)

		dec := MakeDecoder(conn)
		qryResult, err := dec.ExpectQueryResponse()

		log.Debugf("received answer")

		if err != nil {
			log.Warnf("ExpectQueryResponse() failed with `%v`", err)
			conn.Close()
			dec.Release()
			enc.Release()
			continue
		}
		conn.Close()
		dec.Release()
		enc.Release()
		result = append(result, qryResult.Obs[:]...)
	}

	return result, nil
}

func (db *RemoteBackend) TotalCount() (int, error) {
	return -1, nil
}

func (db *RemoteBackend) Shutdown() {
	close(db.stopChan)
}
