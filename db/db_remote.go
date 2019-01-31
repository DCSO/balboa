// balboa
// Copyright (c) 2019, DCSO GmbH

package db

import (
	"net"
	"bytes"
	"errors"
	"github.com/DCSO/balboa/observation"

	"github.com/ugorji/go/codec"
	log "github.com/sirupsen/logrus"
)

type RemoteBackend struct {
	conn net.Conn
	StopChan chan bool
}

type Message struct {
	Observations []observation.InputObservation
	Queries []Query
}

type Query struct {
	Qrdata,Qrrname,Qrrtype,QsensorID string
	Hrdata,Hrrname,Hrrtype,HsensorID bool
}

type Result struct {
	Observations []observation.Observation
	Error string
}

func makeObservationMessage( obs observation.InputObservation ) *Message {
	return &Message{Observations:[]observation.InputObservation{obs},Queries:[]Query{}}
}

func makeQueryMessage( qry Query ) *Message {
	return &Message{Observations:[]observation.InputObservation{},Queries:[]Query{qry}}
}

func MakeRemoteBackend( host string ) (*RemoteBackend,error) {
	conn,err:=net.Dial("tcp",host)
	if err!=nil {
		return nil,err
	}
	return &RemoteBackend{conn:conn,StopChan:make(chan bool)},nil
}

func (db *RemoteBackend) AddObservation( obs observation.InputObservation ) observation.Observation {
	log.Warn("AddObservation() not implemented")
	return observation.Observation{}
}

func (db *RemoteBackend) ConsumeFeed( inChan chan observation.InputObservation ) {
	w:=new(bytes.Buffer)
	h:=new(codec.MsgpackHandle)
	enc:=codec.NewEncoder(w,h)
	for {
		select {
			case <-db.StopChan:
				log.Info("stop request received")
				return
			case obs:=<-inChan:
				log.Debug("received observation")
				err:=enc.Encode(makeObservationMessage(obs))
				if err!=nil {
					log.Warnf("encoding observation failed: %s",err)
					enc.Reset(w)
					continue
				}
				len,err:=w.WriteTo(db.conn)
				if err!=nil {
					log.Warnf("sending observation failed: %s",err)
					enc.Reset(w)
					return
				}
				enc.Reset(w)
				log.Debugf("sent %d bytes",len)
		}
	}
}

func sanitize( s *string ) string {
	if s==nil {
		return ""
	}else{
		return *s
	}
}

func (db *RemoteBackend) Search(qrdata,qrrname,qrrtype,qsensorID *string) ([]observation.Observation,error) {
	qry:=Query{
		Qrdata:sanitize(qrdata),
		Hrdata:qrdata!=nil,
		Qrrname:sanitize(qrrname),
		Hrrname:qrrname!=nil,
		Qrrtype:sanitize(qrrtype),
		Hrrtype:qrrtype!=nil,
		QsensorID:sanitize(qsensorID),
		HsensorID:qsensorID!=nil,
	}
	w:=new(bytes.Buffer)
	h:=new(codec.MsgpackHandle)
	enc:=codec.NewEncoder(w,h)
	enc.Encode(makeQueryMessage(qry))
	n,err_enc:=w.WriteTo(db.conn)
	if err_enc!=nil {
		enc.Reset(w)
		return []observation.Observation{},err_enc
	}
	log.Debugf("sent %d bytes",n)
	dec:=codec.NewDecoder(db.conn,h)
	var result Result
	err_dec:=dec.Decode(&result)
	// local decode failed
	if err_dec!=nil {
		return []observation.Observation{},err_dec
	}
	// seems like a remote error occured
	if result.Error!="" {
		if len(result.Observations)>0 {
			log.Warnf("discarding %v query results due to non-empty error message",len(result.Observations))
		}
		return []observation.Observation{},errors.New(result.Error)
	}
	return result.Observations,nil
}

func (db *RemoteBackend) TotalCount() (int,error) {
	return -1,nil
}

func (db *RemoteBackend) Shutdown() {
	db.conn.Close()
}
