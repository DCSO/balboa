// balboa
// Copyright (c) 2019, DCSO GmbH

package db

import (
	"net"
	"bytes"
	"errors"
	"time"

	"github.com/DCSO/balboa/observation"

	"github.com/ugorji/go/codec"
	log "github.com/sirupsen/logrus"
)

type RemoteBackend struct {
	host string
	obsConn net.Conn
	qryConn net.Conn
	StopChan chan bool
}

type Message struct {
	Observations []observation.InputObservation `codec:"O"`
	Queries []Query `codec:"Q"`
}

type Query struct {
	Qrdata,Qrrname,Qrrtype,QsensorID string
	Hrdata,Hrrname,Hrrtype,HsensorID bool
}

type Result struct {
	Observations []observation.Observation `codec:"O"`
	Error string `codec:"E"`
}

var SleepTimeForReconnect=10*time.Second

func makeObservationMessage( obs observation.InputObservation ) *Message {
	return &Message{Observations:[]observation.InputObservation{obs},Queries:[]Query{}}
}

func makeQueryMessage( qry Query ) *Message {
	return &Message{Observations:[]observation.InputObservation{},Queries:[]Query{qry}}
}

func MakeRemoteBackend( host string ) (*RemoteBackend,error) {
	obsConn,obsConnErr:=net.Dial("tcp",host)
	if obsConnErr!=nil {
		return nil,obsConnErr
	}
	qryConn,qryConnErr:=net.Dial("tcp",host)
	if qryConnErr!=nil {
		return nil,qryConnErr
	}
	return &RemoteBackend{obsConn:obsConn,qryConn:qryConn,StopChan:make(chan bool),host:host},nil
}

func (db *RemoteBackend) AddObservation( obs observation.InputObservation ) observation.Observation {
	log.Warn("AddObservation() not implemented")
	return observation.Observation{}
}

func (db *RemoteBackend) obsReconnect( ack chan bool ){
	for {
		log.Warnf("reconnecting to host=`%s`",db.host)
		conn,err:=net.Dial("tcp",db.host)
		if err==nil {
			log.Warnf("obsReconnect successfull");
			db.obsConn=conn
			ack<-true
			return
		}
		log.Warnf("obsReconnect failed: %s",err)
		time.Sleep(SleepTimeForReconnect)
	}
}

func (db *RemoteBackend) waitForObsReconnect( ) bool {
	ack:=make(chan bool)
	go db.obsReconnect(ack)
	ok:=<-ack
	return ok
}

func (db *RemoteBackend) ConsumeFeed( inChan chan observation.InputObservation ) {
	w:=new(bytes.Buffer)
	h:=new(codec.MsgpackHandle)
	h.ExplicitRelease=true
	h.WriteExt=true
	enc:=codec.NewEncoder(w,h)
	defer enc.Release()
	for {
		select {
			case <-db.StopChan:
				log.Info("stop request received")
				return
			case obs:=<-inChan:
				log.Debug("received observation")
				enc.Reset(w)
				err:=enc.Encode(makeObservationMessage(obs))
				if err!=nil {
					log.Warnf("encoding observation failed: %s",err)
					continue
				}
				len,err:=w.WriteTo(db.obsConn)
				if err!=nil {
					db.obsConn.Close()
					log.Warnf("sending observation failed: %s",err)
					reconnect_ok:=db.waitForObsReconnect()
					if( !reconnect_ok ) { return }
				}
				log.Debugf("sent %d bytes",len)
		}
	}
}

func (db *RemoteBackend) qryReconnect( ack chan bool ){
	for {
		log.Warnf("reconnecting to host=`%s`",db.host)
		conn,err:=net.Dial("tcp",db.host)
		if err==nil {
			log.Warnf("qryReconnect successfull");
			db.qryConn=conn
			ack<-true
			return
		}
		log.Warnf("qryReconnect failed: %s",err)
		time.Sleep(SleepTimeForReconnect)
	}
}

func (db *RemoteBackend) waitForQryReconnect( ) bool {
	ack:=make(chan bool)
	go db.qryReconnect(ack)
	ok:=<-ack
	return ok
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
	n,err_enc:=w.WriteTo(db.qryConn)
	if err_enc!=nil {
		db.qryConn.Close()
		reconnect_ok:=db.waitForQryReconnect()
		if( !reconnect_ok ){
			log.Warnf("query reconnect failed")
		}
		return []observation.Observation{},err_enc
	}
	log.Debugf("sent %d bytes",n)
	dec:=codec.NewDecoder(db.qryConn,h)
	var result Result
	err_dec:=dec.Decode(&result)
	// local decode failed
	if err_dec!=nil {
		db.qryConn.Close()
		reconnect_ok:=db.waitForQryReconnect()
		if( !reconnect_ok ){
			log.Warnf("query reconnect failed")
		}
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
	db.qryConn.Close()
	db.obsConn.Close()
}
