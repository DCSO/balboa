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
	UseRefillInsteadOfPutBack bool
	host string
	obsConn net.Conn
	qryPool *Pool
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

const (
	qryPoolInitialCapacity=23
	qryPoolMaxCapacity=42
)

var SleepTimeForReconnect=10*time.Second

func makeObservationMessage( obs observation.InputObservation ) *Message {
	return &Message{Observations:[]observation.InputObservation{obs},Queries:[]Query{}}
}

func makeQueryMessage( qry Query ) *Message {
	return &Message{Observations:[]observation.InputObservation{},Queries:[]Query{qry}}
}

func MakeRemoteBackend( host string,refill bool ) (*RemoteBackend,error) {
	obsConn,obsConnErr:=net.Dial("tcp",host)
	if obsConnErr!=nil {
		return nil,obsConnErr
	}
	connectFn:=func() (net.Conn, error) {
		log.Debugf("pool: connecting to %s",host)
		c,err:=net.Dial("tcp",host)
		return c,err
	}
	pool,err:=MakePool(qryPoolInitialCapacity,qryPoolMaxCapacity,connectFn)
	if err != nil {
		return nil,err
	}
	return &RemoteBackend{UseRefillInsteadOfPutBack:refill,obsConn:obsConn,qryPool:pool,StopChan:make(chan bool),host:host},nil
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
	enc_err:=enc.Encode(makeQueryMessage(qry))
	if enc_err != nil {
		log.Warnf("unable to encode query")
		return []observation.Observation{},errors.New("query encode error")
	}

	conn,pool_err:=db.qryPool.Get()
	if pool_err!=nil {
		log.Warnf("unable to get connection from pool")
		return []observation.Observation{},pool_err
	}

	wanted:=w.Len()

	n,err_enc:=w.WriteTo(conn)
	if err_enc!=nil {
		log.Infof("sending query failed; closing connection")
		conn.Close()
		return []observation.Observation{},err_enc
	}

	if n!=int64(wanted) {
		log.Infof("sending query failed; short write; closing connection")
		conn.Close()
		return []observation.Observation{},errors.New("sending query failed")
	}

	log.Debugf("sent query (%d bytes)",n)

	dec:=codec.NewDecoder(conn,h)
	var result Result
	err_dec:=dec.Decode(&result)

	log.Debugf("received answer")

	if err_dec!=nil {
		conn.Close()
		return []observation.Observation{},err_dec
	}

	// put back connection
	if db.UseRefillInsteadOfPutBack {
		conn.Close()
		db.qryPool.Refill()
	} else {
		db.qryPool.Put(conn)
	}

	// check for a remote error (non connection related)
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
	db.qryPool.Teardown()
	db.obsConn.Close()
}
