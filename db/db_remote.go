// balboa
// Copyright (c) 2019, DCSO GmbH

package db

import (
	"bytes"
	"errors"
	"net"
	"time"

	obs "github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
	"github.com/ugorji/go/codec"
)

type RemoteBackend struct {
	host     string
	obsConn  net.Conn
	StopChan chan bool
}

type TypedMessage struct {
	Type           uint8  `codec:"T"`
	EncodedMessage []byte `codec:"M"`
}

type QueryMessage struct {
	Qrdata, Qrrname, Qrrtype, QsensorID string
	Hrdata, Hrrname, Hrrtype, HsensorID bool
	Limit                               int
}

type InputMessage struct {
	Obs []obs.InputObservation `codec:"O"`
}

type QueryResponse struct {
	Obs []obs.Observation `codec:"O"`
}

type ErrorResponse struct {
	Message string
}

const (
	TypeInputMessage  = 1
	TypeQueryMessage  = 2
	TypeErrorResponse = 128
	TypeQueryResponse = 129
)

var SleepTimeForReconnect = 10 * time.Second

func (enc *Encoder) encode_observation_message(o obs.InputObservation) (*bytes.Buffer, error) {
	enc.enc.Reset(enc.inner)
	err_inner := enc.enc.Encode(InputMessage{Obs: []obs.InputObservation{o}})
	if err_inner != nil {
		return nil, err_inner
	}
	enc.enc.Reset(enc.outer)
	err_outer := enc.enc.Encode(&TypedMessage{Type: TypeInputMessage, EncodedMessage: enc.inner.Bytes()})
	if err_inner != nil {
		return nil, err_outer
	}
	return enc.outer, nil
}

func (enc *Encoder) encode_query_message(qry QueryMessage) (*bytes.Buffer, error) {
	enc.enc.Reset(enc.inner)
	err_inner := enc.enc.Encode(qry)
	if err_inner != nil {
		return nil, err_inner
	}
	enc.enc.Reset(enc.outer)
	err_outer := enc.enc.Encode(&TypedMessage{Type: TypeQueryMessage, EncodedMessage: enc.inner.Bytes()})
	if err_inner != nil {
		return nil, err_outer
	}
	return enc.outer, nil
}

func (enc *Encoder) Encode_error_response(err ErrorResponse) (*bytes.Buffer, error) {
	enc.enc.Reset(enc.inner)
	err_inner := enc.enc.Encode(err)
	if err_inner != nil {
		return nil, err_inner
	}
	enc.enc.Reset(enc.outer)
	err_outer := enc.enc.Encode(&TypedMessage{Type: TypeQueryMessage, EncodedMessage: enc.inner.Bytes()})
	if err_inner != nil {
		return nil, err_outer
	}
	return enc.outer, nil
}

func (enc *Encoder) Encode_query_response(rep QueryResponse) (*bytes.Buffer, error) {
	enc.enc.Reset(enc.inner)
	err_inner := enc.enc.Encode(rep)
	if err_inner != nil {
		return nil, err_inner
	}
	enc.enc.Reset(enc.outer)
	err_outer := enc.enc.Encode(&TypedMessage{Type: TypeQueryMessage, EncodedMessage: enc.inner.Bytes()})
	if err_inner != nil {
		return nil, err_outer
	}
	return enc.outer, nil
}

func MakeRemoteBackend(host string, refill bool) (*RemoteBackend, error) {
	obsConn, obsConnErr := net.Dial("tcp", host)
	if obsConnErr != nil {
		return nil, obsConnErr
	}
	return &RemoteBackend{obsConn: obsConn, StopChan: make(chan bool), host: host}, nil
}

func (db *RemoteBackend) AddObservation(o obs.InputObservation) obs.Observation {
	log.Warn("AddObservation() not implemented")
	return obs.Observation{}
}

func (db *RemoteBackend) obsReconnect(ack chan bool) {
	for {
		log.Warnf("reconnecting to host=`%s`", db.host)
		conn, err := net.Dial("tcp", db.host)
		if err == nil {
			db.obsConn = conn
			ack <- true
			return
		}
		log.Warnf("reconnect failed: %s", err)
		time.Sleep(SleepTimeForReconnect)
	}
}

func (db *RemoteBackend) waitForObsReconnect() bool {
	ack := make(chan bool)
	go db.obsReconnect(ack)
	ok := <-ack
	return ok
}

type Encoder struct {
	inner *bytes.Buffer
	outer *bytes.Buffer
	enc   *codec.Encoder
}

func (enc *Encoder) Release() {
	enc.enc.Release()
}

func MakeEncoder() *Encoder {
	inner := new(bytes.Buffer)
	outer := new(bytes.Buffer)
	h := new(codec.MsgpackHandle)
	h.ExplicitRelease = true
	h.WriteExt = true
	enc := codec.NewEncoder(inner, h)
	return &Encoder{inner: inner, outer: outer, enc: enc}
}

type Decoder struct {
	conn  net.Conn
	outer *bytes.Buffer
	dec   *codec.Decoder
}

func (dec *Decoder) Expect_typed_message() (*TypedMessage, error) {
	dec.dec.Reset(dec.conn)
	var msg TypedMessage
	err := dec.dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (dec *Decoder) Expect_query_message1(buf []byte) (*QueryMessage, error) {
	dec.dec.Reset(bytes.NewBuffer(buf))
	var msg QueryMessage
	err := dec.dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (dec *Decoder) Expect_input_message1(buf []byte) (*InputMessage, error) {
	dec.dec.Reset(bytes.NewBuffer(buf))
	var msg InputMessage
	err := dec.dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func MakeDecoder(conn net.Conn) *Decoder {
	outer := new(bytes.Buffer)
	h := new(codec.MsgpackHandle)
	h.ExplicitRelease = true
	h.WriteExt = true
	dec := codec.NewDecoder(conn, h)
	return &Decoder{outer: outer, dec: dec, conn: conn}
}

func (dec *Decoder) Release() {
	dec.dec.Release()
}

func (dec *Decoder) expect_query_response() (*QueryResponse, error) {
	dec.dec.Reset(dec.conn)
	var msg TypedMessage
	err_outer := dec.dec.Decode(&msg)
	if err_outer != nil {
		return nil, err_outer
	}
	dec.dec.Reset(bytes.NewBuffer(msg.EncodedMessage))
	if msg.Type == TypeErrorResponse {
		var rep ErrorResponse
		err_inner := dec.dec.Decode(&rep)
		if err_inner != nil {
			return nil, err_inner
		}
		return nil, errors.New(rep.Message)
	}
	if msg.Type != TypeQueryResponse {
		return nil, errors.New("invalid query response")
	}
	var rep QueryResponse
	err_inner := dec.dec.Decode(&rep)
	if err_inner != nil {
		return nil, err_inner
	}
	return &rep, nil
}

func (db *RemoteBackend) ConsumeFeed(inChan chan obs.InputObservation) {
	enc := MakeEncoder()
	defer enc.Release()
	for {
		select {
		case <-db.StopChan:
			log.Info("stop request received")
			return
		case obs := <-inChan:
			log.Debug("received observation")
			w, err := enc.encode_observation_message(obs)
			if err != nil {
				log.Warnf("encoding observation failed: %s", err)
				continue
			}
			len, err := w.WriteTo(db.obsConn)
			if err != nil {
				db.obsConn.Close()
				log.Warnf("sending observation failed: %s", err)
				reconnect_ok := db.waitForObsReconnect()
				if !reconnect_ok {
					return
				}
			}
			log.Debugf("sent %d bytes", len)
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

func (db *RemoteBackend) Search(qrdata, qrrname, qrrtype, qsensorID *string) ([]obs.Observation, error) {
	qry := QueryMessage{
		Qrdata:    sanitize(qrdata),
		Hrdata:    qrdata != nil,
		Qrrname:   sanitize(qrrname),
		Hrrname:   qrrname != nil,
		Qrrtype:   sanitize(qrrtype),
		Hrrtype:   qrrtype != nil,
		QsensorID: sanitize(qsensorID),
		HsensorID: qsensorID != nil,
		Limit:     1000,
	}

	conn, conn_err := net.Dial("tcp", db.host)
	if conn_err != nil {
		log.Warnf("unable to connect to backend")
		return []obs.Observation{}, conn_err
	}
	defer conn.Close()

	enc := MakeEncoder()
	defer enc.Release()

	w, enc_err := enc.encode_query_message(qry)
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

	result, err := dec.expect_query_response()

	log.Debugf("received answer")

	if err != nil {
		conn.Close()
		return []obs.Observation{}, err
	}

	return result.Obs, nil
}

func (db *RemoteBackend) TotalCount() (int, error) {
	return -1, nil
}

func (db *RemoteBackend) Shutdown() {
	db.obsConn.Close()
}
