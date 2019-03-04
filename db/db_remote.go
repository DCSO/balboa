// balboa
// Copyright (c) 2019, DCSO GmbH

package db

import (
	"bytes"
	"errors"
	"net"

	obs "github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
	"github.com/ugorji/go/codec"
)

type RemoteBackend struct {
	host     string
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

func (enc *Encoder) encode_observation_message(o obs.InputObservation) (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	enc.enc.Reset(enc.inner)
	inner_err := enc.enc.Encode(InputMessage{Obs: []obs.InputObservation{o}})
	if inner_err != nil {
		return nil, inner_err
	}
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeInputMessage, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (enc *Encoder) encode_query_message(qry QueryMessage) (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	enc.enc.Reset(enc.inner)
	inner_err := enc.enc.Encode(qry)
	if inner_err != nil {
		return nil, inner_err
	}
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeQueryMessage, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (enc *Encoder) Encode_error_response(err ErrorResponse) (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	inner_err := enc.enc.Encode(err)
	if inner_err != nil {
		return nil, inner_err
	}
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeQueryMessage, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (enc *Encoder) Encode_query_response(rep QueryResponse) (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	enc.enc.Reset(enc.inner)
	inner_err := enc.enc.Encode(rep)
	if inner_err != nil {
		return nil, inner_err
	}
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeQueryResponse, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func MakeRemoteBackend(host string, refill bool) (*RemoteBackend, error) {
	return &RemoteBackend{StopChan: make(chan bool), host: host}, nil
}

func (db *RemoteBackend) AddObservation(o obs.InputObservation) obs.Observation {
	log.Warn("AddObservation() not implemented")
	return obs.Observation{}
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
	conn net.Conn
	outer_dec *codec.Decoder
	inner_dec *codec.Decoder
}

func (dec *Decoder) Expect_typed_message() (*TypedMessage, error) {
	//dec.outer_dec.Reset(dec.conn)
	var msg TypedMessage
	err := dec.outer_dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (dec *Decoder) Expect_query_message1(buf []byte) (*QueryMessage, error) {
	dec.inner_dec.Reset(bytes.NewBuffer(buf))
	var msg QueryMessage
	err := dec.inner_dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (dec *Decoder) Expect_input_message1(buf []byte) (*InputMessage, error) {
	dec.inner_dec.Reset(bytes.NewBuffer(buf))
	var msg InputMessage
	err := dec.inner_dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func MakeDecoder(conn net.Conn) *Decoder {
	outer_h := new(codec.MsgpackHandle)
	outer_h.ExplicitRelease = true
	outer_h.WriteExt = true
	outer_dec := codec.NewDecoder(conn, outer_h)
	inner_h := new(codec.MsgpackHandle)
	inner_h.ExplicitRelease = true
	inner_h.WriteExt = true
	inner_dec := codec.NewDecoder(new(bytes.Buffer), inner_h)
	return &Decoder{inner_dec: inner_dec, outer_dec: outer_dec, conn: conn}
}

func (dec *Decoder) Release() {
	dec.inner_dec.Release()
	dec.outer_dec.Release()
}

func (dec *Decoder) expect_query_response() (*QueryResponse, error) {
	msg, msg_err := dec.Expect_typed_message()
	if msg_err != nil {
		return nil, msg_err
	}
	dec.inner_dec.Reset(bytes.NewBuffer(msg.EncodedMessage))
	if msg.Type == TypeErrorResponse {
		var rep ErrorResponse
		inner_err := dec.inner_dec.Decode(&rep)
		if inner_err != nil {
			return nil, inner_err
		}
		return nil, errors.New(rep.Message)
	}
	if msg.Type != TypeQueryResponse {
		return nil, errors.New("invalid query response")
	}
	var rep QueryResponse
	inner_err := dec.inner_dec.Decode(&rep)
	if inner_err != nil {
		return nil, inner_err
	}
	return &rep, nil
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
			w, err := enc.encode_observation_message(obs)
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
			//log.Warnf("sent %v bytes",n)
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
		log.Warnf("connecting to backend failed %v", conn_err)
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
	close(db.StopChan)
}
