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

const (
	TypeInputRequest             = 1
	TypeQueryRequest             = 2
	TypeBackupRequest            = 3
	TypeDumpRequest              = 4
	TypeErrorResponse            = 128
	TypeQueryResponse            = 129
	TypeQueryStreamStartResponse = 130
	TypeQueryStreamDataResponse  = 131
	TypeQueryStreamEndResponse   = 132
)

type TypedMessage struct {
	Type           uint8  `codec:"T"`
	EncodedMessage []byte `codec:"M"`
}

type BackupRequest struct {
	Path string `codec:"P"`
}

type DumpRequest struct {
	Path string `codec:"P"`
}

type QueryRequest struct {
	Qrdata, Qrrname, Qrrtype, QsensorID string
	Hrdata, Hrrname, Hrrtype, HsensorID bool
	Limit                               int
}

type InputRequest struct {
	Obs obs.InputObservation `codec:"O"`
}

type QueryResponse struct {
	Obs []obs.Observation `codec:"O"`
}

type ErrorResponse struct {
	Message string
}

type Encoder struct {
	inner *bytes.Buffer
	outer *bytes.Buffer
	enc   *codec.Encoder
}

type Decoder struct {
	conn      net.Conn
	outer_dec *codec.Decoder
	inner_dec *codec.Decoder
}

func (enc *Encoder) Release() {
	enc.enc.Release()
}

func (dec *Decoder) ExpectQueryResponse() (*QueryResponse, error) {
	msg, msg_err := dec.ExpectTypedMessage()
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
	if msg.Type == TypeQueryResponse {
		var rep QueryResponse
		inner_err := dec.inner_dec.Decode(&rep)
		if inner_err != nil {
			return nil, inner_err
		}
		return &rep, nil
	} else if msg.Type == TypeQueryStreamStartResponse {
		log.Debugf("got stream start response")
		return dec.ExpectQueryStreamResponse()
	} else {
		return nil, errors.New("invalid query response")
	}
}

func (enc *Encoder) EncodeInputRequest(o obs.InputObservation) (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	enc.enc.Reset(enc.inner)
	inner_err := enc.enc.Encode(InputRequest{Obs: o})
	if inner_err != nil {
		return nil, inner_err
	}
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeInputRequest, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (enc *Encoder) EncodeQueryRequest(qry QueryRequest) (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	enc.enc.Reset(enc.inner)
	inner_err := enc.enc.Encode(qry)
	if inner_err != nil {
		return nil, inner_err
	}
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeQueryRequest, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (enc *Encoder) EncodeQueryStreamStartResponse() (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	enc.enc.Reset(enc.inner)
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeQueryStreamStartResponse, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (enc *Encoder) EncodeQueryStreamEndResponse() (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	enc.enc.Reset(enc.inner)
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeQueryStreamEndResponse, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (enc *Encoder) EncodeQueryStreamDataResponse(entry obs.Observation) (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	enc.enc.Reset(enc.inner)
	inner_err := enc.enc.Encode(entry)
	if inner_err != nil {
		return nil, inner_err
	}
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeQueryStreamDataResponse, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (dec *Decoder) ExpectInputRequestFromBytes(buf []byte) (*InputRequest, error) {
	dec.inner_dec.Reset(bytes.NewBuffer(buf))
	var msg InputRequest
	err := dec.inner_dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (enc *Encoder) EncodeErrorResponse(err ErrorResponse) (*bytes.Buffer, error) {
	enc.inner.Reset()
	enc.outer.Reset()
	inner_err := enc.enc.Encode(err)
	if inner_err != nil {
		return nil, inner_err
	}
	enc.enc.Reset(enc.outer)
	outer_err := enc.enc.Encode(&TypedMessage{Type: TypeQueryRequest, EncodedMessage: enc.inner.Bytes()})
	if outer_err != nil {
		return nil, outer_err
	}
	return enc.outer, nil
}

func (dec *Decoder) ExpectTypedMessage() (*TypedMessage, error) {
	var msg TypedMessage
	err := dec.outer_dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (dec *Decoder) ExpectQueryRequestFromBytes(buf []byte) (*QueryRequest, error) {
	dec.inner_dec.Reset(bytes.NewBuffer(buf))
	var msg QueryRequest
	err := dec.inner_dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (dec *Decoder) ExpectBackupRequestFromBytes(buf []byte) (*BackupRequest, error) {
	dec.inner_dec.Reset(bytes.NewBuffer(buf))
	var msg BackupRequest
	err := dec.inner_dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (dec *Decoder) ExpectDumpRequestFromBytes(buf []byte) (*DumpRequest, error) {
	dec.inner_dec.Reset(bytes.NewBuffer(buf))
	var msg DumpRequest
	err := dec.inner_dec.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (dec *Decoder) ExpectQueryStreamResponse() (*QueryResponse, error) {
	var res []obs.Observation
	for {
		msg, msg_err := dec.ExpectTypedMessage()
		if msg_err != nil {
			return nil, msg_err
		}
		dec.inner_dec.Reset(bytes.NewBuffer(msg.EncodedMessage))
		if msg.Type == TypeErrorResponse {
			var rep ErrorResponse
			inner_err := dec.inner_dec.Decode(&rep)
			if inner_err != nil {
				log.Warnf("got error response during stream data")
				return nil, inner_err
			}
			return nil, errors.New(rep.Message)
		}
		if msg.Type == TypeQueryStreamDataResponse {
			var rep obs.Observation
			inner_err := dec.inner_dec.Decode(&rep)
			if inner_err != nil {
				log.Warnf("decoding stream data response failed")
				return nil, inner_err
			}
			res = append(res, rep)
		} else if msg.Type == TypeQueryStreamEndResponse {
			return &QueryResponse{Obs: res}, nil
		}
	}
}
