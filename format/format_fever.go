// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"encoding/json"
	"time"

	"github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
)

type rdata struct {
	AnsweringHost string `json:"answering_host"`
	Count         int    `json:"count"`
	Rcode         string `json:"rcode"`
	Rdata         string `json:"rdata"`
	Rrtype        string `json:"rrtype"`
	Type          string `json:"type"`
}

type inputJSONstruct struct {
	DNS map[string]struct {
		Rdata []rdata `json:"rdata"`
	} `json:"dns"`
	TimestampEnd   time.Time `json:"timestamp_end"`
	TimestampStart time.Time `json:"timestamp_start"`
}

// MakeFeverAggregateInputObservations is a MakeObservationFunc that accepts
// input in suristasher/FEVER's JSON format.
func MakeFeverAggregateInputObservations(inputJSON []byte, sensorID string, out chan observation.InputObservation, stop chan bool) error {
	var in inputJSONstruct
	var i int64
	err := json.Unmarshal(inputJSON, &in)
	if err != nil {
		log.Warn(err)
		return nil
	}
	for k, v := range in.DNS {
		select {
		case <-stop:
			return nil
		default:
			for _, v2 := range v.Rdata {
				select {
				case <-stop:
					return nil
				default:
					o := observation.InputObservation{
						Count:          v2.Count,
						Rdata:          v2.Rdata,
						Rrname:         k,
						Rrtype:         v2.Rrtype,
						SensorID:       sensorID,
						TimestampEnd:   in.TimestampEnd,
						TimestampStart: in.TimestampStart,
					}
					i++
					out <- o
				}
			}
		}
	}
	log.Infof("enqueued %d observations", i)
	return nil
}
