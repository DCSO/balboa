// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"encoding/json"
	"time"

	"github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
)

type suriDNSEntry struct {
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
	DNS       struct {
		Type    string `json:"type"`
		Version int    `json:"version"`
		Rrtype  string `json:"rrtype"`
		Rcode   string `json:"rcode"`
		Rrname  string `json:"rrname"`
		TTL     int    `json:"ttl"`
		Rdata   string `json:"rdata"`
		Answers []struct {
			Rrname string `json:"rrname"`
			Rrtype string `json:"rrtype"`
			TTL    int    `json:"ttl"`
			Rdata  string `json:"rdata"`
		} `json:"answers"`
		Grouped map[string][]string `json:"grouped"`
	} `json:"dns"`
}

// MakeSuricataInputObservations is a MakeObservationFunc that accepts input
// in Suricata's EVE JSON format (DNS type version 1 and 2 are supported).
func MakeSuricataInputObservations(inputJSON []byte, sensorID string, out chan observation.InputObservation, stop chan bool) error {
	var in suriDNSEntry
	var i int
	err := json.Unmarshal(inputJSON, &in)
	if err != nil {
		log.Warn(err)
		return nil
	}
	if in.EventType != "dns" {
		return nil
	}
	if in.DNS.Type != "answer" {
		return nil
	}
	tst, err := time.Parse("2006-01-02T15:04:05.999999-0700", in.Timestamp)
	if err != nil {
		log.Warn(err)
		return nil
	}
	if in.DNS.Version == 2 {
		// v2 format
		if len(in.DNS.Answers) > 0 {
			// "detailed" format
			for _, answer := range in.DNS.Answers {
				o := observation.InputObservation{
					Count:          1,
					Rdata:          answer.Rdata,
					Rrname:         answer.Rrname,
					Rrtype:         answer.Rrtype,
					SensorID:       sensorID,
					TimestampEnd:   tst,
					TimestampStart: tst,
					Tags:           map[string]struct{}{},
					Selectors:      map[interface{}]struct{}{},
				}
				i++
				out <- o
			}
		} else {
			// "grouped" format
			for rrtype, rdataArr := range in.DNS.Grouped {
				for _, rdata := range rdataArr {
					o := observation.InputObservation{
						Count:          1,
						Rdata:          rdata,
						Rrname:         in.DNS.Rrname,
						Rrtype:         rrtype,
						SensorID:       sensorID,
						TimestampEnd:   tst,
						TimestampStart: tst,
						Tags:           map[string]struct{}{},
						Selectors:      map[interface{}]struct{}{},
					}
					i++
					out <- o
				}
			}
		}
	} else {
		// v1 format
		o := observation.InputObservation{
			Count:          1,
			Rdata:          in.DNS.Rdata,
			Rrname:         in.DNS.Rrname,
			Rrtype:         in.DNS.Rrtype,
			SensorID:       sensorID,
			TimestampEnd:   tst,
			TimestampStart: tst,
			Tags:           map[string]struct{}{},
			Selectors:      map[interface{}]struct{}{},
		}
		i++
		out <- o
	}
	log.Infof("enqueued %d observations", i)
	return nil
}
