// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
)

type pbEntry struct {
	Type      string `json:"type"`
	Timestamp string `json:"@timestamp"`
	DNS       struct {
		Answers []struct {
			Name  string `json:"name"`
			Class string `json:"class"`
			Type  string `json:"type"`
			Data  string `json:"data"`
			TTL   string `json:"ttl"`
		} `json:"answers"`
	} `json:"dns"`
}

// MakePacketbeatInputObservations is a MakeObservationFunc that accepts a
// JSON format from Packetbeat via Logstash. See doc/packetbeat_config.txt
// for more information.
func MakePacketbeatInputObservations(inputJSON []byte, sensorID string, out chan observation.InputObservation, stop chan bool) error {
	var in pbEntry
	var i int
	err := json.Unmarshal(inputJSON, &in)
	if err != nil {
		log.Warn(err)
		return nil
	}
	if in.Type != "dns" {
		return nil
	}
	tst, err := time.Parse("2006-01-02T15:04:05.999Z07", in.Timestamp)
	if err != nil {
		log.Warn(err)
		return nil
	}
	for _, answer := range in.DNS.Answers {
		select {
		case <-stop:
			return nil
		default:
			o := observation.InputObservation{
				Count:          1,
				Rdata:          strings.TrimRight(answer.Data, "."),
				Rrname:         strings.TrimRight(answer.Name, "."),
				Rrtype:         answer.Type,
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
	log.Infof("enqueued %d observations", i)
	return nil
}
