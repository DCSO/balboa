// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"bufio"
	"strconv"
	"strings"
	"time"

	"github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
)

// MakeFjellskaalInputObservations is a MakeObservationFunc that consumes
// input in the format as used by https://github.com/gamelinux/passivedns.
func MakeFjellskaalInputObservations(inputJSON []byte, sensorID string, out chan observation.InputObservation, stop chan bool) error {
	var i int
	log.Info(string(inputJSON))
	scanner := bufio.NewScanner(strings.NewReader(string(inputJSON)))
	for scanner.Scan() {
		select {
		case <-stop:
			return nil
		default:
			vals := strings.Split(scanner.Text(), "||")
			if len(vals) == 9 {
				times := strings.Split(vals[0], ".")
				if len(times) != 2 {
					continue
				}
				epoch, err := strconv.Atoi(times[0])
				if err != nil {
					log.Warn(err)
					continue
				}
				nsec, err := strconv.Atoi(times[1])
				if err != nil {
					log.Warn(err)
					continue
				}
				timestamp := time.Unix(int64(epoch), int64(nsec))
				rrname := vals[4]
				rrtype := vals[5]
				rdata := vals[6]
				count, err := strconv.Atoi(vals[8])
				if err != nil {
					log.Warn(err)
					continue
				}
				o := observation.InputObservation{
					Count:          count,
					Rdata:          strings.TrimRight(rdata, "."),
					Rrname:         strings.TrimRight(rrname, "."),
					Rrtype:         rrtype,
					SensorID:       sensorID,
					TimestampEnd:   timestamp,
					TimestampStart: timestamp,
				}
				i++
				out <- o
			}
		}
	}

	log.Debugf("enqueued %d observations", i)
	return nil
}
