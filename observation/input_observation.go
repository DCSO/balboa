// balboa
// Copyright (c) 2018, DCSO GmbH

package observation

import (
	"time"
)

// InputObservation is a minimal, small observation structure to be used as
// the minimal common input type for all consumers.
type InputObservation struct {
	Count          int
	Rcode          string
	Rdata          string
	Rrtype         string
	Rrname         string
	SensorID       string
	TimestampEnd   time.Time
	TimestampStart time.Time
}

// InChan is the global input channel delivering InputObservations from
// feeders to consumers.
var InChan chan InputObservation

func init() {
	InChan = make(chan InputObservation, 50000)
}
