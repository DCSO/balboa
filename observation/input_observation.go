// balboa
// Copyright (c) 2018, DCSO GmbH

package observation

import (
	"time"
)

// InputObservation is a minimal, small observation structure to be used as
// the minimal common input type for all consumers.
type InputObservation struct {
	Count          uint                     `codec:"C"`
	Rcode          string                   `codec:"-"`
	Rdata          string                   `codec:"D"`
	Rrtype         string                   `codec:"T"`
	Rrname         string                   `codec:"N"`
	SensorID       string                   `codec:"I"`
	TimestampEnd   time.Time                `codec:"L"`
	TimestampStart time.Time                `codec:"F"`
	Tags           map[string]struct{}      `codec:"G,omitempty"`
	Selectors      map[interface{}]struct{} `codec:"S,omitempty"`
}

// InChan is the global input channel delivering InputObservations from
// feeders to consumers.
var InChan chan InputObservation

func init() {
	InChan = make(chan InputObservation, 50000)
}
