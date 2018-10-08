// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import "github.com/DCSO/balboa/observation"

// MakeObservationFunc is a function that accepts a byte array with input
// obtained from a feeder, a sensor ID, a channel for the generated
// InputObservations, and a channel to signal a stop.
type MakeObservationFunc func([]byte, string, chan observation.InputObservation, chan bool) error
