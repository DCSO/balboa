// balboa
// Copyright (c) 2018, DCSO GmbH

package feeder

import (
	"balboa/format"
	"balboa/observation"
)

// Feeder is an interface of a component that accepts observations in a
// specific format and feeds them into a channel of InputObservations.
// An input decoder in the form of a MakeObservationFunc describes the
// operations necessary to transform the input format into an
// InputObservation.
type Feeder interface {
	Run(chan observation.InputObservation) error
	SetInputDecoder(format.MakeObservationFunc)
	Stop(chan bool)
}
