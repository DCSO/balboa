// balboa
// Copyright (c) 2018, DCSO GmbH

package db

import "github.com/DCSO/balboa/observation"

// ObservationDB is the common DB onstance used for this balboa session.
var ObservationDB DB

// DB abstracts a database backend for observation storage.
type DB interface {
	AddObservation(observation.InputObservation) observation.Observation
	ConsumeFeed(chan observation.InputObservation)
	Shutdown()
	TotalCount() (int, error)
	Search(*string, *string, *string, *string) ([]observation.Observation, error)
}
