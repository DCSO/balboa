// balboa
// Copyright (c) 2018, DCSO GmbH

package db

import "balboa/observation"

// ObservationDB is the common DB instance used for this balboa session.
var ObservationDB DB

// DB abstracts a database backend for observation storage.
type DB interface {
	//AddObservation(observation.InputObservation) observation.Observation
	ConsumeFeed(chan observation.InputObservation)
	Backup(path string)
	Dump(path string)
	Shutdown()
	TotalCount() (int, error)
	Search(*string, *string, *string, *string, int) ([]observation.Observation, error)
}
