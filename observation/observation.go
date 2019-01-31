// balboa
// Copyright (c) 2018, DCSO GmbH

package observation

import (
	uuid "github.com/satori/go.uuid"
)

// Observation represents a DNS answer, potentially repeated, observed on a
// given sensor stating a specific RR set.
type Observation struct {
	ID        uuid.UUID `json:"-" codec:"-"`
	Count     int       `json:"count"`
	FirstSeen int       `json:"time_first"`
	LastSeen  int       `json:"time_last"`
	RRType    string    `json:"rrtype"`
	RRName    string    `json:"rrname"`
	RData     string    `json:"rdata"`
	SensorID  string    `json:"sensor_id"`
}
