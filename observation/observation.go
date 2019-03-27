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
	Count     int       `json:"count" codec:"C"`
	FirstSeen int       `json:"time_first" codec:"F"`
	LastSeen  int       `json:"time_last" codec:"L"`
	RRType    string    `json:"rrtype" codec:"T"`
	RRName    string    `json:"rrname" codec:"N"`
	RData     string    `json:"rdata" codec:"D"`
	SensorID  string    `json:"sensor_id" codec:"I"`
}
