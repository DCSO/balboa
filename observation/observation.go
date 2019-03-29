// balboa
// Copyright (c) 2018, DCSO GmbH

package observation

import (
	"time"
	uuid "github.com/satori/go.uuid"
)

// Observation represents a DNS answer, potentially repeated, observed on a
// given sensor stating a specific RR set.
type Observation struct {
	ID        uuid.UUID `json:"-" codec:"-"`
	Count     uint      `json:"count" codec:"C"`
	FirstSeen time.Time `json:"time_first" codec:"F"`
	LastSeen  time.Time `json:"time_last" codec:"L"`
	RRType    string    `json:"rrtype" codec:"T"`
	RRName    string    `json:"rrname" codec:"N"`
	RData     string    `json:"rdata" codec:"D"`
	SensorID  string    `json:"sensor_id" codec:"I"`
}
