// balboa
// Copyright (c) 2018, DCSO GmbH

package observation

import (
	"encoding/json"
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

func (o *Observation) MarshalJSON() ([]byte, error) {
	type Alias Observation
	return json.Marshal(&struct {
		FirstSeen int64 `json:"time_first"`
		LastSeen  int64 `json:"time_last"`
		*Alias
	}{
		FirstSeen: o.FirstSeen.UTC().Unix(),
		LastSeen:  o.LastSeen.UTC().Unix(),
		Alias:     (*Alias)(o),
	})
}

func (o *Observation) UnmarshalJSON(data []byte) error {
	type Alias Observation
	tmp := &struct {
		FirstSeen int64 `json:"time_first"`
		LastSeen  int64 `json:"time_last"`
		*Alias
	}{
		Alias: (*Alias)(o),
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	o.FirstSeen = time.Unix(tmp.FirstSeen, 0)
	o.LastSeen = time.Unix(tmp.LastSeen, 0)
	return nil
}
