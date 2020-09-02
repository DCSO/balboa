// balboa
// Copyright (c) 2020, DCSO GmbH

package db

import (
	"github.com/DCSO/balboa/observation"
)

type MockDB struct {
	obs []observation.Observation
}

func MakeMockDB() *MockDB {
	return &MockDB{
		obs: make([]observation.Observation, 0),
	}
}

func (m *MockDB) ConsumeFeed(inChan chan observation.InputObservation) {
	for in := range inChan {
		o := observation.Observation{
			RRName:    in.Rrname,
			RRType:    in.Rrtype,
			Count:     in.Count,
			RData:     in.Rdata,
			FirstSeen: in.TimestampStart,
			LastSeen:  in.TimestampEnd,
			SensorID:  in.SensorID,
		}
		m.obs = append(m.obs, o)
	}
}

func (m *MockDB) Backup(path string) {
	return
}

func (m *MockDB) Dump(path string) {
	return
}

func (m *MockDB) Shutdown() {
	return
}

func (m *MockDB) TotalCount() (int, error) {
	return len(m.obs), nil
}

func (m *MockDB) Search(qrdata, qrrname, qrrtype, qsensorID *string, limit int) ([]observation.Observation, error) {
	retObs := make([]observation.Observation, 0)
	for _, o := range m.obs {
		if qrdata != nil {
			if *qrdata == o.RData {
				retObs = append(retObs, o)
			}
		}
		if qrrname != nil {
			if *qrrname == o.RRName {
				retObs = append(retObs, o)
			}
		}
	}
	return retObs, nil
}
