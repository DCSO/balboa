// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"testing"
	"time"

	"github.com/DCSO/balboa/observation"
	"github.com/sirupsen/logrus/hooks/test"
)

func TestFeverFormatFail(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeFeverAggregateInputObservations([]byte(`babanana`), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	close(resChan)
	close(stopChan)

	if len(resultObs) != 0 {
		t.Fail()
	}
	if len(hook.Entries) != 1 {
		t.Fail()
	}
}

func TestFeverFormatEmpty(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeFeverAggregateInputObservations([]byte(""), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	close(resChan)
	close(stopChan)

	if len(resultObs) != 0 {
		t.Fail()
	}
	if len(hook.Entries) != 1 {
		t.Fail()
	}
}

const exampleInFever = `{
    "dns": {
        "foo.bar": {
			"rdata": [
				{
					"rdata": "1.2.3.4",
					"count":2,
					"rrtype": "A",
					"type":"answer"
				},
				{
					"rdata": "1.2.3.5",
					"count":1,
					"rrtype": "A",
					"type":"answer"
				}
			]
		}
    },
    "timestamp_start":"2018-10-26T21:02:20+00:00",
    "timestamp_end":"2018-10-26T21:03:20+00:00"
}`

func TestFeverFormat(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeFeverAggregateInputObservations([]byte(exampleInFever), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	close(resChan)
	close(stopChan)

	if len(hook.Entries) != 1 {
		t.Fail()
	}
}
