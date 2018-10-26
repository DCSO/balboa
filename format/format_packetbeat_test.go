// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"testing"
	"time"

	"github.com/DCSO/balboa/observation"
	"github.com/sirupsen/logrus/hooks/test"
)

func TestPacketbeatFormatFail(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakePacketbeatInputObservations([]byte(`babanana`), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakePacketbeatInputObservations([]byte(exampleInPacketbeatInvalidTimestamp), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakePacketbeatInputObservations([]byte(exampleInPacketbeatInvalidType), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	close(resChan)
	close(stopChan)

	if len(resultObs) != 0 {
		t.Fail()
	}
	if len(hook.Entries) != 2 {
		t.Fail()
	}
}

func TestPacketbeatFormatEmpty(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakePacketbeatInputObservations([]byte(""), "foo", resChan, stopChan)
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

const exampleInPacketbeatInvalidTimestamp = `{
    "type": "dns",
    "dns": {
        "answers": [{
            "name": "foo.bar.",
            "data": "1.2.3.4.",
            "type": "A",
            "class":"foo"
        }]

    },
    "@timestamp": "2018-10-26T2"
}`

const exampleInPacketbeatInvalidType = `{
    "type": "whatever",
    "dns": {
        "answers": [{
            "name": "foo.bar.",
            "data": "1.2.3.4.",
            "type": "A",
            "class":"foo"
        }]

    },
    "@timestamp": "2018-10-26T21:03:20.222Z"
}`

const exampleInPacketbeat = `{
    "type": "dns",
    "dns": {
        "answers": [{
            "name": "foo.bar.",
            "data": "1.2.3.4.",
            "type": "A",
            "class":"foo"
        }]

    },
    "@timestamp": "2018-10-26T21:03:20.222Z"
}`

func TestPacketbeatFormat(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation, 100)

	stopChan := make(chan bool)
	err := MakePacketbeatInputObservations([]byte(exampleInPacketbeat), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	close(resChan)
	close(stopChan)

	for o := range resChan {
		resultObs = append(resultObs, o)
	}

	if len(resultObs) != 1 {
		t.Fail()
	}

	if len(hook.Entries) != 1 {
		t.Fail()
	}
}
