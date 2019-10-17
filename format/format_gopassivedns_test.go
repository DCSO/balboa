// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"testing"
	"time"

	"balboa/observation"
	"github.com/sirupsen/logrus/hooks/test"
)

func TestGopassivednsFormatFail(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeGopassivednsInputObservations([]byte(`{"query_id":43264,"rcode":0,"q":"github.com","qtype":"A","a":"192.30.253.112","atype":"A","ttl":60,"dst":"9.9.9.9","src":"192.168.1.79","tstamp":"2018-10-26 19 +0000 UTC","elapsed":35879000,"sport":"40651","level":"","bytes":102,"protocol":"udp","truncated":false,"aa":false,"rd":true,"ra":false}`), "foo", resChan, stopChan)
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

func TestGopassivednsFormatEmpty(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeGopassivednsInputObservations([]byte(""), "foo", resChan, stopChan)
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

const exampleInGopassivedns = `{"query_id":43264,"rcode":0,"q":"github.com","qtype":"A","a":"192.30.253.112","atype":"A","ttl":60,"dst":"9.9.9.9","src":"192.168.1.79","tstamp":"2018-10-26 19:32:36.141184 +0000 UTC","elapsed":35879000,"sport":"40651","level":"","bytes":102,"protocol":"udp","truncated":false,"aa":false,"rd":true,"ra":false}
`

func TestGopassivednsFormat(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeGopassivednsInputObservations([]byte(exampleInGopassivedns), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	close(resChan)
	close(stopChan)

	if len(hook.Entries) != 0 {
		t.Fail()
	}
}
