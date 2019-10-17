// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"testing"
	"time"

	"balboa/observation"
	"github.com/sirupsen/logrus/hooks/test"
)

func TestFjellskaalFormatFail(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeFjellskaalInputObservations([]byte("1322849924||10.1.1.1||8.8.8.8||IN||upload.youtube.com.||A||74.125.43.117||46587||5"), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeFjellskaalInputObservations([]byte("1322849924||10.1.1.1||8.8.8.8||upload.youtube.com.||A||74.125.43.117||46587||5"), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeFjellskaalInputObservations([]byte("X.2332||10.1.1.1||8.8.8.8||IN||upload.youtube.com.||A||74.125.43.117||46587||5"), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeFjellskaalInputObservations([]byte("X.X||10.1.1.1||8.8.8.8||IN||upload.youtube.com.||A||74.125.43.117||46587||5"), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeFjellskaalInputObservations([]byte("23232.X||10.1.1.1||8.8.8.8||IN||upload.youtube.com.||A||74.125.43.117||46587||5"), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeFjellskaalInputObservations([]byte("1322849924.244555||10.1.1.1||8.8.8.8||IN||upload.youtube.com.||A||74.125.43.117||46587||bar"), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	close(resChan)
	close(stopChan)

	if len(resultObs) != 0 {
		t.Fail()
	}
	if len(hook.Entries) != 6 {
		t.Fail()
	}
}

func TestFjellskaalFormatEmpty(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeFjellskaalInputObservations([]byte(""), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	close(resChan)
	close(stopChan)

	if len(resultObs) != 0 {
		t.Fail()
	}
	if len(hook.Entries) != 0 {
		t.Fail()
	}
}

const exampleInFjellskaal = `1322849924.408856||10.1.1.1||8.8.8.8||IN||upload.youtube.com.||A||74.125.43.117||46587||5
1322849924.408857||10.1.1.1||8.8.8.8||IN||upload.youtube.com.||A||74.125.43.116||420509||5
1322849924.408858||10.1.1.1||8.8.8.8||IN||www.adobe.com.||CNAME||www.wip4.adobe.com.||43200||8
1322849924.408859||10.1.1.1||8.8.8.8||IN||www.adobe.com.||A||193.104.215.61||43200||8
1322849924.408860||10.1.1.1||8.8.8.8||IN||i1.ytimg.com.||CNAME||ytimg.l.google.com.||43200||3
1322849924.408861||10.1.1.1||8.8.8.8||IN||clients1.google.com.||A||173.194.32.3||43200||2
`

func TestFjellskaalFormat(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeFjellskaalInputObservations([]byte(exampleInFjellskaal), "foo", resChan, stopChan)
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
