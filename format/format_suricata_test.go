// balboa
// Copyright (c) 2018, DCSO GmbH

package format

import (
	"testing"
	"time"

	"balboa/observation"
	"github.com/sirupsen/logrus/hooks/test"
)

func TestSuricataFormatFail(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeSuricataInputObservations([]byte(`babanana`), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeSuricataInputObservations([]byte(exampleInSuricataInvalidTimestamp), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeSuricataInputObservations([]byte(exampleInSuricataInvalidType), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeSuricataInputObservations([]byte(exampleInSuricataInvalidType2), "foo", resChan, stopChan)
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

func TestSuricataFormatEmpty(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation)
	go func() {
		for o := range resChan {
			resultObs = append(resultObs, o)
		}
	}()

	stopChan := make(chan bool)
	err := MakeSuricataInputObservations([]byte(""), "foo", resChan, stopChan)
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

const exampleInSuricataInvalidTimestamp = `{
    "timestamp": "2009-11-24T21:",
    "event_type": "dns",
    "src_ip": "192.168.2.7",
    "src_port": 53,
    "dest_ip": "x.x.250.50",
    "dest_port": 23242,
    "proto": "UDP",
    "dns": {
		"type": "answer",
		"id":16000,
		"flags":"8180",
		"qr":true,
		"rd":true,
		"ra":true,
		"rcode":"NOERROR",
		"rrname": "twitter.com",
		"rrtype":"A",
		"ttl":8,
		"rdata": "199.16.156.6"
	}
}`

const exampleInSuricataInvalidType = `{
    "timestamp": "2009-11-24T21:27:09.534255-0100",
    "event_type": "foo",
    "src_ip": "192.168.2.7",
    "src_port": 53,
    "dest_ip": "x.x.250.50",
    "dest_port": 23242,
    "proto": "UDP",
    "dns": {
		"type": "answer",
		"id":16000,
		"flags":"8180",
		"qr":true,
		"rd":true,
		"ra":true,
		"rcode":"NOERROR",
		"rrname": "twitter.com",
		"rrtype":"A",
		"ttl":8,
		"rdata": "199.16.156.6"
	}
}`

const exampleInSuricataInvalidType2 = `{
    "timestamp": "2009-11-24T21:27:09.534255-0100",
    "event_type": "dns",
    "src_ip": "192.168.2.7",
    "src_port": 53,
    "dest_ip": "x.x.250.50",
    "dest_port": 23242,
    "proto": "UDP",
    "dns": {
		"type": "foo",
		"id":16000,
		"flags":"8180",
		"qr":true,
		"rd":true,
		"ra":true,
		"rcode":"NOERROR",
		"rrname": "twitter.com",
		"rrtype":"A",
		"ttl":8,
		"rdata": "199.16.156.6"
	}
}`

const exampleInSuricataV2 = `{
    "timestamp": "2009-11-24T21:27:09.534255-0100",
    "event_type": "dns",
    "src_ip": "192.168.2.7",
    "src_port": 53,
    "dest_ip": "x.x.250.50",
    "dest_port": 23242,
    "proto": "UDP",
    "dns": {
        "version": 2,
        "type": "answer",
        "id": 45444,
        "flags": "8180",
        "qr": true,
        "rd": true,
        "ra": true,
        "rcode": "NOERROR",
        "answers": [
        {
            "rrname": "www.suricata-ids.org",
            "rrtype": "CNAME",
            "ttl": 3324,
            "rdata": "suricata-ids.org"
        },
        {
            "rrname": "suricata-ids.org",
            "rrtype": "A",
            "ttl": 10,
            "rdata": "192.0.78.24"
        },
        {
            "rrname": "suricata-ids.org",
            "rrtype": "A",
            "ttl": 10,
            "rdata": "192.0.78.25"
        }
        ]
    }
}`

const exampleInSuricataV2Grouped = `{
    "timestamp": "2009-11-24T21:27:09.534255-0100",
    "event_type": "dns",
    "src_ip": "192.168.2.7",
    "src_port": 53,
    "dest_ip": "x.x.250.50",
    "dest_port": 23242,
    "proto": "UDP",
    "dns": {
		"version": 2,
		"type": "answer",
		"id": 18523,
		"flags": "8180",
		"qr": true,
		"rd": true,
		"ra": true,
		"rcode": "NOERROR",
		"grouped": {
		  "A": [
			"192.0.78.24",
			"192.0.78.25"
		  ],
		  "CNAME": [
			"suricata-ids.org"
		  ]
		}
	}
}`

const exampleInSuricataV1 = `{
    "timestamp": "2009-11-24T21:27:09.534255-0100",
    "event_type": "dns",
    "src_ip": "192.168.2.7",
    "src_port": 53,
    "dest_ip": "x.x.250.50",
    "dest_port": 23242,
    "proto": "UDP",
    "dns": {
		"type": "answer",
		"id":16000,
		"flags":"8180",
		"qr":true,
		"rd":true,
		"ra":true,
		"rcode":"NOERROR",
		"rrname": "twitter.com",
		"rrtype":"A",
		"ttl":8,
		"rdata": "199.16.156.6"
	}
}`

func TestSuricataFormat(t *testing.T) {
	hook := test.NewGlobal()

	resultObs := make([]observation.InputObservation, 0)
	resChan := make(chan observation.InputObservation, 100)

	stopChan := make(chan bool)
	err := MakeSuricataInputObservations([]byte(exampleInSuricataV1), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeSuricataInputObservations([]byte(exampleInSuricataV2), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	err = MakeSuricataInputObservations([]byte(exampleInSuricataV2Grouped), "foo", resChan, stopChan)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	close(resChan)
	close(stopChan)

	for o := range resChan {
		resultObs = append(resultObs, o)
	}

	if len(resultObs) != 7 {
		t.Fail()
	}
	if len(hook.Entries) != 3 {
		t.Fail()
	}
}
