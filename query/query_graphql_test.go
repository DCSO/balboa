// balboa
// Copyright (c) 2020, DCSO GmbH

package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DCSO/balboa/db"
	"github.com/DCSO/balboa/observation"
)

func makeGraphQLQuery(t *testing.T, keyword, addr, field string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(`{"query":"query { entries(` + field + `:\"` + keyword + `\") { rrname rdata time_first time_last count rrtype sensor_id }}"}`)
	targetURL := "http://" + addr + "/query"
	resp, err := http.Post(targetURL, "application/json", &buf)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

type graphQLObservationResult struct {
	Data struct {
		Entries []struct {
			Rrname    string
			Rdata     string
			Count     uint
			Rrtype    string
			TimeFirst uint
			TimeLast  uint
			SensorID  string `json:"sensor_id"`
		}
	}
}

func parseGraphQLResult(t *testing.T, body []byte) []observation.Observation {
	var qlres graphQLObservationResult
	err := json.Unmarshal(body, &qlres)
	if err != nil {
		t.Fatal(err)
	}
	res := make([]observation.Observation, 0)
	for _, e := range qlres.Data.Entries {
		o := observation.Observation{
			RRName:    e.Rrname,
			RData:     e.Rdata,
			RRType:    e.Rrtype,
			Count:     e.Count,
			FirstSeen: time.Unix(int64(e.TimeFirst), 0),
			LastSeen:  time.Unix(int64(e.TimeLast), 0),
			SensorID:  e.SensorID,
		}
		res = append(res, o)
	}
	return res
}

var gq GraphQLFrontend

func testGraphQLQueryOne(t *testing.T) {
	// run queries: foo
	body, err := makeGraphQLQuery(t, "foo", gq.GetAddr(), "rrname")
	if err != nil {
		t.Fatal(err)
	}
	res := parseGraphQLResult(t, body)
	if len(res) != 1 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	if res[0].RRName != "foo" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
}

func testGraphQLQueryAnother(t *testing.T) {
	// run queries: foo
	body, err := makeGraphQLQuery(t, "bar", gq.GetAddr(), "rrname")
	if err != nil {
		t.Fatal(err)
	}
	res := parseGraphQLResult(t, body)
	if len(res) != 1 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	if res[0].RRName != "bar" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
}

func testGraphQLQueryTwo(t *testing.T) {
	// run queries: bar
	body, err := makeGraphQLQuery(t, "1.2.3.4", gq.GetAddr(), "rdata")
	if err != nil {
		t.Fatal(err)
	}
	res := parseGraphQLResult(t, body)
	if len(res) != 2 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	if res[0].RRName != "foo" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
	if res[1].RRName != "baz" {
		t.Fatalf("unexpected result rrname: %s", res[1].RRName)
	}
}

func testGraphQLQueryEmpty(t *testing.T) {
	// run queries: valid query path but nonexisting keyword
	body, err := makeGraphQLQuery(t, "nonexist", gq.GetAddr(), "rrname")
	if err != nil {
		t.Fatal(err)
	}
	res := parseGraphQLResult(t, body)
	if len(res) != 0 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
}

func testGraphQLQueryFail(t *testing.T) {
	// run queries: failed query due to invalid path
	resp, err := http.Get("http://" + gq.GetAddr() + fmt.Sprintf("/blurb/foo"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
}

func TestQueryGraphQL(t *testing.T) {
	// import test data
	db.ObservationDB = db.MakeMockDB()
	inChan := make(chan observation.InputObservation, 100)
	go db.ObservationDB.ConsumeFeed((inChan))
	inChan <- observation.InputObservation{
		Rrname:         "foo",
		Rdata:          "1.2.3.4",
		Rrtype:         "A",
		Count:          10,
		SensorID:       "abc",
		TimestampStart: time.Now(),
		TimestampEnd:   time.Now().Add(1 * time.Second),
	}
	inChan <- observation.InputObservation{
		Rrname:         "bar",
		Rdata:          "1.2.3.5",
		Rrtype:         "A",
		Count:          10,
		SensorID:       "abc",
		TimestampStart: time.Now(),
		TimestampEnd:   time.Now().Add(1 * time.Second),
	}
	inChan <- observation.InputObservation{
		Rrname:         "baz",
		Rdata:          "1.2.3.4",
		Rrtype:         "A",
		Count:          1,
		SensorID:       "abc",
		TimestampStart: time.Now(),
		TimestampEnd:   time.Now().Add(1 * time.Second),
	}
	for v, _ := db.ObservationDB.TotalCount(); v < 3; v, _ = db.ObservationDB.TotalCount() {
		time.Sleep(10 * time.Millisecond)
	}
	close(inChan)

	// create test frontend on random free port
	gq = GraphQLFrontend{}
	listener, err := net.Listen("tcp", fmt.Sprintf(":0"))
	if err != nil {
		t.Fatal(err)
	}
	gq.RunWithListener(listener)
	t.Run("QueryOne", testGraphQLQueryOne)
	t.Run("QueryAnother", testGraphQLQueryAnother)
	t.Run("QueryTwo", testGraphQLQueryTwo)
	t.Run("QueryEmpty", testGraphQLQueryEmpty)
	t.Run("QueryFail", testGraphQLQueryFail)
	gq.Stop()
}
