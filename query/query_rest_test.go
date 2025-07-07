// balboa
// Copyright (c) 2020, 2025, DCSO GmbH

package query

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/DCSO/balboa/db"
	"github.com/DCSO/balboa/observation"
)

func makeRESTQuery(t *testing.T, keyword, addr string, limit, offset *uint64, sensorids []string, rrtype *string) ([]byte, error) {
	url := "http://" + addr + fmt.Sprintf("/pdns/query/%s", keyword)
	if limit != nil {
		url += fmt.Sprintf("?limit=%d", *limit)
	}
	if offset != nil {
		if !strings.Contains(url, "?") {
			url += "?"
		} else {
			url += "&"
		}
		url += fmt.Sprintf("next=%d", *offset)
	}
	if rrtype != nil {
		if !strings.Contains(url, "?") {
			url += "?"
		} else {
			url += "&"
		}
		url += fmt.Sprintf("rrtype=%s", *rrtype)
	}
	if len(sensorids) > 0 {
		if !strings.Contains(url, "?") {
			url += "?"
		} else {
			url += "&"
		}
		url += "sensorids=" + strings.Join(sensorids, ",")
	}
	t.Logf("making REST query to %s", url)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func parseRESTResult(t *testing.T, body []byte) []observation.Observation {
	res := make([]observation.Observation, 0)
	for _, line := range strings.Split(string(body), "\n") {
		if len(line) == 0 {
			continue
		}
		var o observation.Observation
		err := json.Unmarshal([]byte(line), &o)
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, o)
	}
	return res
}

var rq RESTFrontend

func testRESTQueryOne(t *testing.T) {
	// run queries: foo
	body, err := makeRESTQuery(t, "foo", rq.GetAddr(), nil, nil, []string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := parseRESTResult(t, body)
	if len(res) != 1 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	if res[0].RRName != "foo" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
}

func testRESTQueryAnother(t *testing.T) {
	// run queries: foo
	body, err := makeRESTQuery(t, "bar", rq.GetAddr(), nil, nil, []string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := parseRESTResult(t, body)
	if len(res) != 1 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	if res[0].RRName != "bar" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
}

func testRESTQueryTwo(t *testing.T) {
	// run queries: bar
	body, err := makeRESTQuery(t, "1.2.3.4", rq.GetAddr(), nil, nil, []string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := parseRESTResult(t, body)
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

func testRESTQuerySensorID(t *testing.T) {
	body, err := makeRESTQuery(t, "1.2.3.4", rq.GetAddr(), nil, nil, []string{"zbc"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := parseRESTResult(t, body)
	if len(res) != 1 {
		t.Fatalf("unexpected length of result: %d", len(res))

	}
	if res[0].RRName != "baz" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
}

func testRESTQueryRRType(t *testing.T) {
	rrtype := "MX"
	body, err := makeRESTQuery(t, "1.2.3.4", rq.GetAddr(), nil, nil, []string{}, &rrtype)
	if err != nil {
		t.Fatal(err)
	}
	res := parseRESTResult(t, body)
	if len(res) != 1 {
		t.Fatalf("unexpected length of result: %d", len(res))

	}
	if res[0].RRName != "baz" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
}

func testRESTQueryPaginate(t *testing.T) {
	// regular query
	offset := uint64(0)
	limit := uint64(2)
	body, err := makeRESTQuery(t, "1.2.3.4", rq.GetAddr(), &limit, &offset, []string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := parseRESTResult(t, body)
	if len(res) != 2 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	if res[0].RRName != "foo" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
	if res[1].RRName != "baz" {
		t.Fatalf("unexpected result rrname: %s", res[1].RRName)
	}
	// empty result
	offset = uint64(0)
	limit = uint64(0)
	body, err = makeRESTQuery(t, "1.2.3.4", rq.GetAddr(), &limit, &offset, []string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res = parseRESTResult(t, body)
	if len(res) != 0 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	// out of bounds
	offset = uint64(4)
	limit = uint64(5)
	body, err = makeRESTQuery(t, "1.2.3.4", rq.GetAddr(), &limit, &offset, []string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res = parseRESTResult(t, body)
	if len(res) != 0 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	// limit 1
	offset = uint64(0)
	limit = uint64(1)
	body, err = makeRESTQuery(t, "1.2.3.4", rq.GetAddr(), &limit, &offset, []string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res = parseRESTResult(t, body)
	if len(res) != 1 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	if res[0].RRName != "foo" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
	// offset 1
	offset = uint64(1)
	limit = uint64(4)
	body, err = makeRESTQuery(t, "1.2.3.4", rq.GetAddr(), &limit, &offset, []string{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res = parseRESTResult(t, body)
	if len(res) != 1 {
		t.Fatalf("unexpected length of result: %d", len(res))
	}
	if res[0].RRName != "baz" {
		t.Fatalf("unexpected result rrname: %s", res[0].RRName)
	}
}

func testRESTQueryEmpty(t *testing.T) {
	// run queries: valid query path but nonexisting keyword
	_, err := makeRESTQuery(t, "nonexist", rq.GetAddr(), nil, nil, []string{}, nil)
	if err == nil {
		t.Fatal("missing error")
	}
	if err.Error() != "status 404" {
		t.Fatalf("wrong error message: %s", err.Error())
	}
}

func testRESTQueryFail(t *testing.T) {
	// run queries: failed query due to invalid path
	resp, err := http.Get("http://" + rq.GetAddr() + fmt.Sprintf("/blurb/foo"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
}

func TestQueryREST(t *testing.T) {
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
		Rrtype:         "MX",
		Count:          1,
		SensorID:       "zbc",
		TimestampStart: time.Now(),
		TimestampEnd:   time.Now().Add(1 * time.Second),
	}
	for v, _ := db.ObservationDB.TotalCount(); v < 3; v, _ = db.ObservationDB.TotalCount() {
		time.Sleep(10 * time.Millisecond)
	}
	close(inChan)

	// create test frontend on random free port
	rq = RESTFrontend{}
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	rq.RunWithListener(listener)
	t.Run("QueryOne", testRESTQueryOne)
	t.Run("QueryAnother", testRESTQueryAnother)
	t.Run("QueryTwo", testRESTQueryTwo)
	t.Run("QueryEmpty", testRESTQueryEmpty)
	t.Run("QueryFail", testRESTQueryFail)
	t.Run("QueryPaginate", testRESTQueryPaginate)
	t.Run("QuerySensorID", testRESTQuerySensorID)
	t.Run("QueryRRType", testRESTQueryRRType)
	rq.Stop()
}
