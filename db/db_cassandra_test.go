package db

import (
	"testing"
	"time"

	mock "github.com/maraino/go-mock"
	"github.com/willfaught/gockle"
	"github.com/DCSO/balboa/observation"
)

func TestCassandraSimpleConsume(t *testing.T) {
	db, m := makeCassandraDBMock()
	defer db.Shutdown()

	inChan := make(chan observation.InputObservation)
	stopChan := make(chan bool)

	go db.ConsumeFeed(inChan)

	timeStart := time.Now()
	timeEnd := time.Now()

	cnt := 0
	var iteratorMock = &gockle.IteratorMock{}
	iteratorMock.When("ScanMap", mock.Any).Call(func(m map[string]interface{}) bool {
		m["rrname"] = "foo.bar"
		m["rrtype"] = "A"
		m["rdata"] = "12.34.56.78"
		m["count"] = 2
		m["sensor_id"] = "deadcafe"
		cnt++
		return cnt < 4
	})
	iteratorMock.When("Close").Return(nil)

	cnt2 := 0
	var iteratorMockRdata = &gockle.IteratorMock{}
	iteratorMockRdata.When("ScanMap", mock.Any).Call(func(m map[string]interface{}) bool {
		m["rrname"] = "foo.bar"
		m["rrtype"] = "A"
		m["rdata"] = "12.34.56.79"
		m["count"] = 96
		m["sensor_id"] = "deadcafe"
		cnt2++
		return cnt2 < 2
	})
	iteratorMockRdata.When("Close").Return(nil)

	cnt3 := 0
	var iteratorMockCnt = &gockle.IteratorMock{}
	iteratorMockCnt.When("ScanMap", mock.Any).Call(func(m map[string]interface{}) bool {
		m["count"] = int64(3)
		cnt3++
		return cnt3 < 2
	})
	iteratorMockCnt.When("Close").Return(nil)

	cnt4 := 0
	var iteratorMockFirstItem = &gockle.IteratorMock{}
	iteratorMockFirstItem.When("ScanMap", mock.Any).Call(func(m map[string]interface{}) bool {
		m["rrname"] = "foo.bar"
		cnt4++
		return cnt4 < 2
	})
	iteratorMockFirstItem.When("Close").Return(nil)

	m.When("ScanIterator", "SELECT * FROM observations_by_rrname WHERE rrname = ?", mock.Any).Return(iteratorMock)
	m.When("Exec", "UPDATE observations_by_rdata SET first_seen = ?, last_seen = ? where rdata = ? and rrname = ?  and rrtype = ? and sensor_id = ?;", mock.Any).Return()
	m.When("Exec", "UPDATE observations_by_rdata SET last_seen = ? where rdata = ? and rrname = ?  and rrtype = ? and sensor_id = ?;", mock.Any).Return()
	m.When("Exec", "UPDATE observations_by_rrname SET first_seen = ?, last_seen = ? where rrname = ? and rdata = ? and rrtype = ? and sensor_id = ?;", mock.Any).Return()
	m.When("Exec", "UPDATE observations_by_rrname SET last_seen = ? where rrname = ? and rdata = ? and rrtype = ? and sensor_id = ?;", mock.Any).Return()
	m.When("ScanIterator", "SELECT * FROM observations_by_rdata WHERE rdata = ?", []interface{}{"12.34.56.79"}).Return(iteratorMockRdata)
	m.When("Exec", "UPDATE observations_counts SET count = count + ? where rdata = ? and rrname = ? and rrtype = ? and sensor_id = ?;", mock.Any).Return()
	m.When("ScanIterator", "SELECT rrname FROM observations_by_rrname WHERE rrname = ? and rdata = ? and rrtype = ? and sensor_id = ?;", mock.Any).Return(iteratorMockFirstItem)
	m.When("ScanIterator", "SELECT count FROM observations_counts WHERE rrname = ? AND rdata = ? AND rrtype = ? AND sensor_id = ?", mock.Any).Return(iteratorMockCnt)
	m.When("Close").Return()

	o := observation.InputObservation{
		Rrname:         "foo.bar",
		Rrtype:         "A",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.78",
		TimestampEnd:   timeStart,
		TimestampStart: timeEnd,
		Count:          1,
	}
	inChan <- o

	o = observation.InputObservation{
		Rrname:         "foo.bar",
		Rrtype:         "MX",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.77",
		TimestampEnd:   timeStart,
		TimestampStart: timeEnd,
		Count:          1,
	}
	inChan <- o

	for i := 0; i < 500; i++ {
		o = observation.InputObservation{
			Rrname:         "foo.bar",
			Rrtype:         "NS",
			SensorID:       "deadcafe",
			Rdata:          "12.34.56.79",
			TimestampEnd:   timeStart,
			TimestampStart: timeEnd,
			Count:          2,
		}
		inChan <- o
	}
	close(stopChan)

	str := "foo.bar"
	obs, err := db.Search(nil, &str, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 3 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}

	str = "12.34.56.79"
	obs, err = db.Search(&str, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 1 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}

	str = "12.34.56.79"
	obs, err = db.Search(&str, nil, nil, &str)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 0 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}

}
