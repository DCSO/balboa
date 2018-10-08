package db

import (
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/DCSO/balboa/observation"
)

func createMysqlTmpDB(t *testing.T) (*MySQLDB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	return MakeMySQLDB(db), mock
}

func TestMySQLSimpleConsume(t *testing.T) {
	db, mock := createMysqlTmpDB(t)
	defer db.Shutdown()

	inChan := make(chan observation.InputObservation)
	stopChan := make(chan bool)

	go db.ConsumeFeed(inChan, stopChan)

	timeStart := time.Now()
	timeEnd := time.Now()
	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO observations")
	mock.ExpectExec("INSERT INTO observations").WithArgs("deadcafe", "x.foo.bar", "12.34.56.78", nil, "A", 1, timeStart, timeEnd).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO observations").WithArgs("deadcafe", "y.foo.bar", "12.34.56.77", nil, "MX", 1, timeStart, timeEnd).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 48; i++ {
		mock.ExpectExec("INSERT INTO observations").WithArgs("deadcafe", "foo.bar", "12.34.56.79", nil, "NS", 2, timeStart, timeEnd).WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectPrepare("SELECT (.+) FROM observations")
	retRows := sqlmock.NewRows(
		[]string{
			"sensor_id",
			"rrname",
			"rdata",
			"rrtype",
			"count",
			"time_first",
			"time_last",
		},
	)
	retRows.AddRow("12.34.56.78", "A", "x.foo.bar", "deadcafe", 1, timeStart, timeEnd)
	retRows.AddRow("12.34.56.78", "NX", "y.foo.bar", "deadcafe", 1, timeStart, timeEnd)
	retRows.AddRow("12.34.56.78", "NS", "foo.bar", "deadcafe", 96, timeStart, timeEnd)
	mock.ExpectQuery("SELECT (.+) FROM observations").WillReturnRows(retRows)
	mock.ExpectPrepare("SELECT (.+) FROM observations")
	retRows = sqlmock.NewRows(
		[]string{
			"sensor_id",
			"rrname",
			"rdata",
			"rrtype",
			"count",
			"time_first",
			"time_last",
		},
	)
	retRows.AddRow("12.34.56.78", "NS", "foo.bar", "deadcafe", 96, timeStart, timeEnd)
	mock.ExpectQuery("SELECT (.+) FROM observations").WillReturnRows(retRows)
	mock.ExpectPrepare("SELECT (.+) FROM observations")
	retRows = sqlmock.NewRows(
		[]string{
			"sensor_id",
			"rrname",
			"rdata",
			"rrtype",
			"count",
			"time_first",
			"time_last",
		},
	)
	mock.ExpectQuery("SELECT (.+) FROM observations").WillReturnRows(retRows)
	mock.ExpectQuery("SELECT (.+) FROM observations").WillReturnRows(sqlmock.NewRows(
		[]string{
			"count",
		}).AddRow(3))

	inChan <- observation.InputObservation{
		Rrname:         "x.foo.bar",
		Rrtype:         "A",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.78",
		TimestampEnd:   timeEnd,
		TimestampStart: timeStart,
		Count:          1,
	}
	inChan <- observation.InputObservation{
		Rrname:         "y.foo.bar",
		Rrtype:         "MX",
		SensorID:       "deadcafe",
		Rdata:          "12.34.56.77",
		TimestampEnd:   timeEnd,
		TimestampStart: timeStart,
		Count:          1,
	}
	for i := 0; i < 50; i++ {
		inChan <- observation.InputObservation{
			Rrname:         "foo.bar",
			Rrtype:         "NS",
			SensorID:       "deadcafe",
			Rdata:          "12.34.56.79",
			TimestampEnd:   timeEnd,
			TimestampStart: timeStart,
			Count:          2,
		}
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

	tc, err := db.TotalCount()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 3 {
		t.Fatalf("wrong number of results: %d", len(obs))
	}

}
