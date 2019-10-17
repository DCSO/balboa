// balboa
// Copyright (c) 2018, DCSO GmbH

package feeder

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"balboa/format"
	"balboa/observation"

	log "github.com/sirupsen/logrus"
)

// HTTPFeeder is a Feeder implementation that accepts HTTP requests to obtain
// observations.
type HTTPFeeder struct {
	StopChan            chan bool
	StoppedChan         chan bool
	IsRunning           bool
	Port                int
	Host                string
	MakeObservationFunc format.MakeObservationFunc
	Server              *http.Server
	OutChan             chan observation.InputObservation
}

// MakeHTTPFeeder creates a new HTTPFeeder listening on a specific address
// and port.
func MakeHTTPFeeder(host string, port int) *HTTPFeeder {
	return &HTTPFeeder{
		IsRunning:           false,
		StopChan:            make(chan bool),
		Port:                port,
		Host:                host,
		MakeObservationFunc: format.MakeFeverAggregateInputObservations,
	}
}

// SetInputDecoder states that the given MakeObservationFunc should be used to
// parse and decode data delivered to this feeder.
func (f *HTTPFeeder) SetInputDecoder(fn format.MakeObservationFunc) {
	f.MakeObservationFunc = fn
}

func (f *HTTPFeeder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sensorID := r.Header.Get("X-Sensor-ID")
	if r.Method == http.MethodPost {
		body, err := ioutil.ReadAll(r.Body)
		log.Infof("got %d bytes via HTTP", len(body))
		if err != nil {
			log.Warn(err)
			return
		}
		f.MakeObservationFunc(body, sensorID, f.OutChan, f.StopChan)
	}
	w.WriteHeader(200)
}

// Run starts the feeder.
func (f *HTTPFeeder) Run(out chan observation.InputObservation) error {
	f.OutChan = out
	f.Server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", f.Host, f.Port),
		Handler: f,
	}
	log.Infof("accepting submissions on port %v", f.Port)
	go func() {
		err := f.Server.ListenAndServe()
		if err != nil {
			log.Info(err)
		}
	}()
	f.IsRunning = true
	return nil
}

// Stop causes the feeder to stop accepting requests and close all
// associated channels, including the passed notification channel.
func (f *HTTPFeeder) Stop(stopChan chan bool) {
	if f.IsRunning {
		f.Server.Shutdown(context.TODO())
	}
	close(stopChan)
}
