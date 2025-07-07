// balboa
// Copyright (c) 2020, 2025, DCSO GmbH

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/DCSO/balboa/db"
	"github.com/DCSO/balboa/observation"
	log "github.com/sirupsen/logrus"
)

const (
	// defaultLimit specifies the default limit to use if not given as a GET
	// parameter.
	defaultLimit = 1000
	// defaultOffset specifies the default offset to use if not given as a GET
	// parameter.
	defaultOffset = 0
	// queryPathPrefix specifies the HTTP GET path prefix before the actual
	// search subject string.
	queryPathPrefix = "/pdns/query/"
)

// RESTFrontend represents a concurrent component that provides a HTTP-based
// query interface for the database. It is meant to be compatible with CIRCL's
// interface as described in https://www.circl.lu/services/passive-dns/.
type RESTFrontend struct {
	Server    *http.Server
	Listener  net.Listener
	IsRunning bool
}

type restHandler struct{}

func limitSlice(slice []observation.Observation, limit, offset uint64) []observation.Observation {
	if uint64(len(slice)) >= offset {
		start := offset
		end := min(uint64(len(slice)), offset+limit)
		return slice[start:end]
	}
	return nil
}

func parseParams(r *http.Request) (uint64, uint64, *string, []*string) {
	var limit uint64 = defaultLimit
	var offset uint64 = defaultOffset
	var rrtype *string = nil
	var sensorIDs []*string = []*string{}
	limParams, ok := r.URL.Query()["limit"]
	if ok && len(limParams[0]) > 0 {
		limVal, err := strconv.ParseUint(limParams[0], 10, 32)
		if err == nil {
			limit = limVal
		}
	}
	offParams, ok := r.URL.Query()["next"]
	if ok && len(offParams[0]) > 0 {
		offVal, err := strconv.ParseUint(offParams[0], 10, 32)
		if err == nil {
			offset = offVal
		}
	}
	rrtypeParams, ok := r.URL.Query()["rrtype"]
	if ok && len(rrtypeParams[0]) > 0 {
		rrtype = &rrtypeParams[0]
	}
	sensorIDParams, ok := r.URL.Query()["sensorids"]
	if ok && len(sensorIDParams[0]) > 0 {
		ids := strings.Split(sensorIDParams[0], ",")
		// deduplicate sensor IDs
		slices.Sort(ids)
		for _, val := range slices.Compact(ids) {
			if len(val) > 0 {
				//] skip empty sensor IDs
				sensorIDs = append(sensorIDs, &val)
			}
		}
	}
	return limit, offset, rrtype, sensorIDs
}

func (rh *restHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, queryPathPrefix) {
		w.WriteHeader(404)
		return
	}
	subject := strings.Replace(r.URL.Path, queryPathPrefix, "", -1)
	allres := make([]observation.Observation, 0)
	limit, offset, rrtype, sensorIDs := parseParams(r)

	if len(sensorIDs) == 0 {
		results, err := db.ObservationDB.Search(nil, &subject, rrtype, nil, int(limit+offset))
		if err != nil {
			log.Error(err)
			return
		}
		allres = append(allres, results...)
		results, err = db.ObservationDB.Search(&subject, nil, rrtype, nil, int(limit+offset))
		if err != nil {
			log.Error(err)
			return
		}
		allres = append(allres, results...)
	} else {
		for _, sensorID := range sensorIDs {
			results, err := db.ObservationDB.Search(nil, &subject, rrtype, sensorID, int(limit+offset))
			if err != nil {
				log.Error(err)
				return
			}
			allres = append(allres, results...)
			results, err = db.ObservationDB.Search(&subject, nil, rrtype, sensorID, int(limit+offset))
			if err != nil {
				log.Error(err)
				return
			}
			allres = append(allres, results...)
			if len(allres) >= int(limit+offset) {
				// if we have enough results, stop searching
				// to avoid unnecessary load on the database
				break
			}
		}
	}
	if len(allres) == 0 {
		w.WriteHeader(404)
		return
	}
	allres = limitSlice(allres, limit, offset)
	for _, rs := range allres {
		json, err := json.Marshal(&rs)
		if err == nil {
			w.Write(json)
			w.Write([]byte("\n"))
		}
	}
}

// RunWithListener starts this instance of a RESTFrontend in the background,
// accepting new requests using the given net.Listener.
func (g *RESTFrontend) RunWithListener(l net.Listener) {
	handler := &restHandler{}
	g.Server = &http.Server{
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	g.Listener = l
	log.Infof("serving CIRCL-like REST on %v", l.Addr().String())
	go func() {
		err := g.Server.Serve(l)
		if err != nil {
			log.Info(err)
		}
		g.IsRunning = true
	}()
}

// GetAddr returns the address for this frontend's net.Listener.
func (g *RESTFrontend) GetAddr() string {
	return g.Listener.Addr().String()
}

// Run starts this instance of a RESTFrontend in the background, accepting
// new requests on the configured port.
func (g *RESTFrontend) Run(port int) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal(err)
	}
	g.Listener = listener
	g.RunWithListener(listener)
}

// Stop causes this instance of a RESTFrontend to cease accepting requests.
func (g *RESTFrontend) Stop() {
	if g.IsRunning {
		g.Server.Shutdown(context.TODO())
		g.Listener.Close()
	}
}
