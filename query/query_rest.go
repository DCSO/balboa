// balboa
// Copyright (c) 2020, DCSO GmbH

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	// queryPathPrefix specifies the HTTP GET path prefix before the actual
	// search subject string.
	queryPathPrefix = "/pdns/query/"
)

// RESTFrontend represents a concurrent component that provides a HTTP-based
// query interface for the database. It is meant to be compatible with CIRCL's
// interface as described in https://www.circl.lu/services/passive-dns/.
type RESTFrontend struct {
	Server    *http.Server
	IsRunning bool
}

type restHandler struct{}

func (rh *restHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, queryPathPrefix) {
		w.WriteHeader(404)
		return
	}
	subject := strings.Replace(r.URL.Path, queryPathPrefix, "", -1)
	allres := make([]observation.Observation, 0)

	var limit int64 = defaultLimit
	limParams, ok := r.URL.Query()["limit"]
	if ok && len(limParams[0]) == 1 {
		limVal, err := strconv.ParseInt(limParams[0], 10, 32)
		if err == nil {
			limit = limVal
		}
	}

	results, err := db.ObservationDB.Search(nil, &subject, nil, nil, int(limit))
	if err != nil {
		log.Error(err)
		return
	}
	allres = append(allres, results...)
	results, err = db.ObservationDB.Search(&subject, nil, nil, nil, int(limit))
	if err != nil {
		log.Error(err)
		return
	}
	allres = append(allres, results...)

	if len(allres) == 0 {
		w.WriteHeader(404)
		return
	}
	for _, rs := range allres {
		json, err := json.Marshal(&rs)
		if err == nil {
			w.Write(json)
			w.Write([]byte("\n"))
		}
	}
}

// Run starts this instance of a RESTFrontend in the background, accepting
// new requests on the configured port.
func (g *RESTFrontend) Run(port int) {
	handler := &restHandler{}
	g.Server = &http.Server{
		Addr:         fmt.Sprintf(":%v", port),
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Infof("serving CIRCL-like REST on port %v", port)
	go func() {
		err := g.Server.ListenAndServe()
		if err != nil {
			log.Info(err)
		}
		g.IsRunning = true
	}()
}

// Stop causes this instance of a RESTFrontend to cease accepting requests.
func (g *RESTFrontend) Stop() {
	if g.IsRunning {
		g.Server.Shutdown(context.TODO())
	}
}
