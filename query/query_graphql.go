// balboa
// Copyright (c) 2018, DCSO GmbH

package query

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/DCSO/balboa/db"
	"github.com/DCSO/balboa/observation"

	graphql "github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/errors"
	"github.com/graph-gophers/graphql-go/relay"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

const (
	txtSchema = `# A DNS resource record type.
	enum RRType {
	  A
	  A6
	  AAAA
	  AFSDB
	  ALIAS
	  APL
	  AXFR
	  CAA
	  CDNSKEY
	  CDS
	  CERT
	  CNAME
	  DHCID
	  DLV
	  DNAME
	  DNSKEY
	  DS
	  HINFO
	  HIP
	  IPSECKEY
	  IXFR
	  KEY
	  KX
	  LOC
	  MX
	  NAPTR
	  NS
	  NSEC
	  NSEC3
	  NSEC3PARAM
	  OPENPGPKEY
	  OPT
	  PTR
	  RRSIG
	  RP
	  SIG
	  SOA
	  SPF
	  SRV
	  SSHFP
	  TA
	  TKEY
	  TLSA
	  TSIG
	  TXT
	  URI
	}

	# A single observation, unique for the combination of sensor, rrname,
	# rdata and rrtype. Corresponds, roughly, to a pDNS COF item, but with
	# additional Aliases (linked via IP in A/AAAA records).
	type Entry {
		# The number of observed occurrences of this observation.
		count: Int!

		# The RRName seen in this observation.
		rrname: String!

		# The RRType seen in this observation.
		rrtype: RRType

		# The RData seen in this observation.
		rdata: String!

		# Time this observation was first seen, as Unix timestamp.
		time_first: Int!

		# Time this observation was first seen, as RFC 3339 formatted time.
		time_first_rfc3339: String!

		# Time this observation was last seen, as Unix timestamp.
		time_last: Int!

		# Time this observation was last seen, as RFC 3339 formatted time.
		time_last_rfc3339: String!

		# Some identifier describing the source of this observation.
		sensor_id: String

		# Entries referencing the same IP (for A/AAAA) observed on the same
		# sensor.
		aliases(limit: Int = 1000): [LeafEntry]
	}

	# A single observation, unique for the combination of sensor, rrname,
	# rdata and rrtype. Corresponds, roughly, to a pDNS COF item.
	type LeafEntry {
		# The number of observed occurrences of this observation.
		count: Int!

		# The RRName seen in this observation.
		rrname: String!

		# The RRType seen in this observation.
		rrtype: RRType

		# The RData seen in this observation.
		rdata: String!

		# Time this observation was first seen, as Unix timestamp.
		time_first: Int!

		# Time this observation was first seen, as RFC 3339 formatted time.
		time_first_rfc3339: String!

		# Time this observation was last seen, as Unix timestamp.
		time_last: Int!

		# Time this observation was last seen, as RFC 3339 formatted time.
		time_last_rfc3339: String!

		# Some identifier describing the source of this observation.
		sensor_id: String
	}

	input EntryInput {
		# The number of observed occurrences of this observation.
		count: Int!

		# The RRName seen in this observation.
		rrname: String!

		# The RRType seen in this observation.
		rrtype: RRType!

		# The RData seen in this observation.
		rdata: String!

		# Time this observation was first seen, as Unix timestamp.
		time_first: Int!

		# Time this observation was last seen, as Unix timestamp.
		time_last: Int!

		# Some identifier describing the source of this observation.
		sensor_id: String!
	}

	# Some runtime values describing the current state of the database.
	type Stats {
		# Total number of keys in the database.
		total_count: Int!

		# Number of concurrent goroutines in the server instance.
		num_goroutines: Int!
	}

	type Query {
		# Returns a set of observations satisfying the given query parameters.
		# Providing rdata, rrname, rrtype and/or sensor_id will restrict the
		# results to the set of observations that match all of the given
		# constraints.
		entries(rdata: String, rrname: String, rrtype: RRType, sensor_id: String, limit: Int = 1000): [Entry]

		# Returns some runtime values describing the current state of the database.
		stats(): Stats
	}

	#type Mutation {
	#	announceObservation(observation: EntryInput!): Entry!
	#}

	schema {
		query: Query
		#mutation: Mutation
	}`
)

// GraphQLFrontend represents a concurrent component that provides a GraphQL
// query interface for the database.
type GraphQLFrontend struct {
	Server    *http.Server
	IsRunning bool
}

// Resolver is just used to bundle top level methods.
type Resolver struct{}

// Entries returns a collection of Entry resolvers, given parameters such as
// Rdata, RRname, RRtype and sensor ID.
func (r *Resolver) Entries(args struct {
	Rdata    *string
	Rrname   *string
	Rrtype   *string
	SensorID *string
	Limit    int32
}) (*[]*EntryResolver, error) {
	startTime := time.Now()
	defer func() {
		var rdata, rrname, rrtype, sensorID string
		if args.Rdata != nil {
			rdata = *args.Rdata
		} else {
			rdata = ("nil")
		}
		if args.Rrname != nil {
			rrname = *args.Rrname
		} else {
			rrname = ("nil")
		}
		if args.Rrtype != nil {
			rrtype = *args.Rrtype
		} else {
			rrtype = ("nil")
		}
		if args.SensorID != nil {
			sensorID = *args.SensorID
		} else {
			sensorID = ("nil")
		}
		log.Debugf("finished query for (%s/%s/%s/%s) in %v", rdata, rrname, rrtype, sensorID, time.Since(startTime))
	}()

	l := make([]*EntryResolver, 0)
	if args.Rdata == nil && args.Rrname == nil {
		return nil, &errors.QueryError{
			Message: "at least one of the 'rdata' or 'rrname' parameters is required",
		}
	}
	results, err := db.ObservationDB.Search(args.Rdata, args.Rrname, args.Rrtype, args.SensorID, int(args.Limit))
	if err != nil {
		return nil, err
	}
	for _, r := range results {
		er := EntryResolver{
			entry: r,
		}
		l = append(l, &er)
	}
	return &l, nil
}

// AnnounceObservation is a mutation that adds a single new observation
// to the database.
//func (r *Resolver) AnnounceObservation(args struct {
//	Observation struct {
//		Count     int32
//		TimeFirst int32
//		TimeLast  int32
//		RRType    string
//		RRName    string
//		RData     string
//		SensorID  string
//	}
//}) *EntryResolver {
//	inObs := observation.InputObservation{
//		Count:          int(args.Observation.Count),
//		TimestampStart: time.Unix(int64(args.Observation.TimeFirst), 0),
//		TimestampEnd:   time.Unix(int64(args.Observation.TimeLast), 0),
//		Rrname:         args.Observation.RRName,
//		Rrtype:         args.Observation.RRType,
//		Rdata:          args.Observation.RData,
//		SensorID:       args.Observation.SensorID,
//	}
//	resObs := db.ObservationDB.AddObservation(inObs)
//	return &EntryResolver{
//		entry: resObs,
//	}
//}

// Stats returns a Stats resolver.
func (r *Resolver) Stats() (*StatsResolver, error) {
	return &StatsResolver{}, nil
}

// StatsResolver is a resolver for the Stats type.
type StatsResolver struct {
	//totalCount int32
}

// TotalCount returns the total number of keys in the database.
func (r *StatsResolver) TotalCount() int32 {
	val, err := db.ObservationDB.TotalCount()
	if err != nil {
		log.Error(err)
	}
	return int32(val)
}

// NumGoroutines returns the number of currently running goroutines
// in balboa.
func (r *StatsResolver) NumGoroutines() int32 {
	return int32(runtime.NumGoroutine())
}

// EntryResolver is a resolver for the Entry type.
type EntryResolver struct {
	entry observation.Observation
}

// ID returns the ID field of the corresponding entry.
func (r *EntryResolver) ID() graphql.ID {
	id, _ := uuid.NewV4()
	return graphql.ID(id.String())
}

// RRName returns the RRName field of the corresponding entry.
func (r *EntryResolver) RRName() string {
	return r.entry.RRName
}

// Rdata returns the Rdata field of the corresponding entry.
func (r *EntryResolver) Rdata() string {
	return r.entry.RData
}

// RRType returns the RRType field of the corresponding entry.
func (r *EntryResolver) RRType() *string {
	return &r.entry.RRType
}

// Count returns the Count field of the corresponding entry.
func (r *EntryResolver) Count() int32 {
	return int32(r.entry.Count)
}

// TimeFirst returns the first seen timestamp of the corresponding entry.
func (r *EntryResolver) TimeFirst() int32 {
	return int32(r.entry.FirstSeen.Unix())
}

// TimeFirstRFC3339 returns first seen time, as RFC 3339 string, of the corresponding entry.
func (r *EntryResolver) TimeFirstRFC3339() string {
	return r.entry.FirstSeen.Format(time.RFC3339)
}

// TimeLast returns the last seen timestamp of the corresponding entry.
func (r *EntryResolver) TimeLast() int32 {
	return int32(r.entry.LastSeen.Unix())
}

// TimeLastRFC3339 returns last seen time, as RFC 3339 string, of the corresponding entry.
func (r *EntryResolver) TimeLastRFC3339() string {
	return r.entry.LastSeen.Format(time.RFC3339)
}

// SensorID returns the sensor ID field of the corresponding entry.
func (r *EntryResolver) SensorID() *string {
	return &r.entry.SensorID
}

// Aliases returns resolvers for Entries with the same IPs in Rdata (for
// A/AAAA type entries).
func (r *EntryResolver) Aliases(args struct{ Limit int32 }) *[]*EntryResolver {
	l := make([]*EntryResolver, 0)
	if !(r.entry.RRType == "A" || r.entry.RRType == "AAAA") {
		return nil
	}
	results, err := db.ObservationDB.Search(&r.entry.RData, nil, nil, &r.entry.SensorID, int(args.Limit))
	if err != nil {
		return nil
	}
	for _, rs := range results {
		if rs.RRName != r.entry.RRName {
			er := EntryResolver{
				entry: rs,
			}
			l = append(l, &er)
		}
	}
	return &l
}

// Run starts this instance of a GraphQLFrontend in the background, accepting
// new requests on the configured port.
func (g *GraphQLFrontend) Run(port int) {
	schema := graphql.MustParseSchema(txtSchema, &Resolver{})
	g.Server = &http.Server{
		Addr:         fmt.Sprintf(":%v", port),
		Handler:      &relay.Handler{Schema: schema},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Infof("serving GraphQL on port %v", port)
	go func() {
		err := g.Server.ListenAndServe()
		if err != nil {
			log.Info(err)
		}
		g.IsRunning = true
	}()
}

// Stop causes this instance of a GraphQLFrontend to cease accepting requests.
func (g *GraphQLFrontend) Stop() {
	if g.IsRunning {
		g.Server.Shutdown(context.TODO())
	}
}
