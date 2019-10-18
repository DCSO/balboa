package format

import (
	"balboa/observation"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
)

func parseDomainString(rdata []byte) (domain string) {
	domain = ""
	var i uint = 0
	for i < uint(len(rdata)) && rdata[i] != 0 {
		di := uint(rdata[i])
		if (i+1 > uint(len(rdata))) || (i+1+di > uint(len(rdata))) {
			return "" // parsing error
		}
		domain = domain + string(rdata[i+1:i+1+di]) + "."
		i += 1 + di
	}
	if len(domain) < 2 {
		return "" // parsing error
	}
	domain = domain[0 : len(domain)-1]
	return domain
}

const (
	RRT_A     = 1
	RRT_AAAA  = 28
	RRT_CNAME = 5
	RRT_MX    = 15
	RRT_NS    = 2
	RRT_PTR   = 12
	RRT_SOA   = 6
	RRT_SRV   = 33
	RRT_TXT   = 16
)

func parseRRType(rrtype int) string {
	switch rrtype {
	case RRT_A:
		return "A"
	case RRT_AAAA:
		return "AAAA"
	case RRT_CNAME:
		return "CNAME"
	case RRT_MX:
		return "MX"
	case RRT_NS:
		return "NS"
	case RRT_PTR:
		return "PTR"
	case RRT_SOA:
		return "SOA"
	case RRT_SRV:
		return "SRV"
	case RRT_TXT:
		return "TXT"
	default:
		return fmt.Sprintf("%d", rrtype)
	}
}

func parseRData(rdata []byte, rrtype int) string {
	switch rrtype {
	case RRT_A:
		if len(rdata) != 4 {
			// expecting exactly 4 byte
			return "" // corrupt record
		}
		return fmt.Sprintf("%d.%d.%d.%d", rdata[0], rdata[1], rdata[2], rdata[3])
	case RRT_NS, RRT_CNAME:
		return parseDomainString(rdata)
	default:
		return string(rdata)
	}
}

// MakeNmsgInputObservations is a MakeObservationFunc that accepts input
// in the SIE NMSG format
func MakeNmsgInputObservations(inputNmsg []byte, sensorID string, out chan observation.InputObservation, stop chan bool) error {
	var nd NewDomain
	err := proto.Unmarshal(inputNmsg, &nd)
	if err != nil {
		return err
	}

	o := observation.InputObservation{
		Count: 1,
		// TODO: Rdata should be a []string or even better a [][]byte field
		Rdata:          parseRData(nd.GetRdata()[0], int(nd.GetRrtype())),
		Rrtype:         parseRRType(int(nd.GetRrtype())),
		Rrname:         parseDomainString(nd.GetRrname()),
		SensorID:       sensorID,
		TimestampEnd:   time.Unix(int64(nd.GetTimeSeen()), 0),
		TimestampStart: time.Unix(int64(nd.GetTimeSeen()), 0),
	}
	out <- o
	return nil
}
