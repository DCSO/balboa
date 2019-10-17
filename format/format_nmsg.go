package format

import (
	"balboa/observation"
	"github.com/gogo/protobuf/proto"
	"time"
)

// MakeNmsgInputObservations is a MakeObservationFunc that accepts input
// in the SIE NMSG format
func MakeNmsgInputObservations(inputNmsg []byte, sensorID string, out chan observation.InputObservation, stop chan bool) error {
	var nd NewDomain
	err := proto.Unmarshal(inputNmsg, &nd)
	if err != nil {
		return err
	}
	o := observation.InputObservation{
		Count: uint(nd.GetCount()),
		Rcode: "",
		// how is this a string ? this propably should be [][]byte since RDATA can contain multiple non printable elements!
		Rdata: string(nd.GetRdata()[0]),
		// why is this a string?
		Rrtype: string(nd.GetRrtype()),
		// ...
		Rrname:         string(nd.GetRrname()),
		SensorID:       "",
		TimestampEnd:   time.Unix(int64(nd.GetTimeLast()), 0),
		TimestampStart: time.Unix(int64(nd.GetTimeFirst()), 0),
	}
	out <- o
	return nil
}
