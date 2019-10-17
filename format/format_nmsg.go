package format

import (
	"balboa/observation"
	log "github.com/sirupsen/logrus"
)

// MakeNmsgInputObservations is a MakeObservationFunc that accepts input
// in the SIE NMSG format
func MakeNmsgInputObservations(inputNmsg []byte, sensorID string, out chan observation.InputObservation, stop chan bool) error {
	log.Printf("%v", inputNmsg)
	return nil
}
