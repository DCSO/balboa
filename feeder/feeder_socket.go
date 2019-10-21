// balboa
// Copyright (c) 2018, DCSO GmbH

package feeder

import (
	"bufio"
	"net"
	"time"

	"github.com/DCSO/balboa/format"
	"github.com/DCSO/balboa/observation"

	log "github.com/sirupsen/logrus"
)

// SocketFeeder is a Feeder implementation that reds data from a UNIX socket.
type SocketFeeder struct {
	ObsChan             chan observation.InputObservation
	Verbose             bool
	Running             bool
	InputListener       net.Listener
	MakeObservationFunc format.MakeObservationFunc
	StopChan            chan bool
	StoppedChan         chan bool
}

func (sf *SocketFeeder) handleServerConnection() {
	for {
		select {
		case <-sf.StopChan:
			sf.InputListener.Close()
			close(sf.StoppedChan)
			return
		default:
			sf.InputListener.(*net.UnixListener).SetDeadline(time.Now().Add(1e9))
			c, err := sf.InputListener.Accept()
			if nil != err {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				log.Info(err)
			}

			scanner := bufio.NewScanner(c)
			buf := make([]byte, 0, 32*1024*1024)
			scanner.Buffer(buf, 32*1024*1024)
			for {
				for scanner.Scan() {
					select {
					case <-sf.StopChan:
						sf.InputListener.Close()
						close(sf.StoppedChan)
						return
					default:
						json := scanner.Bytes()
						sf.MakeObservationFunc(json, "[unknown]", sf.ObsChan, sf.StopChan)
					}
				}
				errRead := scanner.Err()
				if errRead == nil {
					break
				} else if errRead == bufio.ErrTooLong {
					log.Warn(errRead)
					scanner = bufio.NewScanner(c)
					scanner.Buffer(buf, 2*cap(buf))
				} else {
					log.Warn(errRead)
				}
			}
		}
	}
}

// MakeSocketFeeder returns a new SocketFeeder reading from the Unix socket
// inputSocket and writing parsed events to outChan. If no such socket could be
// created for listening, the error returned is set accordingly.
func MakeSocketFeeder(inputSocket string) (*SocketFeeder, error) {
	var err error
	si := &SocketFeeder{
		Verbose:  false,
		StopChan: make(chan bool),
	}
	si.InputListener, err = net.Listen("unix", inputSocket)
	if err != nil {
		return nil, err
	}
	return si, err
}

// SetInputDecoder states that the given MakeObservationFunc should be used to
// parse and decode data delivered to this feeder.
func (sf *SocketFeeder) SetInputDecoder(fn format.MakeObservationFunc) {
	sf.MakeObservationFunc = fn
}

// Run starts the feeder.
func (sf *SocketFeeder) Run(out chan observation.InputObservation) error {
	if !sf.Running {
		sf.ObsChan = out
		sf.Running = true
		sf.StopChan = make(chan bool)
		go sf.handleServerConnection()
	}
	return nil
}

// Stop causes the SocketFeeder to stop reading from the socket and close all
// associated channels, including the passed notification channel.
func (sf *SocketFeeder) Stop(stoppedChan chan bool) {
	if sf.Running {
		sf.StoppedChan = stoppedChan
		close(sf.StopChan)
		sf.Running = false
	}
}
