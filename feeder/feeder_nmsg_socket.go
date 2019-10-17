package feeder

import (
	"balboa/format"
	"balboa/observation"
	"bufio"
	"net"
	"time"

	"github.com/farsightsec/go-nmsg"
)

// SocketFeeder is a Feeder implementation that reads NMSG data from a network UDP socket.
type NmsgSocketFeeder struct {
	ObsChan             chan observation.InputObservation
	Verbose             bool
	Running             bool
	InputListener       *net.UDPConn
	NmsgInput           nmsg.Input
	MakeObservationFunc format.MakeObservationFunc
	Mtu                 int
	StopChan            chan bool
	StoppedChan         chan bool
}

// MakeNmsgSocketFeeder returns a new NmsgSocketFeeder reading from the specified UDP
// network socket and writing parsed events to outChan. If no such socket could be
// created for listening, the error returned is set accordingly
func MakeNmsgSocketFeeder(bindAddrPort string, mtu int) (n *NmsgSocketFeeder, err error) {
	n = &NmsgSocketFeeder{
		Verbose:  false,
		StopChan: make(chan bool),
	}
	n.Mtu = mtu
	sAddr, err := net.ResolveUDPAddr("udp", bindAddrPort)
	if err != nil {
		return nil, err
	}
	n.InputListener, err = net.ListenUDP("udp", sAddr)
	if err != nil {
		return nil, err
	}
	return n, err
}

func (n *NmsgSocketFeeder) handleServerConnection() {
	reader := bufio.NewReader(n.InputListener)
	input := nmsg.NewInput(reader, n.Mtu)
	for {
		_ = n.InputListener.SetReadDeadline(time.Now().Add(5 * time.Second))
		select {
		case <-n.StopChan:
			_ = n.InputListener.Close()
			close(n.StoppedChan)
			return
		default:
			payload, err := input.Recv()
			if err != nil {
				if nmsg.IsDataError(err) {
					continue
				}
				break
			}
			_ = n.MakeObservationFunc(payload.GetPayload(), "[unknown]", n.ObsChan, n.StopChan)
		}
	}
}

func (n *NmsgSocketFeeder) Run(out chan observation.InputObservation) error {
	if !n.Running {
		n.ObsChan = out
		n.Running = true
		n.StopChan = make(chan bool)
		go n.handleServerConnection()
	}
	return nil
}

func (n *NmsgSocketFeeder) SetInputDecoder(fn format.MakeObservationFunc) {
	n.MakeObservationFunc = fn
}

func (n *NmsgSocketFeeder) Stop(stoppedChan chan bool) {
	if n.Running {
		n.StoppedChan = stoppedChan
		close(n.StopChan)
		n.Running = false
	}
}
