package feeder

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/DCSO/balboa/format"
	"github.com/DCSO/balboa/observation"
	"github.com/farsightsec/go-nmsg"
	"github.com/gogo/protobuf/proto"
)

// makeNmsgPayload wraps a NewDomain message for the given name and A record
// into an NMSG payload of the given vendor.
func makeNmsgPayload(t *testing.T, vid uint32, rrname string, rdata []byte) *nmsg.NmsgPayload {
	t.Helper()
	wireName := make([]byte, 0)
	for _, label := range []string{rrname, "com"} {
		wireName = append(wireName, byte(len(label)))
		wireName = append(wireName, label...)
	}
	wireName = append(wireName, 0)
	rrtype := uint32(format.RRT_A)
	if len(rdata) != 4 {
		rrtype = format.RRT_TXT
	}
	timeSeen := uint32(1500000000)
	b, err := proto.Marshal(&format.NewDomain{
		Rrname:   wireName,
		Rrtype:   &rrtype,
		Rdata:    [][]byte{rdata},
		TimeSeen: &timeSeen,
	})
	if err != nil {
		t.Fatal(err)
	}
	msgtype := uint32(5)
	sec, nsec := int64(timeSeen), uint32(0)
	return &nmsg.NmsgPayload{
		Vid:      &vid,
		Msgtype:  &msgtype,
		TimeSec:  &sec,
		TimeNsec: &nsec,
		Payload:  b,
	}
}

// startNmsgFeeder runs a NmsgSocketFeeder on a random local port and returns
// it together with its observation channel and a connection to send to it.
func startNmsgFeeder(t *testing.T) (*NmsgSocketFeeder, chan observation.InputObservation, *net.UDPConn) {
	t.Helper()
	f, err := MakeNmsgSocketFeeder("127.0.0.1:0", 1480)
	if err != nil {
		t.Fatal(err)
	}
	f.SetInputDecoder(format.MakeNmsgInputObservations)
	out := make(chan observation.InputObservation, 100)
	if err := f.Run(out); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, f.InputListener.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		stopped := make(chan bool)
		f.Stop(stopped)
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Error("feeder did not stop")
		}
	})
	return f, out, conn
}

// sendNmsg writes the payloads to conn, one container (or fragment) per
// datagram.
func sendNmsg(t *testing.T, conn *net.UDPConn, compress bool, payloads ...*nmsg.NmsgPayload) {
	t.Helper()
	output := nmsg.UnbufferedOutput(conn)
	output.SetCompression(compress)
	output.SetMaxSize(nmsg.EtherContainerSize, 0)
	for _, p := range payloads {
		if err := output.Send(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}

func expectObservations(t *testing.T, out chan observation.InputObservation, names ...string) []observation.InputObservation {
	t.Helper()
	result := make([]observation.InputObservation, 0)
	for _, name := range names {
		select {
		case o := <-out:
			if o.Rrname != name {
				t.Fatalf("expected observation for %s, got %s", name, o.Rrname)
			}
			result = append(result, o)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for observation for %s", name)
		}
	}
	return result
}

func expectNoObservations(t *testing.T, out chan observation.InputObservation) {
	t.Helper()
	select {
	case o := <-out:
		t.Fatalf("unexpected observation: %v", o)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestNmsgSocketFeeder(t *testing.T) {
	t.Parallel()
	_, out, conn := startNmsgFeeder(t)

	names := make([]string, 0)
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("test%d", i)
		names = append(names, name+".com")
		sendNmsg(t, conn, i%2 == 0, makeNmsgPayload(t, 2, name, []byte{10, 0, 0, byte(i)}))
	}

	for i, o := range expectObservations(t, out, names...) {
		if o.Rrtype != "A" || o.Rdata != fmt.Sprintf("10.0.0.%d", i) || o.SensorID != "[unknown]" ||
			o.TimestampStart.Unix() != 1500000000 {
			t.Fatalf("unexpected observation: %v", o)
		}
	}
	expectNoObservations(t, out)
}

func TestNmsgSocketFeederIgnoresOtherVendors(t *testing.T) {
	t.Parallel()
	_, out, conn := startNmsgFeeder(t)

	sendNmsg(t, conn, false, makeNmsgPayload(t, 1, "other", []byte{10, 0, 0, 1}))
	sendNmsg(t, conn, false, makeNmsgPayload(t, 2, "sie", []byte{10, 0, 0, 2}))

	expectObservations(t, out, "sie.com")
	expectNoObservations(t, out)
}

func TestNmsgSocketFeederFragmented(t *testing.T) {
	t.Parallel()
	_, out, conn := startNmsgFeeder(t)

	rdata := make([]byte, 4*nmsg.EtherContainerSize)
	for i := range rdata {
		rdata[i] = 'a' + byte(i%26)
	}
	sendNmsg(t, conn, false, makeNmsgPayload(t, 2, "large", rdata))

	o := expectObservations(t, out, "large.com")[0]
	if o.Rrtype != "TXT" || o.Rdata != string(rdata) {
		t.Fatalf("unexpected observation: %s %s (%d bytes)", o.Rrname, o.Rrtype, len(o.Rdata))
	}
}

func TestNmsgSocketFeederRecoversFromBadInput(t *testing.T) {
	t.Parallel()
	_, out, conn := startNmsgFeeder(t)

	send := func(b []byte) {
		t.Helper()
		if _, err := conn.Write(b); err != nil {
			t.Fatal(err)
		}
	}

	// container with a corrupted payload checksum
	var buf bytes.Buffer
	output := nmsg.UnbufferedOutput(&buf)
	if err := output.Send(makeNmsgPayload(t, 2, "badcrc", []byte{10, 0, 0, 1})); err != nil {
		t.Fatal(err)
	}
	badCRC := buf.Bytes()
	badCRC[len(badCRC)-1] ^= 0x01
	send(badCRC)
	sendNmsg(t, conn, false, makeNmsgPayload(t, 2, "first", []byte{10, 0, 0, 2}))

	// container header announcing a length beyond the maximum container size
	send([]byte{'N', 'M', 'S', 'G', 0, 2, 0xff, 0xff, 0xff, 0xff})
	sendNmsg(t, conn, false, makeNmsgPayload(t, 2, "second", []byte{10, 0, 0, 3}))

	expectObservations(t, out, "first.com", "second.com")
	expectNoObservations(t, out)
}
