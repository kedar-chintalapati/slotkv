package node

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func newTestMux(t *testing.T, advertise string, groups ...int) *mux {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := newMux(ln, advertise, groups)
	t.Cleanup(func() { m.Close() })
	return m
}

func acceptWithin(t *testing.T, l net.Listener, d time.Duration) net.Conn {
	t.Helper()
	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := l.Accept()
		ch <- res{c, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Accept: %v", r.err)
		}
		return r.c
	case <-time.After(d):
		t.Fatal("Accept timed out")
		return nil
	}
}

func readString(t *testing.T, c net.Conn, n int) string {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, n)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(buf)
}

func TestMuxRoutesTrafficByGroup(t *testing.T) {
	a := newTestMux(t, "a:7001", 0, 1)
	b := newTestMux(t, "b:7001", 0, 1)
	target := raft.ServerAddress(b.ln.Addr().String())

	if got := b.layer(0).Addr().String(); got != "b:7001" {
		t.Fatalf("Addr() = %q, want the advertised address", got)
	}

	// Dial group 1 first, so routing cannot be by arrival order.
	c1, err := a.layer(1).Dial(target, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c0, err := a.layer(0).Dial(target, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := c0.Write([]byte("zero")); err != nil {
		t.Fatal(err)
	}

	s0 := acceptWithin(t, b.layer(0), time.Second)
	s1 := acceptWithin(t, b.layer(1), time.Second)
	if got := readString(t, s0, 4); got != "zero" {
		t.Fatalf("group 0 received %q", got)
	}
	if got := readString(t, s1, 3); got != "one" {
		t.Fatalf("group 1 received %q", got)
	}

	// The connection is duplex and nothing else was queued.
	if _, err := s1.Write([]byte("ack")); err != nil {
		t.Fatal(err)
	}
	if got := readString(t, c1, 3); got != "ack" {
		t.Fatalf("reply = %q", got)
	}
	for g := 0; g < 2; g++ {
		select {
		case c := <-b.layer(g).accept:
			t.Fatalf("group %d has an unexpected extra connection %v", g, c)
		default:
		}
	}
}

func TestMuxDropsUnknownGroup(t *testing.T) {
	b := newTestMux(t, "b:7001", 0)
	c, err := net.Dial("tcp", b.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 99)
	c.Write(hdr[:])
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read after unknown group = %v, want EOF", err)
	}
}

func TestMuxLayerCloseDropsConnections(t *testing.T) {
	a := newTestMux(t, "a:7001", 0)
	b := newTestMux(t, "b:7001", 0)
	c, err := a.layer(0).Dial(raft.ServerAddress(b.ln.Addr().String()), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	s := acceptWithin(t, b.layer(0), time.Second)

	b.layer(0).Close()
	if _, err := b.layer(0).Accept(); err == nil {
		t.Fatal("Accept succeeded on a closed layer")
	}
	s.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := s.Read(make([]byte, 1)); err == nil {
		t.Fatal("accepted connection survived layer Close")
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("dialer read = %v, want EOF", err)
	}

	// The shared listener stays up for the other layers; a closed layer cannot dial.
	a.layer(0).Close()
	if _, err := a.layer(0).Dial(raft.ServerAddress(b.ln.Addr().String()), time.Second); err == nil {
		t.Fatal("Dial succeeded on a closed layer")
	}
	b.Close()
	if _, err := net.DialTimeout("tcp", b.ln.Addr().String(), 500*time.Millisecond); err == nil {
		t.Fatal("listener still accepting after mux Close")
	}
}
