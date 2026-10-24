package node

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const (
	muxHeaderTimeout = 3 * time.Second
	muxAcceptBacklog = 16
)

// mux shares one TCP listener between the Raft groups on a node. A dialer
// writes the 4-byte big-endian group ID first; the accept loop reads it and
// hands the connection to that group's stream layer.
type mux struct {
	ln     net.Listener
	adv    addr
	layers map[uint32]*layer
	done   chan struct{}
	once   sync.Once
}

// addr is a net.Addr holding the advertised Raft address.
type addr string

func (a addr) Network() string { return "tcp" }
func (a addr) String() string  { return string(a) }

// newMux starts accepting on ln. advertise is the address other nodes dial,
// which is what each layer reports as its own.
func newMux(ln net.Listener, advertise string, groups []int) *mux {
	m := &mux{ln: ln, adv: addr(advertise), layers: make(map[uint32]*layer, len(groups)), done: make(chan struct{})}
	for _, g := range groups {
		m.layers[uint32(g)] = &layer{
			m:      m,
			group:  uint32(g),
			accept: make(chan net.Conn, muxAcceptBacklog),
			done:   make(chan struct{}),
			conns:  map[*muxConn]struct{}{},
		}
	}
	go m.serve()
	return m
}

// layer returns the stream layer for group g, or nil.
func (m *mux) layer(g int) *layer { return m.layers[uint32(g)] }

// Close stops accepting and closes every layer and its connections.
func (m *mux) Close() error {
	m.once.Do(func() {
		close(m.done)
		m.ln.Close()
		for _, l := range m.layers {
			l.Close()
		}
	})
	return nil
}

func (m *mux) serve() {
	for {
		c, err := m.ln.Accept()
		if err != nil {
			select {
			case <-m.done:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		go m.route(c)
	}
}

// route reads the group header and delivers c to that group, or drops it.
func (m *mux) route(c net.Conn) {
	var hdr [4]byte
	c.SetReadDeadline(time.Now().Add(muxHeaderTimeout))
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		c.Close()
		return
	}
	c.SetReadDeadline(time.Time{})
	l := m.layers[binary.BigEndian.Uint32(hdr[:])]
	if l == nil {
		c.Close()
		return
	}
	l.deliver(c)
}

// layer is one group's raft.StreamLayer over the shared listener. It tracks
// its connections so that Close drops all of them, as a process exit would.
type layer struct {
	m      *mux
	group  uint32
	accept chan net.Conn
	done   chan struct{}
	once   sync.Once

	mu    sync.Mutex
	conns map[*muxConn]struct{} // nil once closed
}

type muxConn struct {
	net.Conn
	l *layer
}

func (c *muxConn) Close() error {
	c.l.mu.Lock()
	delete(c.l.conns, c)
	c.l.mu.Unlock()
	return c.Conn.Close()
}

// track registers c, or closes it if the layer is closed.
func (l *layer) track(c net.Conn) (net.Conn, error) {
	mc := &muxConn{c, l}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conns == nil {
		c.Close()
		return nil, net.ErrClosed
	}
	l.conns[mc] = struct{}{}
	return mc, nil
}

func (l *layer) deliver(c net.Conn) {
	mc, err := l.track(c)
	if err != nil {
		return
	}
	select {
	case l.accept <- mc:
	case <-l.done:
		mc.Close()
	}
}

// Accept implements net.Listener.
func (l *layer) Accept() (net.Conn, error) {
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close implements net.Listener. It leaves the shared listener open.
func (l *layer) Close() error {
	l.once.Do(func() {
		close(l.done)
		l.mu.Lock()
		conns := l.conns
		l.conns = nil
		l.mu.Unlock()
		for c := range conns {
			c.Conn.Close()
		}
	})
	return nil
}

// Addr implements net.Listener and returns the advertised address.
func (l *layer) Addr() net.Addr { return l.m.adv }

// Dial implements raft.StreamLayer.
func (l *layer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	c, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], l.group)
	c.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := c.Write(hdr[:]); err != nil {
		c.Close()
		return nil, err
	}
	c.SetWriteDeadline(time.Time{})
	return l.track(c)
}
