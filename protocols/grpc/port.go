package grpc

import (
	"errors"
	"net"
	"sync"
	"time"
)

// The grpc listen sources of a process share their TCP listeners the way
// httpmux shares HTTP ones: a hot reload starts the new relay while the old
// one still runs, so the new source must not fail to bind. A port hands its
// new connections to the newest source on it and closes once the last one
// lets go; each source's gRPC server (TLS included) serves what it is
// handed.
var (
	portsMu sync.Mutex
	ports   = map[string]*port{}
)

// port is one bound TCP listener.
type port struct {
	key  string
	ln   net.Listener
	subs []*portListener // newest last
}

// portListener is one source's share of a port: a net.Listener whose Accept
// returns the connections the port hands it.
type portListener struct {
	p     *port
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

// listenPort binds addr, or shares it when a grpc listen source of this
// process holds it. ":0" binds a fresh port every time.
func listenPort(addr string) (*portListener, error) {
	portsMu.Lock()
	defer portsMu.Unlock()
	p, ok := ports[addr]
	if !ok {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		key := addr
		if _, num, err := net.SplitHostPort(addr); err == nil && num == "0" {
			key = ln.Addr().String()
		}
		p = &port{key: key, ln: ln}
		ports[key] = p
		go p.accept()
	}
	l := &portListener{p: p, conns: make(chan net.Conn), done: make(chan struct{})}
	p.subs = append(p.subs, l)
	return l, nil
}

// accept hands every connection to the newest source until the listener
// closes.
func (p *port) accept() {
	var delay time.Duration
	for {
		c, err := p.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// A temporary failure, such as too many open files.
			delay = min(max(2*delay, 5*time.Millisecond), time.Second)
			time.Sleep(delay)
			continue
		}
		delay = 0
		p.handOff(c)
	}
}

// handOff gives c to the newest source, or closes it when none is left.
func (p *port) handOff(c net.Conn) {
	for {
		portsMu.Lock()
		var l *portListener
		if n := len(p.subs); n > 0 {
			l = p.subs[n-1]
		}
		portsMu.Unlock()
		if l == nil {
			_ = c.Close()
			return
		}
		select {
		case l.conns <- c:
			return
		case <-l.done: // it let go meanwhile: try the next newest
		}
	}
}

// Accept returns the next connection handed to this source.
func (l *portListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close lets go of the port, closing it when no other source holds it.
func (l *portListener) Close() error {
	l.once.Do(func() {
		close(l.done)
		portsMu.Lock()
		defer portsMu.Unlock()
		p := l.p
		for i, s := range p.subs {
			if s == l {
				p.subs = append(p.subs[:i], p.subs[i+1:]...)
				break
			}
		}
		if len(p.subs) == 0 {
			delete(ports, p.key)
			_ = p.ln.Close()
		}
	})
	return nil
}

// Addr is the address the port is bound to.
func (l *portListener) Addr() net.Addr { return l.p.ln.Addr() }
