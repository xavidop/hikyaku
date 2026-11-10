// Package httpmux shares HTTP listeners between the listen sources (http,
// ws, sse) and the sse destinations of a process. Several of them can serve
// different paths on one address, and a hot reload re-registers a path to
// swap its handler without closing the listener.
package httpmux

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	mu      sync.Mutex
	servers = map[string]*server{}
)

type server struct {
	addr   string
	tlsKey string
	ln     net.Listener
	srv    *http.Server

	mu     sync.RWMutex
	mounts map[string]*mount
}

type mount struct {
	h      http.Handler
	handle *Handle
}

// Options describe the listener a registration needs.
type Options struct {
	// TLS serves HTTPS; every registration on an address must agree.
	TLS *tls.Config
	// TLSKey identifies the TLS settings (the cert, key and CA files), so
	// two registrations can be compared.
	TLSKey string
}

// Handle is one registration.
type Handle struct {
	s    *server
	path string
	once sync.Once
}

// Register serves h at path on addr, starting a server for addr on first
// use. A path ending in "/" also serves everything below it. Registering a
// path that is taken replaces its handler: the earlier handle's Release then
// leaves the new one in place.
func Register(addr, path string, o Options, h http.Handler) (*Handle, error) {
	if path == "" {
		path = "/"
	}
	mu.Lock()
	defer mu.Unlock()
	s, ok := servers[addr]
	if ok && s.tlsKey != o.TLSKey {
		return nil, fmt.Errorf("listen %s: already serving with different TLS settings", addr)
	}
	if !ok {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("listen %s: %w", addr, err)
		}
		if o.TLS != nil {
			ln = tls.NewListener(ln, o.TLS)
		}
		// ":0" binds a fresh port each time: such a server is found by the
		// address it got, not the one asked for.
		key := addr
		if _, port, err := net.SplitHostPort(addr); err == nil && port == "0" {
			key = ln.Addr().String()
		}
		s = &server{addr: key, tlsKey: o.TLSKey, ln: ln, mounts: map[string]*mount{}}
		s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second}
		servers[key] = s
		go func() {
			if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				_ = err // the listener was closed under us; Release reports nothing
			}
		}()
	}
	hd := &Handle{s: s, path: path}
	s.mu.Lock()
	s.mounts[path] = &mount{h: h, handle: hd}
	s.mu.Unlock()
	return hd, nil
}

// Addr is the address actually bound (useful with ":0").
func (h *Handle) Addr() string { return h.s.ln.Addr().String() }

// Release unmounts the handler (unless another registration replaced it)
// and stops the server once nothing is mounted on it.
func (h *Handle) Release() {
	h.once.Do(func() {
		mu.Lock()
		defer mu.Unlock()
		s := h.s
		s.mu.Lock()
		if m, ok := s.mounts[h.path]; ok && m.handle == h {
			delete(s.mounts, h.path)
		}
		empty := len(s.mounts) == 0
		s.mu.Unlock()
		if !empty {
			return
		}
		delete(servers, s.addr)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(ctx)
	})
}

// ServeHTTP dispatches to the exact path, else the longest "/"-ended
// prefix.
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	m, ok := s.mounts[r.URL.Path]
	if !ok {
		var prefixes []string
		for p := range s.mounts {
			if strings.HasSuffix(p, "/") && strings.HasPrefix(r.URL.Path, p) {
				prefixes = append(prefixes, p)
			}
		}
		if len(prefixes) > 0 {
			sort.Slice(prefixes, func(i, j int) bool { return len(prefixes[i]) > len(prefixes[j]) })
			m, ok = s.mounts[prefixes[0]], true
		}
	}
	s.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	m.h.ServeHTTP(w, r)
}
