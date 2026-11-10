// Package admin is the admin API of a running relay: health and readiness
// probes, Prometheus metrics, hot reload, redacted debug views, a live tail
// of envelopes, the feedback endpoint of the history-aware router, and
// history reads, exports and deletes.
//
// Mutating endpoints, and every endpoint that shows message content,
// require server.admin.token as a bearer token; without a token they
// answer loopback clients only.
package admin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
)

// Options configure the server.
type Options struct {
	Listen  string
	Token   string
	Metrics *metrics.Metrics
	// NoMetricsEndpoint leaves /metrics out (observability.metrics.prometheus:
	// false); the metrics are still counted.
	NoMetricsEndpoint bool
	Logger            *slog.Logger
	// Engine returns the engine serving now (a reload swaps it).
	Engine func() *engine.Engine
	// Reload rebuilds the relay from its spec files.
	Reload func(ctx context.Context) error
	// SpecYAML returns the spec with every secret value redacted.
	SpecYAML func() []byte
	// Redact removes secret values from what /v1/tail and /v1/history
	// return (messages can carry them); nil returns them as they are.
	Redact func([]byte) []byte
	// ModelHealth checks the models of the AI chains (readiness.models).
	ModelHealth func(ctx context.Context) error
	// ReadyOnModels makes /readyz fail while ModelHealth fails.
	ReadyOnModels bool
	// ReadyOnDestinations makes /readyz fail while a required destination
	// can take nothing (default true).
	ReadyOnDestinations bool
	// Version is shown by /healthz.
	Version string
	// Hub carries the engine's events to /v1/tail; nil makes a new one.
	// Servers that share a hub (an admin API that moved on reload) share
	// its events.
	Hub *Hub
}

// Server is the admin API.
type Server struct {
	o       atomic.Pointer[Options]
	hub     *Hub
	ownHub  bool // New made the hub, so Shutdown closes it
	metrics http.Handler
	srv     *http.Server
	ln      net.Listener
	mux     *http.ServeMux
	done    chan struct{} // closed by Shutdown: ends the tail streams
	stop    sync.Once
}

// New builds the server (call Start, or Listen then Serve, to answer).
func New(o Options) *Server {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{hub: o.Hub, mux: http.NewServeMux(), done: make(chan struct{})}
	if s.hub == nil {
		s.hub, s.ownHub = NewHub(), true
	}
	if o.Metrics != nil {
		s.metrics = o.Metrics.Handler()
	}
	s.o.Store(&o)
	s.routes()
	s.srv = &http.Server{Handler: s.mux, ReadHeaderTimeout: 10 * time.Second}
	return s
}

// opts are the options in effect.
func (s *Server) opts() *Options { return s.o.Load() }

// Configure gives a running server new options, from its next request on:
// the token, the readiness checks and the /metrics endpoint follow them.
// Listen, Hub, Metrics and Logger keep the values New had.
func (s *Server) Configure(o Options) {
	cur := s.opts()
	o.Listen, o.Hub, o.Metrics, o.Logger = cur.Listen, cur.Hub, cur.Metrics, cur.Logger
	s.o.Store(&o)
}

// Hub is where the engine's tap sends events (Options.Tap of the engine).
func (s *Server) Hub() *Hub { return s.hub }

// Handler is the API's handler (tests).
func (s *Server) Handler() http.Handler { return s.mux }

// Start listens and serves in the background.
func (s *Server) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}
	s.Serve()
	return nil
}

// Listen binds Options.Listen without answering yet: connections wait
// until Serve. A reload binds a new admin address first, so an address
// that is taken fails the reload.
func (s *Server) Listen() error {
	addr := s.opts().Listen
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("admin: listen %s: %w", addr, err)
	}
	s.ln = ln
	return nil
}

// Serve answers on the bound address in the background.
func (s *Server) Serve() {
	go func() {
		if err := s.srv.Serve(s.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.opts().Logger.Error("admin server stopped", "err", err)
		}
	}()
}

// Addr is the bound address.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Shutdown stops the server: it closes the address, ends its tail streams
// and waits until ctx ends for the requests in flight, such as the POST
// /reload that moved the admin API. A hub New made is closed too; a shared
// one is left to its owner.
func (s *Server) Shutdown(ctx context.Context) error {
	s.stop.Do(func() { close(s.done) })
	if s.ownHub {
		s.hub.Close()
	}
	err := s.srv.Shutdown(ctx)
	if s.ln != nil {
		_ = s.ln.Close() // bound but never served
	}
	return err
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "version": s.opts().Version})
	})
	s.mux.HandleFunc("GET /readyz", s.readyz)
	if s.metrics != nil {
		s.mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
			if s.opts().NoMetricsEndpoint {
				http.NotFound(w, r)
				return
			}
			s.metrics.ServeHTTP(w, r)
		})
	}
	s.mux.HandleFunc("POST /reload", s.protect(s.reload))
	s.mux.HandleFunc("GET /debug/config", s.protect(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		if o := s.opts(); o.SpecYAML != nil {
			_, _ = w.Write(o.SpecYAML())
		}
	}))
	s.mux.HandleFunc("GET /debug/state", s.protect(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, s.opts().Engine().Health(r.Context()))
	}))
	s.mux.HandleFunc("GET /v1/tail", s.protect(s.redacting(s.tail)))
	s.mux.HandleFunc("POST /v1/feedback", s.protect(s.feedback))
	s.mux.HandleFunc("GET /v1/history/{route}/{key}", s.protect(s.redacting(s.historyShow)))
	s.mux.HandleFunc("DELETE /v1/history/{route}/{key}", s.protect(s.historyForget))
	s.mux.HandleFunc("GET /v1/history", s.protect(s.redacting(s.historyExport)))
}

// protect requires the bearer token, or a loopback client when there is
// none.
func (s *Server) protect(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if token := s.opts().Token; token != "" {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got)), []byte(token)) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeJSON(w, 401, map[string]string{"error": "unauthorized"})
				return
			}
		} else if !loopback(r.RemoteAddr) {
			writeJSON(w, 403, map[string]string{"error": "set server.admin.token to use this endpoint from another host"})
			return
		}
		h(w, r)
	}
}

func loopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// redacting passes what h writes through Options.Redact. Each write of
// these handlers is a whole JSON document or event, so a secret is never
// split across two writes.
func (s *Server) redacting(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if redact := s.opts().Redact; redact != nil {
			w = &redactWriter{ResponseWriter: w, redact: redact}
		}
		h(w, r)
	}
}

type redactWriter struct {
	http.ResponseWriter
	redact func([]byte) []byte
}

func (w *redactWriter) Write(b []byte) (int, error) {
	if _, err := w.ResponseWriter.Write(w.redact(b)); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *redactWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	o := s.opts()
	h := o.Engine().Health(r.Context())
	ready := (h.Ready || !o.ReadyOnDestinations) && h.ListenersUp
	body := map[string]any{"ready": ready, "destinations": h.Destinations, "stores": h.Stores}
	if len(h.Sources) > 0 {
		body["sources"] = h.Sources
	}
	if o.ReadyOnModels && o.ModelHealth != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := o.ModelHealth(ctx); err != nil {
			ready = false
			body["models"] = err.Error()
		}
	}
	body["ready"] = ready
	code := 200
	if !ready {
		code = 503
	}
	writeJSON(w, code, body)
}

// reload answers once the reload is done. A reload that moves the admin
// API shuts this server down after the answer: Shutdown waits for it.
func (s *Server) reload(w http.ResponseWriter, r *http.Request) {
	reload := s.opts().Reload
	if reload == nil {
		writeJSON(w, 501, map[string]string{"error": "reload is not available"})
		return
	}
	if err := reload(r.Context()); err != nil {
		writeJSON(w, 422, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "reloaded"})
}

// FeedbackRequest is the body of POST /v1/feedback.
type FeedbackRequest struct {
	MsgID   string   `json:"msg_id"`
	Correct []string `json:"correct"`
	Note    string   `json:"note,omitempty"`
	Source  string   `json:"source,omitempty"`
}

// historyStores are the distinct stores the AI routes keep history in.
func historyStores(e *engine.Engine) []store.HistoryStore {
	seen := map[store.Store]bool{}
	var out []store.HistoryStore
	add := func(name string) {
		st, err := e.Store(name)
		if err == nil && !seen[st] {
			seen[st] = true
			out = append(out, st)
		}
	}
	for _, r := range e.Spec().Routes {
		var walk func(rs *config.RouterSpec)
		walk = func(rs *config.RouterSpec) {
			if rs == nil {
				return
			}
			if rs.History != nil {
				add(rs.History.Store)
			}
			for i := range rs.Fallback {
				walk(&rs.Fallback[i])
			}
		}
		walk(r.Router)
	}
	if len(out) == 0 {
		add("")
	}
	return out
}

func (s *Server) feedback(w http.ResponseWriter, r *http.Request) {
	var req FeedbackRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "body: " + err.Error()})
		return
	}
	if req.MsgID == "" || len(req.Correct) == 0 {
		writeJSON(w, 400, map[string]string{"error": "msg_id and correct are required"})
		return
	}
	e := s.opts().Engine()
	for _, d := range req.Correct {
		if _, ok := e.Spec().DestinationByName(d); !ok {
			writeJSON(w, 400, map[string]string{"error": fmt.Sprintf("destination %q is not declared", d)})
			return
		}
	}
	src := req.Source
	if src == "" {
		src = "api"
	}
	fb := store.Feedback{Correct: req.Correct, Note: req.Note, Source: src, At: time.Now().UTC()}
	for _, st := range historyStores(e) {
		rec, err := st.Feedback(r.Context(), req.MsgID, fb)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": err.Error()})
			return
		}
		if m := s.opts().Metrics; m != nil {
			m.Feedback.WithLabelValues(rec.Route).Inc()
		}
		writeJSON(w, 200, rec)
		return
	}
	writeJSON(w, 404, map[string]string{"error": "no history record for message " + req.MsgID + " (unknown, or expired)"})
}

// HistoryView is what GET /v1/history/{route}/{key} returns: the records,
// and the turns exactly as the model sees them.
type HistoryView struct {
	Route   string             `json:"route"`
	Key     string             `json:"key"`
	System  string             `json:"system,omitempty"`
	Records []store.CallRecord `json:"records"`
	Turns   []store.Turn       `json:"turns"`
}

func (s *Server) routeHistory(e *engine.Engine, route string) (*config.RouterSpec, store.HistoryStore, error) {
	rt, ok := e.Spec().RouteByName(route)
	if !ok {
		return nil, nil, fmt.Errorf("route %q is not declared", route)
	}
	var found *config.RouterSpec
	var walk func(rs *config.RouterSpec)
	walk = func(rs *config.RouterSpec) {
		if rs == nil || found != nil {
			return
		}
		if rs.History != nil {
			found = rs
			return
		}
		for i := range rs.Fallback {
			walk(&rs.Fallback[i])
		}
	}
	walk(rt.Router)
	if found == nil {
		return nil, nil, fmt.Errorf("route %q keeps no history", route)
	}
	st, err := e.Store(found.History.Store)
	return found, st, err
}

func (s *Server) historyShow(w http.ResponseWriter, r *http.Request) {
	e := s.opts().Engine()
	route, key := r.PathValue("route"), r.PathValue("key")
	rs, st, err := s.routeHistory(e, route)
	if err != nil {
		writeJSON(w, 404, map[string]string{"error": err.Error()})
		return
	}
	recs, err := st.Recent(r.Context(), route, key, router.WindowOf(rs.History.Window))
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": err.Error()})
		return
	}
	maxBytes := config.DefaultAIMaxBytes
	if rs.Input != nil && rs.Input.MaxBytes > 0 {
		maxBytes = int(rs.Input.MaxBytes)
	}
	turns := store.BuildTurns(recs, "(the next message)", maxBytes)
	view := HistoryView{Route: route, Key: key, Records: recs, Turns: turns.Turns}
	if recs == nil {
		view.Records = []store.CallRecord{}
	}
	if len(recs) > 0 {
		view.System = strings.TrimSpace(strings.TrimSpace(rs.Instructions) + "\n\n" + store.HistoryPreamble)
	}
	writeJSON(w, 200, view)
}

func (s *Server) historyForget(w http.ResponseWriter, r *http.Request) {
	e := s.opts().Engine()
	route, key := r.PathValue("route"), r.PathValue("key")
	_, st, err := s.routeHistory(e, route)
	if err != nil {
		writeJSON(w, 404, map[string]string{"error": err.Error()})
		return
	}
	if err := st.Forget(r.Context(), route, key); err != nil {
		writeJSON(w, 503, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "forgotten", "route": route, "key": key})
}

// historyExport streams records as NDJSON: ?route=r&since=30d&labeled=true.
func (s *Server) historyExport(w http.ResponseWriter, r *http.Request) {
	e := s.opts().Engine()
	q := r.URL.Query()
	since := time.Time{}
	if v := q.Get("since"); v != "" {
		d, err := config.ParseDuration(v)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		since = time.Now().Add(-d)
	}
	labeled := q.Get("labeled") == "true"
	route := q.Get("route")
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, st := range historyStores(e) {
		err := st.Records(r.Context(), route, since, func(rec store.CallRecord) error {
			if labeled && (rec.Feedback == nil || len(rec.Feedback.Correct) == 0) {
				return nil
			}
			return enc.Encode(rec)
		})
		if err != nil {
			_ = enc.Encode(map[string]string{"error": err.Error()})
			return
		}
	}
}

// Event is one tail event.
type Event struct {
	Type     string             `json:"type"`
	Envelope *envelope.Envelope `json:"envelope"`
	Report   *engine.Report     `json:"report,omitempty"`
}

func (s *Server) tail(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, 500, map[string]string{"error": "streaming unsupported"})
		return
	}
	source := r.URL.Query().Get("source")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)
	fl.Flush()
	ch, cancel := s.hub.Subscribe(256)
	defer cancel()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done: // this server is shutting down; the hub may live on
			return
		case <-ping.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if source != "" && ev.Envelope.Source != source {
				continue
			}
			b, err := json.Marshal(Event{Type: ev.Type, Envelope: ev.Envelope, Report: ev.Report})
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b)
			fl.Flush()
		}
	}
}

// Hub fans engine events out to tail subscribers. A slow subscriber loses
// events rather than slowing the relay down.
type Hub struct {
	mu     sync.Mutex
	subs   map[chan engine.Event]struct{}
	closed bool
}

// NewHub returns an empty hub.
func NewHub() *Hub { return &Hub{subs: map[chan engine.Event]struct{}{}} }

// Publish sends an event to every subscriber that keeps up.
func (h *Hub) Publish(ev engine.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// Subscribe returns a channel of events and its cancel.
func (h *Hub) Subscribe(buf int) (<-chan engine.Event, func()) {
	ch := make(chan engine.Event, buf)
	h.mu.Lock()
	if h.closed {
		close(ch)
		h.mu.Unlock()
		return ch, func() {}
	}
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			if _, ok := h.subs[ch]; ok {
				delete(h.subs, ch)
				close(ch)
			}
		})
	}
}

// Close ends every subscription.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for ch := range h.subs {
		delete(h.subs, ch)
		close(ch)
	}
}

// Subscribers is how many tails are open.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}
