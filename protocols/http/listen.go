package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// Listener is an http listen source: hikyaku is the server.
type Listener struct {
	refused  func(source string) // hikyaku_backpressure_total
	name     string
	kind     string
	listen   string
	path     string
	methods  []string
	maxBody  int64
	ack      string // delivered | accepted
	durable  bool   // a disk buffer: accepted waits until the message is on disk
	respond  *config.Respond
	auth     *inbound.Authenticator
	tlsOpts  httpmux.Options
	dedupe   *dedupe
	mu       sync.Mutex
	handle   *httpmux.Handle
	ready    chan struct{}
	readyErr error
	out      chan<- *envelope.Envelope
}

// dedupe drops messages whose key was seen within the ttl.
type dedupe struct {
	key   *transform.Template
	ttl   time.Duration
	store func() (store.KV, error)
	src   string
	count func(source string) // hikyaku_deduped_total

	once sync.Once
	kv   store.KV
	err  error
}

func newDedupe(src string, d *config.Dedupe, env plugin.Env) (*dedupe, error) {
	if d == nil {
		return nil, nil
	}
	key, err := transform.CompileTemplate("dedupe.key", d.Key)
	if err != nil {
		return nil, err
	}
	ttl := d.TTL.Std()
	if ttl <= 0 {
		ttl = config.DefaultDedupeTTL
	}
	name := d.Store
	return &dedupe{key: key, ttl: ttl, src: src, count: env.Deduped, store: func() (store.KV, error) {
		if env.Store == nil {
			return store.NewMemory("dedupe"), nil
		}
		return env.Store(name)
	}}, nil
}

// seen reports a duplicate. A key that renders empty, or a store that is
// down, lets the message through: dedupe never drops by accident.
func (d *dedupe) seen(ctx context.Context, env *envelope.Envelope) bool {
	if d == nil {
		return false
	}
	k, err := d.key.Render(env)
	if err != nil || k == "" {
		return false
	}
	d.once.Do(func() { d.kv, d.err = d.store() })
	if d.err != nil {
		return false
	}
	fresh, err := d.kv.SetNX(ctx, "dedupe:"+d.src+":"+k, []byte(env.ID), d.ttl)
	dup := err == nil && !fresh
	if dup && d.count != nil {
		d.count(d.src)
	}
	return dup
}

// Dedupes reports that the listener applies its dedupe: block itself (it
// answers a duplicate with its status), so the engine leaves it alone.
func (l *Listener) Dedupes() bool { return l.dedupe != nil }

func newListener(ctx context.Context, s plugin.Spec, src *config.Source) (*Listener, error) {
	l := &Listener{
		name:    s.Name,
		kind:    s.Kind,
		listen:  src.Listen,
		path:    src.Path,
		maxBody: int64(src.MaxBodyBytes),
		ack:     src.Ack,
		durable: s.Env.Buffer == "disk",
		respond: src.Respond,
		ready:   make(chan struct{}),
		refused: s.Env.Refused,
	}
	if l.path == "" {
		l.path = "/"
	}
	if l.maxBody <= 0 {
		l.maxBody = config.DefaultMaxBodyBytes
	}
	if l.ack == "" {
		l.ack = "delivered"
	}
	l.methods = []string{http.MethodPost, http.MethodPut}
	if len(src.Methods) > 0 {
		l.methods = nil
		for _, m := range src.Methods {
			l.methods = append(l.methods, strings.ToUpper(m))
		}
	}
	tlsCfg, key, err := httpmux.ServerTLS(src.TLS)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", s.Name, err)
	}
	if s.Kind == "https" && tlsCfg == nil {
		return nil, fmt.Errorf("source %s: an https listener needs tls.cert_file and tls.key_file", s.Name)
	}
	l.tlsOpts = httpmux.Options{TLS: tlsCfg, TLSKey: key}
	if l.auth, err = inbound.New(ctx, src.Auth); err != nil {
		return nil, fmt.Errorf("source %s: %w", s.Name, err)
	}
	if l.dedupe, err = newDedupe(s.Name, src.Dedupe, s.Env); err != nil {
		return nil, fmt.Errorf("source %s: %w", s.Name, err)
	}
	return l, nil
}

// Name is the source name.
func (l *Listener) Name() string { return l.name }

// Addr is the bound address once listening (useful with ":0").
func (l *Listener) Addr() string {
	<-l.ready
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.handle == nil {
		return ""
	}
	return l.handle.Addr()
}

// Health reports whether the listener is up.
func (l *Listener) Health(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.handle == nil {
		return errors.New("not listening")
	}
	return nil
}

// Start serves until ctx ends.
func (l *Listener) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	l.out = out
	h, err := httpmux.Register(l.listen, l.path, l.tlsOpts, http.HandlerFunc(l.serve))
	l.mu.Lock()
	l.handle, l.readyErr = h, err
	l.mu.Unlock()
	close(l.ready)
	if err != nil {
		return err
	}
	<-ctx.Done()
	h.Release()
	l.mu.Lock()
	l.handle = nil
	l.mu.Unlock()
	return nil
}

type reply struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

var propagator = propagation.TraceContext{}

func (l *Listener) serve(w http.ResponseWriter, r *http.Request) {
	if !slices.Contains(l.methods, r.Method) {
		w.Header().Set("Allow", strings.Join(l.methods, ", "))
		writeJSON(w, http.StatusMethodNotAllowed, reply{Status: "method_not_allowed"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, l.maxBody+1))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, reply{Status: "bad_request", Error: err.Error()})
		return
	}
	if int64(len(body)) > l.maxBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, reply{Status: "too_large"})
		return
	}
	if err := l.auth.Check(r, body); err != nil {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeJSON(w, http.StatusUnauthorized, reply{Status: "unauthorized"})
		return
	}
	// Decoded after the checks above: a sender signs the bytes it sent.
	body, decoded, err := decodeBody(r.Header.Get("Content-Encoding"), body, l.maxBody)
	switch {
	case errors.Is(err, errTooLarge):
		writeJSON(w, http.StatusRequestEntityTooLarge, reply{Status: "too_large"})
		return
	case err != nil:
		writeJSON(w, http.StatusBadRequest, reply{Status: "bad_request", Error: err.Error()})
		return
	}
	env := envelope.New(l.name, l.kind, body)
	if key := r.Header.Get("Idempotency-Key"); key != "" && len(key) <= 256 {
		env.ID = key
		env.Attributes["idempotency_key"] = key
	}
	env.Subject = r.URL.Path
	env.ContentType = r.Header.Get("Content-Type")
	for k, v := range r.Header {
		env.Headers[k] = slices.Clone(v)
	}
	l.auth.StripCredentials(env.Headers)
	if decoded {
		// The message holds the decoded body now.
		delete(env.Headers, "Content-Encoding")
		delete(env.Headers, "Content-Length")
	}
	env.Attributes["http.method"] = r.Method
	env.Attributes["http.path"] = r.URL.Path
	if q := l.auth.StripQuery(r.URL.RawQuery); q != "" {
		env.Attributes["http.query"] = q
	}
	env.Attributes["remote_addr"] = r.RemoteAddr
	if sc := trace.SpanContextFromContext(propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))); sc.IsValid() {
		env.Trace = sc
	}
	if l.dedupe.seen(r.Context(), env) {
		writeJSON(w, http.StatusOK, reply{ID: env.ID, Status: "duplicate"})
		return
	}

	wait := l.ack == "delivered"
	var done chan envelope.Outcome
	var queued chan struct{}
	if wait || l.durable {
		done = make(chan envelope.Outcome, 1)
		env.OnAck(func(_ context.Context, o envelope.Outcome) { done <- o })
	}
	if !wait && l.durable {
		queued = make(chan struct{})
		env.OnQueued(func() { close(queued) })
	}
	select {
	case l.out <- env:
	default:
		// The buffer is full: push back instead of dropping (design §7.5).
		if l.refused != nil {
			l.refused(l.name)
		}
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, reply{ID: env.ID, Status: "busy"})
		return
	}
	if !wait {
		if queued != nil {
			l.awaitDisk(w, r, env, queued, done)
			return
		}
		writeJSON(w, http.StatusAccepted, reply{ID: env.ID, Status: "accepted"})
		return
	}
	timeout := config.DefaultRespondWait
	if l.respond != nil && l.respond.Timeout > 0 {
		timeout = l.respond.Timeout.Std()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case o := <-done:
		l.answer(w, env, o)
	case <-timer.C:
		if l.respond != nil {
			writeJSON(w, http.StatusGatewayTimeout, reply{ID: env.ID, Status: "pending", Error: "no reply in time; the message is still being delivered"})
			return
		}
		writeJSON(w, http.StatusAccepted, reply{ID: env.ID, Status: "pending"})
	case <-r.Context().Done():
		// The caller went away. The message is the relay's now: it is
		// still delivered, and a retry with the same Idempotency-Key is safe.
	}
}

// answer writes the outcome: the reply of respond.from in sync mode, else a
// status the caller can act on (503 asks it to retry a message the relay
// could not keep).
func (l *Listener) answer(w http.ResponseWriter, env *envelope.Envelope, o envelope.Outcome) {
	if l.respond != nil {
		if rp, ok := o.Replies[l.respond.From]; ok && rp != nil {
			for k, vs := range rp.Headers {
				if dropHeaders[k] {
					continue
				}
				for _, v := range vs {
					w.Header().Add(k, v)
				}
			}
			status := rp.Status
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			_, _ = w.Write(rp.Body)
			return
		}
	}
	switch o.Status {
	case envelope.Delivered, envelope.Filtered, envelope.Unrouted:
		if l.respond != nil {
			writeJSON(w, http.StatusAccepted, reply{ID: env.ID, Status: o.Status.String(), Error: l.respond.From + " did not take the message"})
			return
		}
		writeJSON(w, http.StatusOK, reply{ID: env.ID, Status: o.Status.String()})
	case envelope.DeadLettered:
		// A body a route's validate step refused is the caller's to fix:
		// it is told what is wrong with it.
		if slices.Contains(plugin.Tags(o.Err), plugin.TagSchema) {
			writeJSON(w, http.StatusUnprocessableEntity, reply{ID: env.ID, Status: o.Status.String(), Error: o.Err.Error()})
			return
		}
		code := http.StatusAccepted
		if l.respond != nil {
			code = http.StatusBadGateway
		}
		writeJSON(w, code, reply{ID: env.ID, Status: o.Status.String()})
	default:
		w.Header().Set("Retry-After", "5")
		msg := ""
		if o.Err != nil {
			msg = "the relay could not keep the message; retry"
		}
		writeJSON(w, http.StatusServiceUnavailable, reply{ID: env.ID, Status: o.Status.String(), Error: msg})
	}
}

// awaitDisk answers an ack: accepted request once a disk buffer has the
// message: 202 when it is on disk, 503 when the buffer could not write it.
func (l *Listener) awaitDisk(w http.ResponseWriter, r *http.Request, env *envelope.Envelope, queued <-chan struct{}, done <-chan envelope.Outcome) {
	timer := time.NewTimer(config.DefaultRespondWait)
	defer timer.Stop()
	select {
	case <-queued:
		writeJSON(w, http.StatusAccepted, reply{ID: env.ID, Status: "accepted"})
	case o := <-done:
		if o.Status == envelope.Failed {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusServiceUnavailable, reply{ID: env.ID, Status: "failed", Error: "the buffer could not store the message"})
			return
		}
		writeJSON(w, http.StatusAccepted, reply{ID: env.ID, Status: o.Status.String()})
	case <-timer.C:
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, reply{ID: env.ID, Status: "busy", Error: "the buffer did not store the message in time"})
	case <-r.Context().Done():
	}
}
