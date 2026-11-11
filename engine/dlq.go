package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
)

// DLQ reasons.
const (
	ReasonDelivery  = "delivery"  // a required destination and its fallbacks failed
	ReasonNoRoute   = "no_route"  // no router of the chain could decide
	ReasonTransform = "transform" // a route transform failed
	ReasonSchema    = "schema"    // a route's validate step refused the body
)

// DLQInfo is the metadata stored with a dead letter, under the dlq
// attribute.
type DLQInfo struct {
	At          time.Time `json:"at"`
	Relay       string    `json:"relay"`
	Route       string    `json:"route,omitempty"`
	Destination string    `json:"destination,omitempty"`
	Reason      string    `json:"reason"`
	Attempts    int       `json:"attempts"`
	Error       string    `json:"error"`
	Hops        []Hop     `json:"hops,omitempty"`
}

// deadLetter is the DLQ and its fallback chain.
type deadLetter struct {
	hops   []dlqHop
	policy retryPolicy
}

type dlqHop struct {
	name string
	sink plugin.Sink
	own  bool // built here: closed with the engine
}

func (e *Engine) buildDLQ(ctx context.Context) error {
	spec := e.spec.DeadLetter
	if spec == nil {
		spec = &config.DeadLetter{Endpoint: config.Endpoint{Type: "stderr"}}
	}
	retry := config.ResolveRetry(spec.Retry, &config.Retry{Attempts: config.DefaultDLQAttempts, Backoff: config.Duration(100 * time.Millisecond), MaxBackoff: config.Duration(2 * time.Second)})
	retry.RetryOn = config.TagList{"connect", "timeout", "5xx", "429"}
	dl := &deadLetter{policy: newRetryPolicy(retry)}
	primary, err := e.dlqSink(ctx, "dead_letter", spec.Endpoint)
	if err != nil {
		return fmt.Errorf("dead_letter: %w", err)
	}
	dl.hops = append(dl.hops, primary)
	for _, f := range spec.Fallback {
		if d, ok := e.dests[f]; ok && d.group == nil {
			dl.hops = append(dl.hops, dlqHop{name: f, sink: d.endpoints[0].sink})
			continue
		}
		h, err := e.dlqSink(ctx, f, config.Endpoint{Type: f})
		if err != nil {
			return fmt.Errorf("dead_letter.fallback %s: %w", f, err)
		}
		dl.hops = append(dl.hops, h)
	}
	e.dlq = dl
	return nil
}

// dlqSink builds a DLQ hop. stdout, stderr and file are built in, so the
// last resort never depends on which protocols a binary links.
func (e *Engine) dlqSink(ctx context.Context, name string, ep config.Endpoint) (dlqHop, error) {
	switch ep.Type {
	case "stdout":
		return dlqHop{name: name, sink: newWriterSink(name, os.Stdout, ep.Format), own: true}, nil
	case "stderr":
		return dlqHop{name: name, sink: newWriterSink(name, os.Stderr, ep.Format), own: true}, nil
	case "file":
		s, err := newFileSink(name, ep.Path, ep.Format)
		if err != nil {
			return dlqHop{}, err
		}
		return dlqHop{name: name, sink: s, own: true}, nil
	}
	s, err := plugin.NewSink(ctx, plugin.Spec{Name: name, Kind: ep.Type, Options: ep.Options, Config: &ep, Env: e.pluginEnv()})
	if err != nil {
		return dlqHop{}, err
	}
	return dlqHop{name: name, sink: s, own: true}, nil
}

// deadLetter stores a message in the DLQ, falling through its fallback
// chain. It returns an error only when every hop failed: the message is
// then nacked, never dropped.
func (e *Engine) deadLetter(ctx context.Context, env *envelope.Envelope, info DLQInfo) error {
	ctx, span := metrics.Tracer().Start(ctx, "hikyaku.dead_letter", trace.WithAttributes(
		attribute.String("hikyaku.id", env.ID), attribute.String("hikyaku.reason", info.Reason),
		attribute.String("hikyaku.route", info.Route), attribute.String("hikyaku.destination", info.Destination)))
	defer span.End()
	info.At = e.now().UTC()
	info.Relay = e.spec.Name
	denv := env.Clone()
	denv.SetAttribute("dlq", info)
	var errs []error
	for _, h := range e.dlq.hops {
		if err := e.dlqWrite(ctx, h, denv); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", h.name, err))
			e.log.Error("dead-letter hop failed", "hop", h.name, "id", env.ID, "err", err)
			continue
		}
		if e.m != nil {
			e.m.DLQ.WithLabelValues(info.Destination, "stored").Inc()
		}
		span.SetAttributes(attribute.String("hikyaku.dlq.hop", h.name))
		e.log.Warn("dead-lettered", "id", env.ID, "route", info.Route, "destination", info.Destination, "reason", info.Reason, "hop", h.name, "err", info.Error)
		e.notifier.emit(Alert{Event: config.NotifyDeadLetter, Destination: info.Destination, Route: info.Route, Reason: info.Reason, Error: info.Error, MessageID: env.ID})
		return nil
	}
	if e.m != nil {
		e.m.DLQ.WithLabelValues(info.Destination, "failed").Inc()
	}
	err := fmt.Errorf("every dead-letter hop failed: %w", errors.Join(errs...))
	span.SetStatus(codes.Error, err.Error())
	return err
}

func (e *Engine) dlqWrite(ctx context.Context, h dlqHop, env *envelope.Envelope) error {
	var err error
	for n := 1; n <= e.dlq.policy.attempts; n++ {
		actx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = h.sink.Deliver(actx, env)
		cancel()
		if err == nil {
			return nil
		}
		class, ra := plugin.Classify(err)
		if !e.dlq.policy.retryable(class, plugin.Tags(err)) || n == e.dlq.policy.attempts {
			return err
		}
		if !sleep(ctx, e.dlq.policy.waitFor(n, ra, e.rand)) {
			return err
		}
	}
	return err
}

func (e *Engine) closeDLQ(ctx context.Context) {
	if e.dlq == nil {
		return
	}
	for _, h := range e.dlq.hops {
		if h.own {
			_ = h.sink.Close(ctx)
		}
	}
}

// writerSink writes one NDJSON line per message to a writer: the wire
// envelope (default) or the body.
type writerSink struct {
	name   string
	mu     sync.Mutex
	w      io.Writer
	body   bool
	closer io.Closer
}

func newWriterSink(name string, w io.Writer, format string) *writerSink {
	return &writerSink{name: name, w: w, body: format == "body"}
}

func newFileSink(name, path, format string) (*writerSink, error) {
	if path == "" {
		return nil, errors.New("file: path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s := newWriterSink(name, f, format)
	s.closer = f
	return s, nil
}

func (s *writerSink) Name() string { return s.name }

func (s *writerSink) Deliver(_ context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	var line []byte
	if s.body {
		line = append(append([]byte(nil), env.Body...), '\n')
		if len(env.Body) > 0 && env.Body[len(env.Body)-1] == '\n' {
			line = line[:len(line)-1]
		}
	} else {
		b, err := json.Marshal(env)
		if err != nil {
			return nil, plugin.Permanentf("encode: %v", err)
		}
		line = append(b, '\n')
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(line); err != nil {
		return nil, plugin.Wrap(plugin.Transient, []string{"io"}, err)
	}
	if f, ok := s.w.(*os.File); ok && s.closer != nil {
		if err := f.Sync(); err != nil {
			return nil, plugin.Wrap(plugin.Transient, []string{"io"}, err)
		}
	}
	return &plugin.Result{Endpoint: s.name}, nil
}

func (s *writerSink) Health(context.Context) error { return nil }

func (s *writerSink) Close(context.Context) error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

// ReadDLQ reads a file DLQ: one wire envelope per line, with its dlq
// metadata. Lines that are not envelopes are reported, not skipped.
func ReadDLQ(r io.Reader) ([]*envelope.Envelope, error) {
	dec := json.NewDecoder(r)
	var out []*envelope.Envelope
	for n := 1; ; n++ {
		var env envelope.Envelope
		err := dec.Decode(&env)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, fmt.Errorf("dead letter %d: %w", n, err)
		}
		out = append(out, &env)
	}
}

// InfoOf returns the dlq metadata of a dead letter read back from a file.
func InfoOf(env *envelope.Envelope) (DLQInfo, bool) {
	v, ok := env.Attribute("dlq")
	if !ok {
		return DLQInfo{}, false
	}
	if info, ok := v.(DLQInfo); ok {
		return info, true
	}
	b, err := json.Marshal(v)
	if err != nil {
		return DLQInfo{}, false
	}
	var info DLQInfo
	if err := json.Unmarshal(b, &info); err != nil || strings.TrimSpace(info.Reason) == "" {
		return DLQInfo{}, false
	}
	return info, true
}
