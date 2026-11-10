// Package fault is a fault-injecting sink: a type: fault destination that
// fails, slows down or times out as configured, and Wrap, which injects the
// same faults in front of a real sink (hikyaku chaos). The resilience suite
// drives retries, breakers, fallbacks and the DLQ with it.
package fault

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// Register makes type: fault available.
func Register() {
	plugin.RegisterSink("fault", func(_ context.Context, s plugin.Spec) (plugin.Sink, error) {
		ep, ok := s.Config.(*config.Endpoint)
		if !ok || ep == nil {
			return nil, fmt.Errorf("destination %s: missing config", s.Name)
		}
		var f config.Fault
		if ep.Fault != nil {
			f = *ep.Fault
		}
		return New(s.Name, f)
	})
}

// Injector decides, attempt by attempt, whether and how to fail.
type Injector struct {
	f       config.Fault
	mu      sync.Mutex
	rnd     *rand.Rand
	attempt atomic.Int64
}

// NewInjector validates a fault spec.
func NewInjector(f config.Fault) (*Injector, error) {
	switch f.Error {
	case "", "status", "connect", "timeout":
	default:
		return nil, fmt.Errorf("fault.error %q: use status, connect or timeout", f.Error)
	}
	if f.Status == 0 {
		f.Status = 503
	}
	seed := uint64(f.Seed)
	if f.Seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	return &Injector{f: f, rnd: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}, nil
}

// Fault is the spec the injector applies.
func (in *Injector) Fault() config.Fault { return in.f }

// Before runs before an attempt: the latency, then the failure, if any.
func (in *Injector) Before(ctx context.Context) error {
	if d := in.f.Latency.Std(); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, ctx.Err())
		}
	}
	n := in.attempt.Add(1)
	fail := n <= int64(in.f.FailFirst)
	if !fail && in.f.Fail > 0 {
		in.mu.Lock()
		fail = in.rnd.Float64() < float64(in.f.Fail)
		in.mu.Unlock()
	}
	if !fail {
		return nil
	}
	switch in.f.Error {
	case "connect":
		return plugin.Errorf(plugin.Transient, []string{plugin.TagConnect}, "fault: connection refused (injected)")
	case "timeout":
		<-ctx.Done()
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, fmt.Errorf("fault: timed out (injected): %w", ctx.Err()))
	}
	return plugin.StatusError(in.f.Status, in.f.RetryAfter.Std(), "injected")
}

// Attempts is how many attempts the injector has seen.
func (in *Injector) Attempts() int64 { return in.attempt.Load() }

// Sink is a type: fault destination: after its faults, it accepts the
// message and keeps a count (and the last few messages) for tests.
type Sink struct {
	name string
	in   *Injector

	mu        sync.Mutex
	delivered []*envelope.Envelope
}

// New builds a fault sink.
func New(name string, f config.Fault) (*Sink, error) {
	in, err := NewInjector(f)
	if err != nil {
		return nil, fmt.Errorf("destination %s: %w", name, err)
	}
	return &Sink{name: name, in: in}, nil
}

// keep bounds the messages a fault sink remembers.
const keep = 1000

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver applies the faults, then accepts.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if err := s.in.Before(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delivered = append(s.delivered, env.Clone())
	if len(s.delivered) > keep {
		s.delivered = s.delivered[len(s.delivered)-keep:]
	}
	return &plugin.Result{Status: 202, Endpoint: "fault:" + s.name}, nil
}

// Delivered returns the messages accepted (the last 1000).
func (s *Sink) Delivered() []*envelope.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*envelope.Envelope(nil), s.delivered...)
}

// Attempts is how many attempts reached the sink.
func (s *Sink) Attempts() int64 { return s.in.Attempts() }

// Health is fine.
func (s *Sink) Health(context.Context) error { return nil }

// Close does nothing.
func (s *Sink) Close(context.Context) error { return nil }

// Wrap injects faults in front of a real sink.
func Wrap(inner plugin.Sink, f config.Fault) (plugin.Sink, error) {
	in, err := NewInjector(f)
	if err != nil {
		return nil, err
	}
	return &wrapped{inner: inner, in: in}, nil
}

type wrapped struct {
	inner plugin.Sink
	in    *Injector
}

func (w *wrapped) Name() string { return w.inner.Name() }

func (w *wrapped) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if err := w.in.Before(ctx); err != nil {
		return nil, err
	}
	return w.inner.Deliver(ctx, env)
}

func (w *wrapped) Health(ctx context.Context) error { return w.inner.Health(ctx) }

// Start starts the wrapped sink when it serves something of its own.
func (w *wrapped) Start(ctx context.Context) error {
	if s, ok := w.inner.(plugin.Starter); ok {
		return s.Start(ctx)
	}
	return nil
}
func (w *wrapped) Close(ctx context.Context) error { return w.inner.Close(ctx) }
