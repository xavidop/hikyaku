package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin"
	"github.com/xavidop/hikyaku/plugin"
)

// TickerOptions are the settings of a ticker source.
type TickerOptions struct {
	Interval  string `yaml:"interval,omitempty" jsonschema:"pattern=^[0-9.]+(ns|us|µs|ms|s|m|h)$" jsonschema_description:"Time between messages, a Go duration (default 1s)."`
	Count     int    `yaml:"count,omitempty" jsonschema:"minimum=0" jsonschema_description:"Emit this many messages, then stop once all are settled; 0 runs until hikyaku stops."`
	SettleLog string `yaml:"settle_log,omitempty" jsonschema_description:"File each settle is appended to as \"<n> <id> <final>\", for tests."`
}

// Ticker emits {"n":i} every interval. Each message stays pending until
// hikyaku settles it: final forgets it, not final emits it again (same ID,
// same body) at the next tick, before any new message.
type Ticker struct {
	name      string
	interval  time.Duration
	count     int
	ack       string
	settleLog string
	log       *slog.Logger

	mu      sync.Mutex
	emitted int
	ids     map[int]string // n → message ID, until settled final
	pending map[int]bool   // emitted, not settled
	retry   []int          // settled as failed, to emit again
}

func newTicker(_ context.Context, spec plugin.Spec) (plugin.Source, error) {
	var o TickerOptions
	if err := spec.Decode(&o); err != nil {
		return nil, err
	}
	interval := time.Second
	if o.Interval != "" {
		d, err := time.ParseDuration(o.Interval)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("ticker %s: options.interval %q is not a positive duration", spec.Name, o.Interval)
		}
		interval = d
	}
	return &Ticker{
		name:      spec.Name,
		interval:  interval,
		count:     o.Count,
		ack:       grpcplugin.AckMode(spec),
		settleLog: o.SettleLog,
		log:       spec.Env.Log(),
		ids:       map[int]string{},
		pending:   map[int]bool{},
	}, nil
}

// Name is the source name.
func (t *Ticker) Name() string { return t.name }

// Start emits until ctx ends or, with count, until every message is
// settled.
func (t *Ticker) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	tk := time.NewTicker(t.interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tk.C:
		}
		env, n, finished := t.next()
		if finished {
			return nil
		}
		if env == nil {
			continue
		}
		select {
		case out <- env:
		case <-ctx.Done():
			t.requeue(n) // never handed over: emit it again next time
			return nil
		}
	}
}

func (t *Ticker) requeue(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pending, n)
	t.retry = append(t.retry, n)
}

// next is the message to emit: a failed one first, else a new one.
func (t *Ticker) next() (env *envelope.Envelope, n int, finished bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case len(t.retry) > 0:
		n, t.retry = t.retry[0], t.retry[1:]
	case t.count == 0 || t.emitted < t.count:
		n = t.emitted
		t.emitted++
		t.ids[n] = envelope.NewID()
	default:
		return nil, 0, len(t.pending) == 0
	}
	t.pending[n] = true
	env = envelope.New(t.name, "ticker", []byte(`{"n":`+strconv.Itoa(n)+`}`))
	env.ID = t.ids[n]
	env.Subject = t.name
	env.ContentType = "application/json"
	env.SetAttribute("ticker.n", n)
	env.SetAttribute("ordering_key", t.name)
	plugin.OnSettle(env, t.ack, func(_ context.Context, final bool) { t.settled(n, final) })
	return env, n, false
}

// settled records hikyaku's outcome for message n.
func (t *Ticker) settled(n int, final bool) {
	t.mu.Lock()
	id := t.ids[n]
	delete(t.pending, n)
	if final {
		delete(t.ids, n)
	} else {
		t.retry = append(t.retry, n)
	}
	t.mu.Unlock()
	t.log.Debug("ticker message settled", "n", n, "id", id, "final", final)
	if t.settleLog != "" {
		if err := appendLine(t.settleLog, fmt.Sprintf("%d %s %t", n, id, final)); err != nil {
			t.log.Warn("settle log", "error", err)
		}
	}
}

// Health is always fine: the ticker needs nothing.
func (t *Ticker) Health(context.Context) error { return nil }

func appendLine(path, line string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(line + "\n"); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
