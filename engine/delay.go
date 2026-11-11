package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// Delayed delivery (destination deliver_after: and deliver_at:). A route's message
// for such a destination is held until it is due, then delivered through
// the destination's normal path: its batch, retries and fallbacks.
//
// The due time is the deliver_at template's RFC 3339 time when it renders
// one, else the time the message was received plus deliver_after. Both are
// absolute, so a message that waits again after a restart (it is still in
// the disk buffer) does not start its wait over.
//
// A held message gives its worker slot back, so held messages do not stop
// others. Only messages a route sends are held: hikyaku send, dlq replay
// --to and chaos deliver at once, and so does a fallback hop.
//
// At shutdown a held message is not dead-lettered. It fails (a nack), which
// leaves it in the disk buffer for the next start; with the memory buffer
// the source has to send it again.

// errHeld is the failure of a message a delay still held when the relay
// shut down.
var errHeld = errors.New("the relay shut down while a delay held the message")

// TagDeliverAt tags a deliver_at template that did not render a time.
const TagDeliverAt = "deliver_at"

func (d *destination) delayed() bool { return d.delay > 0 || d.deliverAt != nil }

// dueAt is when env is due at d.
func (d *destination) dueAt(env *envelope.Envelope, now time.Time) (time.Time, error) {
	if d.deliverAt != nil {
		s, err := d.deliverAt.Render(env)
		if err != nil {
			return time.Time{}, err
		}
		if s = strings.TrimSpace(s); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return time.Time{}, fmt.Errorf("deliver_at %q is not an RFC 3339 time such as 2026-12-24T09:00:00Z", s)
			}
			return t, nil
		}
	}
	if d.delay <= 0 {
		return now, nil
	}
	received := env.ReceivedAt
	if received.IsZero() {
		received = now
	}
	return received.Add(d.delay), nil
}

// hold waits until env is due at d. ok is false when the message is not to
// be delivered now: the report says why.
func (e *Engine) hold(ctx context.Context, d *destination, env *envelope.Envelope) (rep DeliveryReport, ok bool) {
	fail := func(err error) (DeliveryReport, bool) {
		return DeliveryReport{Destination: d.name, Required: d.required, Err: err, Error: err.Error()}, false
	}
	due, err := d.dueAt(env, e.now())
	if err != nil {
		return fail(plugin.Wrap(plugin.Permanent, []string{TagDeliverAt}, fmt.Errorf("destination %s: %w", d.name, err)))
	}
	wait := due.Sub(e.now())
	if wait <= 0 {
		return rep, true
	}
	if e.m != nil {
		e.m.Delayed.WithLabelValues(d.name).Inc()
		defer e.m.Delayed.WithLabelValues(d.name).Dec()
	}
	if holdsSlot(ctx) {
		<-e.sem
		defer func() { e.sem <- struct{}{} }()
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
		return rep, true
	case <-e.stopping:
	case <-ctx.Done():
	}
	return fail(fmt.Errorf("destination %s: %w", d.name, errHeld))
}
