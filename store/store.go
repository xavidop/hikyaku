// Package store defines the shared-state backends of a relay: the routing
// history the AI router reads and writes (HistoryStore), and the small
// key-value operations behind the decision cache, poll dedupe, idempotency
// and retry-budget counters (KV). The memory implementation here serves local
// runs and tests; the Redis implementation is its own module, stores/redis,
// and is what makes several replicas share one state.
package store

import (
	"context"
	"errors"
	"time"
)

// Outcomes recorded on a CallRecord.
const (
	OutcomeDelivered     = "delivered"      // the chosen destinations took it on their primaries
	OutcomeFallback      = "fallback"       // delivered, but a destination fallback served at least one
	OutcomeDLQ           = "dlq"            // a required destination failed; the message is in the DLQ
	OutcomeLowConfidence = "low_confidence" // the model was not sure; on_low_confidence took it
)

// Answer is one candidate's part of a routing decision.
type Answer struct {
	Candidate   string  `json:"candidate"`
	Probability float64 `json:"p"`
	Chosen      bool    `json:"chosen,omitempty"`
	// Margin is the gap between the top two options of a single choice; set on
	// the chosen answer only.
	Margin float64 `json:"margin,omitempty"`
}

// Feedback is a correction of a past decision, from a human (the admin API,
// `hikyaku history correct`) or a system (a destination that answered with
// X-Hikyaku-Reroute).
type Feedback struct {
	Correct []string  `json:"correct"`
	Note    string    `json:"note,omitempty"`
	Source  string    `json:"source,omitempty"` // api | cli | destination
	At      time.Time `json:"at"`
}

// CallRecord is one routed call, appended after delivery so the outcome is
// known.
type CallRecord struct {
	MsgID       string    `json:"msg_id"`
	At          time.Time `json:"at"`
	Route       string    `json:"route"`
	Key         string    `json:"key"`             // the rendered history.key
	State       string    `json:"state,omitempty"` // the redacted state, a short summary, or empty
	Decision    []Answer  `json:"decision"`
	Model       string    `json:"model"`        // which model in the fallback chain answered
	DeliveredTo []string  `json:"delivered_to"` // after destination fallbacks
	Outcome     string    `json:"outcome"`      // delivered | fallback | dlq | low_confidence
	Feedback    *Feedback `json:"feedback,omitempty"`
}

// Chosen returns the candidates the decision picked, in decision order.
func (r CallRecord) Chosen() []string {
	var out []string
	for _, a := range r.Decision {
		if a.Chosen {
			out = append(out, a.Candidate)
		}
	}
	return out
}

// Delivered reports whether the call reached its destinations (on primaries
// or fallbacks).
func (r CallRecord) Delivered() bool {
	return r.Outcome == OutcomeDelivered || r.Outcome == OutcomeFallback
}

// Window bounds the history sent to the model.
type Window struct {
	MaxEntries int           // newest N records (0 = 10)
	MaxAge     time.Duration // and no older than this (0 = no age limit)
}

// DefaultMaxEntries is the window size when none is set.
const DefaultMaxEntries = 10

// Entries is MaxEntries with the default applied.
func (w Window) Entries() int {
	if w.MaxEntries <= 0 {
		return DefaultMaxEntries
	}
	return w.MaxEntries
}

// AppendOptions says how long a record is kept and how many are kept per key.
type AppendOptions struct {
	TTL  time.Duration // mandatory: history is never kept forever
	Keep int           // records kept per key (the Redis list is trimmed to this); 0 = 2×DefaultMaxEntries
}

// KeepOrDefault is Keep with the default applied.
func (o AppendOptions) KeepOrDefault() int {
	if o.Keep <= 0 {
		return 2 * DefaultMaxEntries
	}
	return o.Keep
}

// ErrNotFound is returned by Feedback for an unknown (or expired) message ID.
var ErrNotFound = errors.New("store: not found")

// ErrNoTTL is returned by Append when the TTL is zero: history must expire.
var ErrNoTTL = errors.New("store: history records need a ttl")

// ErrUnavailable is returned without touching the backend while a store's
// circuit breaker is open, so a dead Redis is not hammered.
var ErrUnavailable = errors.New("store: unavailable (circuit open)")

// HistoryStore keeps the routing history of every (route, key).
type HistoryStore interface {
	// Append adds a record as the newest of its (route, key).
	Append(ctx context.Context, route, key string, rec CallRecord, o AppendOptions) error
	// Recent returns the newest records within the window, oldest first
	// (newest last), the order the model reads them in.
	Recent(ctx context.Context, route, key string, w Window) ([]CallRecord, error)
	// Feedback attaches a correction to the record of a message.
	Feedback(ctx context.Context, msgID string, fb Feedback) (CallRecord, error)
	// Forget deletes the history of a key (right to be forgotten).
	Forget(ctx context.Context, route, key string) error
	// Records calls fn for every record of a route (every route when route is
	// empty) received at or after since, oldest key first. It backs
	// `hikyaku history export`.
	Records(ctx context.Context, route string, since time.Time, fn func(CallRecord) error) error
}

// KV is the small key-value surface behind the cache, dedupe, idempotency and
// counters.
type KV interface {
	// Get returns a value and whether it exists.
	Get(ctx context.Context, k string) ([]byte, bool, error)
	// SetNX stores v under k only if k does not exist, reporting whether it did
	// store it.
	SetNX(ctx context.Context, k string, v []byte, ttl time.Duration) (bool, error)
	// Incr adds one to the counter k (created at 0 with the ttl) and returns
	// the new value.
	Incr(ctx context.Context, k string, ttl time.Duration) (int64, error)
}

// Deleter is a KV that can remove a key. Dedupe uses it to forget the key
// of a message that failed, so the message's retry is not taken for a
// duplicate. The memory and redis stores implement it.
type Deleter interface {
	Delete(ctx context.Context, k string) error
}

// Store is one shared-state backend, declared under stores: in the spec.
type Store interface {
	HistoryStore
	KV
	// Name is the store's name in the spec.
	Name() string
	// Health reports whether the backend answers.
	Health(ctx context.Context) error
	// Close releases the backend's connections.
	Close() error
}
