package gcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// defaultStableAfter: a Receive that ran this long resets the failure count
// even if it delivered nothing.
const defaultStableAfter = 30 * time.Second

var errNotStarted = errors.New("not started")

// PubSubSource is a pubsub source: a StreamingPull subscriber.
//
// Its Receive callback hands each message over and then waits until the
// relay settles it, so prefetch (MaxOutstandingMessages) bounds the
// messages in flight and the messages of one ordering key reach the relay
// one at a time. The client extends the ack deadline of every message it
// holds, up to an hour.
//
// When Start's context ends, Receive stops pulling but keeps running: the
// client's default (graceful) shutdown keeps extending the deadlines of the
// messages in flight and sends the ack or nack of each one as the relay
// settles it, and Receive returns once none is left. Start returns at once,
// because the engine finishes the messages in flight only after every
// source's Start has returned. Close waits for that Receive, then closes the
// client. The stream stays open while it drains, so Pub/Sub may push it a
// few messages (or a nacked one back) that it no longer reads: those are
// redelivered once the subscription's ack deadline passes.
type PubSubSource struct {
	name     string
	kind     string
	sub      string // projects/<project>/subscriptions/<id>
	subID    string
	ack      string
	maxBytes int64
	rc       reconnectOptions
	client   *pubsub.Client
	receiver *pubsub.Subscriber
	log      *slog.Logger

	stableAfter time.Duration
	rnd         func() float64

	mu       sync.Mutex
	status   error         // why Receive is failing, nil while it runs
	recvDone chan struct{} // closed once the last Receive returned
	closed   bool
}

func newPubSubSource(ctx context.Context, spec plugin.Spec) (*PubSubSource, error) {
	o, err := parsePubSubSource(spec)
	if err != nil {
		return nil, err
	}
	client, err := pubsub.NewClient(ctx, o.client.project, o.client.opts...)
	if err != nil {
		return nil, fmt.Errorf("source %s: pubsub client: %w", spec.Name, err)
	}
	receiver := client.Subscriber(o.subscription)
	receiver.ReceiveSettings.MaxOutstandingMessages = o.prefetch
	receiver.ReceiveSettings.MaxExtension = defaultMaxExtension
	// nil ShutdownOptions is the graceful shutdown the source relies on:
	// wait, without a timeout, for every message in flight to be settled.
	receiver.ReceiveSettings.ShutdownOptions = nil
	return &PubSubSource{
		name: spec.Name, kind: spec.Kind, sub: o.subscription, subID: receiver.ID(),
		ack: o.ack, maxBytes: o.maxBytes, rc: o.reconnect,
		client: client, receiver: receiver, log: spec.Env.Log(),
		stableAfter: defaultStableAfter, rnd: rand.Float64, status: errNotStarted,
	}, nil
}

// Name is the source name.
func (s *PubSubSource) Name() string { return s.name }

// Health is why Receive is failing, else whether the subscription exists
// (credentials that may consume it but not read it, such as
// roles/pubsub.subscriber, count as healthy).
func (s *PubSubSource) Health(ctx context.Context) error {
	s.mu.Lock()
	st := s.status
	s.mu.Unlock()
	if st != nil && !errors.Is(st, errNotStarted) {
		return st
	}
	_, err := s.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: s.sub})
	return existence("subscription", s.sub, err)
}

// Start receives until ctx ends. Receive retries its stream on transient
// failures itself; when it gives up (the subscription is missing, the
// credentials are refused), Start runs it again after a backoff, and fails
// after reconnect.max_attempts failures in a row.
func (s *PubSubSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	failures := 0
	for {
		done := make(chan struct{})
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return errors.New("pubsub source is closed")
		}
		s.recvDone, s.status = done, nil
		s.mu.Unlock()

		var delivered atomic.Int64
		var err error
		start := time.Now()
		go func() {
			defer close(done)
			err = s.receiver.Receive(ctx, func(cctx context.Context, m *pubsub.Message) {
				s.handle(cctx, out, m, &delivered)
			})
		}()
		select {
		case <-ctx.Done():
			// Receive drains the messages in flight; Close waits for it.
			return nil
		case <-done:
		}
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			err = errors.New("receive ended")
		}
		err = fmt.Errorf("receive %s: %w", s.sub, err)
		s.setStatus(err)
		if delivered.Load() > 0 || time.Since(start) >= s.stableAfter {
			failures = 0
		} else {
			failures++
			if s.rc.maxAttempts > 0 && failures >= s.rc.maxAttempts {
				return fmt.Errorf("giving up after %d failed receives in a row: %w", failures, err)
			}
		}
		wait := backoff.Delay(s.rc.backoff, s.rc.maxBackoff, s.rc.jitter, max(failures, 1), s.rnd)
		s.log.Warn("pubsub source receiving again", "source", s.name, "subscription", s.sub,
			"failures", failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			return nil
		}
	}
}

// handle hands one message to the relay and waits until it is settled, or
// until Receive stops. ctx is the callback's context, which ends with
// Start's or when Receive fails.
func (s *PubSubSource) handle(ctx context.Context, out chan<- *envelope.Envelope, m *pubsub.Message, delivered *atomic.Int64) {
	if ctx.Err() != nil {
		m.Nack()
		return
	}
	if int64(len(m.Data)) > s.maxBytes {
		// A nack would bring it back forever.
		s.log.Error("pubsub message over max_body_bytes; acked and dropped", "source", s.name,
			"subscription", s.sub, "message_id", m.ID, "bytes", len(m.Data), "max_body_bytes", s.maxBytes)
		m.Ack()
		return
	}
	env := s.envelope(m)
	settled := make(chan struct{})
	plugin.OnSettle(env, s.ack, func(_ context.Context, final bool) {
		if final {
			m.Ack()
		} else {
			m.Nack()
		}
		close(settled)
	})
	select {
	case out <- env:
		delivered.Add(1)
	case <-ctx.Done():
		// The relay never saw it: let Pub/Sub redeliver it.
		m.Nack()
		return
	}
	select {
	case <-settled:
	case <-ctx.Done():
		// Shutdown: Receive keeps the message's lease and still sends its
		// ack or nack when the relay settles it.
	}
}

// envelope turns a message into an envelope.
func (s *PubSubSource) envelope(m *pubsub.Message) *envelope.Envelope {
	env := envelope.New(s.name, s.kind, m.Data)
	if key, ok := lookupFold(m.Attributes, attrIdempotencyKey); ok && key != "" {
		env.ID = key
	} else if m.ID != "" {
		env.ID = m.ID
	}
	env.Subject = s.subID
	if v, ok := lookupFold(m.Attributes, attrSubject); ok && v != "" {
		env.Subject = v
	}
	if v, ok := lookupFold(m.Attributes, attrContentType); ok && v != "" {
		env.ContentType = v
	} else {
		env.ContentType = detectContentType(m.Data)
	}
	for k, v := range m.Attributes {
		if equalFold(k, attrIdempotencyKey) || hasPrefixFold(k, internalPrefix) {
			continue
		}
		env.Headers[k] = []string{v}
	}
	env.Attributes["pubsub.message_id"] = m.ID
	env.Attributes["pubsub.subscription"] = s.sub
	if !m.PublishTime.IsZero() {
		env.Attributes["pubsub.publish_time"] = m.PublishTime.UTC().Format(time.RFC3339Nano)
	}
	if m.OrderingKey != "" {
		env.Attributes["pubsub.ordering_key"] = m.OrderingKey
		env.Attributes["ordering_key"] = m.OrderingKey
	}
	if m.DeliveryAttempt != nil {
		env.Attributes["pubsub.delivery_attempt"] = *m.DeliveryAttempt
	}
	return env
}

func (s *PubSubSource) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

// Close waits until the last Receive has sent the acks and nacks of the
// messages in flight (the engine calls Close once they are all settled),
// then closes the client. When ctx ends first, the messages still unsettled
// are redelivered once their ack deadline passes.
func (s *PubSubSource) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	done := s.recvDone
	s.mu.Unlock()
	var err error
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			err = fmt.Errorf("pubsub source %s: messages still in flight at close: %w", s.name, ctx.Err())
		}
	}
	if cerr := s.client.Close(); cerr != nil && err == nil && !strings.Contains(cerr.Error(), "is closing") {
		err = cerr
	}
	return err
}
