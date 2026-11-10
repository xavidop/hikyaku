package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xavidop/hikyaku/plugin"
)

// Tags of failures that are not a Kafka error code.
const (
	// tagThrottled marks a produce that ran out of time while the broker
	// throttled the client (a quota).
	tagThrottled = "kafka:THROTTLED"
	// tagTemplate marks a topic, key, header or body template that failed
	// to render.
	tagTemplate = "template"
)

var (
	errClosed     = errors.New("kafka client is closed")
	errNotStarted = errors.New("not started")
)

// permanent are the Kafka errors a retry cannot fix. UNKNOWN_TOPIC_OR_PARTITION
// is retriable for Kafka, but the producer only gives up on it once metadata
// settled on the topic not existing (topics are never auto-created).
var permanent = map[int16]bool{
	kerr.MessageTooLarge.Code:            true,
	kerr.RecordListTooLarge.Code:         true,
	kerr.InvalidTopicException.Code:      true,
	kerr.TopicAuthorizationFailed.Code:   true,
	kerr.ClusterAuthorizationFailed.Code: true,
	kerr.SaslAuthenticationFailed.Code:   true,
	kerr.InvalidRecord.Code:              true,
	kerr.UnknownTopicOrPartition.Code:    true,
	kerr.UnknownTopicID.Code:             true,
}

// kafkaTags are the tags of a Kafka error: kafka:<CODE>, plus the short
// kafka:NOT_LEADER that the design's examples use.
func kafkaTags(ke *kerr.Error) []string {
	tags := []string{"kafka:" + ke.Message}
	if ke == kerr.NotLeaderForPartition {
		tags = append(tags, "kafka:NOT_LEADER")
	}
	return tags
}

// classify turns a produce failure into a classified error. throttle is how
// much longer the broker throttles the client, zero when it does not.
func classify(err error, throttle time.Duration) error {
	if err == nil {
		return nil
	}
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return err
	}
	var ke *kerr.Error
	hasKafka := errors.As(err, &ke)
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kgo.ErrRecordTimeout):
		if throttle > 0 {
			return &plugin.Error{Class: plugin.RateLimited, RetryAfter: throttle, Tags: []string{tagThrottled}, Err: err}
		}
		tags := []string{plugin.TagTimeout}
		if hasKafka {
			tags = append(tags, kafkaTags(ke)...)
		}
		return plugin.Wrap(plugin.Timeout, tags, err)
	case errors.Is(err, context.Canceled), errors.Is(err, kgo.ErrClientClosed), errors.Is(err, errClosed):
		// The relay or the destination is shutting down.
		return plugin.Wrap(plugin.Transient, nil, err)
	case hasKafka:
		tags := kafkaTags(ke)
		switch {
		case ke == kerr.ThrottlingQuotaExceeded:
			return &plugin.Error{Class: plugin.RateLimited, RetryAfter: throttle, Tags: tags, Err: err}
		case permanent[ke.Code]:
			return plugin.Wrap(plugin.Permanent, tags, err)
		case ke.Retriable, ke == kerr.UnknownServerError:
			return plugin.Wrap(plugin.Transient, tags, err)
		}
		return plugin.Wrap(plugin.Permanent, tags, err)
	case authFailure(err): // the brokers' certificate does not verify
		return plugin.Wrap(plugin.Permanent, nil, err)
	case isNetwork(err):
		return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
	}
	return plugin.Wrap(plugin.Transient, nil, err)
}

// isNetwork reports whether err is a connection failure: a dial, a reset, a
// connection closed under a request, or a broker that hung up at once
// (usually TLS or SASL that the broker expects differently).
func isNetwork(err error) bool {
	var (
		oe  *net.OpError
		de  *net.DNSError
		eof *kgo.ErrFirstReadEOF
	)
	return errors.As(err, &oe) || errors.As(err, &de) || errors.As(err, &eof) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}

// authFailure reports whether an error is one that waiting will not fix:
// the credentials, the ACLs or the brokers' certificate are wrong.
func authFailure(err error) bool {
	for _, e := range []*kerr.Error{
		kerr.SaslAuthenticationFailed, kerr.IllegalSaslState, kerr.UnsupportedSaslMechanism,
		kerr.TopicAuthorizationFailed, kerr.GroupAuthorizationFailed, kerr.ClusterAuthorizationFailed,
	} {
		if errors.Is(err, e) {
			return true
		}
	}
	var cv *tls.CertificateVerificationError
	return errors.As(err, &cv)
}

// connWatch follows a client's connection attempts (dial, TLS, SASL). The
// client retries a refused connection on its own, and neither a consumer's
// polls nor a producer's records say why; this does. A connection that
// succeeds clears it.
type connWatch struct {
	mu sync.Mutex
	// refusals counts the attempts in a row that failed for good (see
	// authFailure); err is the last one, nil after a success.
	refusals int
	err      error
	// next is closed at the next refusal.
	next chan struct{}
	// onRefused, when set, hears of every refusal and how many in a row.
	onRefused func(n int, err error)
}

var _ kgo.HookBrokerConnect = (*connWatch)(nil)

func newConnWatch(onRefused func(int, error)) *connWatch {
	return &connWatch{next: make(chan struct{}), onRefused: onRefused}
}

// OnBrokerConnect records a connection attempt.
func (w *connWatch) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	w.mu.Lock()
	switch {
	case err == nil:
		w.refusals, w.err = 0, nil
		w.mu.Unlock()
		return
	case !authFailure(err):
		w.mu.Unlock()
		return
	}
	w.refusals++
	w.err = err
	close(w.next)
	w.next = make(chan struct{})
	n, cb := w.refusals, w.onRefused
	w.mu.Unlock()
	if cb != nil {
		cb(n, err)
	}
}

// refused returns a channel closed at the next refusal, and the refusal
// that stands (no connection succeeded since it), nil for none.
func (w *connWatch) refused() (<-chan struct{}, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.next, w.err
}

// throttle remembers until when a broker throttles the client.
type throttle struct{ until atomic.Int64 } // unix nanoseconds

var _ kgo.HookBrokerThrottle = (*throttle)(nil)

// OnBrokerThrottle records a throttle the broker imposed.
func (t *throttle) OnBrokerThrottle(_ kgo.BrokerMetadata, d time.Duration, _ bool) {
	if d <= 0 {
		return
	}
	until := time.Now().Add(d).UnixNano()
	for {
		cur := t.until.Load()
		if until <= cur || t.until.CompareAndSwap(cur, until) {
			return
		}
	}
}

// remaining is how much longer the client is throttled.
func (t *throttle) remaining() time.Duration {
	return max(time.Until(time.Unix(0, t.until.Load())), 0)
}

// ctxError classifies a delivery that its context ended: a deadline is a
// timeout (rate limited while the broker throttles), a cancellation is the
// relay shutting down.
func ctxError(ctx context.Context, what string, throttle time.Duration) error {
	return classify(fmt.Errorf("%s: %w", what, ctx.Err()), throttle)
}
