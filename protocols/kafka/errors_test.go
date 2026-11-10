package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xavidop/hikyaku/plugin"
)

func TestClassify(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	cases := []struct {
		name       string
		err        error
		throttle   time.Duration
		class      plugin.Class
		tags       []string
		retryAfter time.Duration
	}{
		{"not leader", fmt.Errorf("produce: %w", kerr.NotLeaderForPartition), 0, plugin.Transient,
			[]string{"kafka:NOT_LEADER_FOR_PARTITION", "kafka:NOT_LEADER"}, 0},
		{"leader not available", kerr.LeaderNotAvailable, 0, plugin.Transient, []string{"kafka:LEADER_NOT_AVAILABLE"}, 0},
		{"not enough replicas", kerr.NotEnoughReplicas, 0, plugin.Transient, []string{"kafka:NOT_ENOUGH_REPLICAS"}, 0},
		{"unknown server error", kerr.UnknownServerError, 0, plugin.Transient, []string{"kafka:UNKNOWN_SERVER_ERROR"}, 0},
		{"message too large", fmt.Errorf("%w (uncompressed_bytes=9)", kerr.MessageTooLarge), 0, plugin.Permanent, []string{"kafka:MESSAGE_TOO_LARGE"}, 0},
		{"invalid topic", kerr.InvalidTopicException, 0, plugin.Permanent, []string{"kafka:INVALID_TOPIC_EXCEPTION"}, 0},
		{"topic authorization", kerr.TopicAuthorizationFailed, 0, plugin.Permanent, []string{"kafka:TOPIC_AUTHORIZATION_FAILED"}, 0},
		{"cluster authorization", kerr.ClusterAuthorizationFailed, 0, plugin.Permanent, []string{"kafka:CLUSTER_AUTHORIZATION_FAILED"}, 0},
		{"sasl", fmt.Errorf("%w: bad password", kerr.SaslAuthenticationFailed), 0, plugin.Permanent, []string{"kafka:SASL_AUTHENTICATION_FAILED"}, 0},
		{"invalid record", kerr.InvalidRecord, 0, plugin.Permanent, []string{"kafka:INVALID_RECORD"}, 0},
		{"unknown topic", kerr.UnknownTopicOrPartition, 0, plugin.Permanent, []string{"kafka:UNKNOWN_TOPIC_OR_PARTITION"}, 0},
		{"other non-retriable", kerr.InvalidTimestamp, 0, plugin.Permanent, []string{"kafka:INVALID_TIMESTAMP"}, 0},
		{"throttling quota", kerr.ThrottlingQuotaExceeded, 2 * time.Second, plugin.RateLimited,
			[]string{"kafka:THROTTLING_QUOTA_EXCEEDED", plugin.Tag429}, 2 * time.Second},
		{"deadline", fmt.Errorf("produce: %w", context.DeadlineExceeded), 0, plugin.Timeout, []string{plugin.TagTimeout}, 0},
		{"deadline while throttled", context.DeadlineExceeded, time.Second, plugin.RateLimited,
			[]string{tagThrottled, plugin.Tag429}, time.Second},
		{"record timeout with the last error", fmt.Errorf("%w, last err: %w", kgo.ErrRecordTimeout, kerr.LeaderNotAvailable), 0,
			plugin.Timeout, []string{plugin.TagTimeout, "kafka:LEADER_NOT_AVAILABLE"}, 0},
		{"cancelled", context.Canceled, 0, plugin.Transient, nil, 0},
		{"client closed", kgo.ErrClientClosed, 0, plugin.Transient, nil, 0},
		{"closed", errClosed, 0, plugin.Transient, nil, 0},
		{"dial", dial, 0, plugin.Transient, []string{plugin.TagConnect}, 0},
		{"certificate", &net.OpError{Op: "remote error", Err: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}}, 0,
			plugin.Permanent, nil, 0},
		{"dns", &net.DNSError{Err: "no such host", Name: "kafka"}, 0, plugin.Transient, []string{plugin.TagConnect}, 0},
		{"eof", fmt.Errorf("read: %w", io.EOF), 0, plugin.Transient, []string{plugin.TagConnect}, 0},
		{"reset", syscall.ECONNRESET, 0, plugin.Transient, []string{plugin.TagConnect}, 0},
		{"unknown", errors.New("odd"), 0, plugin.Transient, nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := classify(tc.err, tc.throttle)
			class, retryAfter := plugin.Classify(err)
			assert.Equal(t, tc.class, class)
			assert.Equal(t, tc.tags, plugin.Tags(err))
			assert.Equal(t, tc.retryAfter, retryAfter)
			assert.ErrorIs(t, err, tc.err, "the cause stays reachable")
		})
	}
	assert.NoError(t, classify(nil, 0))
	already := plugin.Permanentf("bad")
	assert.Same(t, already, classify(already, 0), "a classified error is kept")
}

func TestAuthFailure(t *testing.T) {
	assert.True(t, authFailure(&kgo.ErrGroupSession{Err: fmt.Errorf("%w: wrong password", kerr.SaslAuthenticationFailed)}))
	assert.True(t, authFailure(kerr.GroupAuthorizationFailed))
	assert.True(t, authFailure(kerr.TopicAuthorizationFailed))
	assert.False(t, authFailure(&kgo.ErrGroupSession{Err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}}))
	assert.False(t, authFailure(kerr.NotLeaderForPartition))
	assert.True(t, authFailure(&net.OpError{Op: "dial", Err: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}}),
		"a certificate that does not verify will not verify later")
}

func TestConnWatch(t *testing.T) {
	var seen []int
	w := newConnWatch(func(n int, err error) {
		seen = append(seen, n)
		assert.ErrorIs(t, err, kerr.SaslAuthenticationFailed)
	})
	next, err := w.refused()
	assert.NoError(t, err)
	w.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, syscall.ECONNREFUSED) // a broker that is down: not a refusal
	_, err = w.refused()
	assert.NoError(t, err)
	select {
	case <-next:
		t.Fatal("a dial failure is not a refusal")
	default:
	}
	refusal := fmt.Errorf("%w: wrong password", kerr.SaslAuthenticationFailed)
	w.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, refusal)
	w.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, refusal)
	<-next
	_, err = w.refused()
	assert.ErrorIs(t, err, kerr.SaslAuthenticationFailed)
	assert.Equal(t, []int{1, 2}, seen, "refusals in a row are counted")
	w.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, nil)
	_, err = w.refused()
	assert.NoError(t, err, "a connection that succeeds clears the refusal")
	w.OnBrokerConnect(kgo.BrokerMetadata{}, 0, nil, refusal)
	assert.Equal(t, []int{1, 2, 1}, seen, "and restarts the count")
}

func TestThrottle(t *testing.T) {
	var th throttle
	assert.Zero(t, th.remaining())
	th.OnBrokerThrottle(kgo.BrokerMetadata{}, 0, false)
	assert.Zero(t, th.remaining())
	th.OnBrokerThrottle(kgo.BrokerMetadata{}, time.Hour, false)
	th.OnBrokerThrottle(kgo.BrokerMetadata{}, time.Minute, true) // a shorter one does not shorten it
	assert.Greater(t, th.remaining(), 59*time.Minute)
}

func TestCtxError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()
	err := ctxError(ctx, "produce", 0)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)
	assert.ErrorContains(t, err, "produce: context deadline exceeded")
}
