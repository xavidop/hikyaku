package nats

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/xavidop/hikyaku/plugin"
)

func TestClassify(t *testing.T) {
	permission := fmt.Errorf("%w: %s", nats.ErrPermissionViolation, `Permissions Violation for Publish to "secret"`)
	cases := []struct {
		name  string
		err   error
		class plugin.Class
		tags  []string
	}{
		{"deadline", context.DeadlineExceeded, plugin.Timeout, []string{plugin.TagTimeout}},
		{"nats timeout", nats.ErrTimeout, plugin.Timeout, []string{plugin.TagTimeout}},
		{"async ack timeout", jetstream.ErrAsyncPublishTimeout, plugin.Timeout, []string{plugin.TagTimeout}},
		{"canceled", context.Canceled, plugin.Transient, nil},
		{"no servers", nats.ErrNoServers, plugin.Transient, []string{plugin.TagConnect}},
		{"closed", nats.ErrConnectionClosed, plugin.Transient, []string{plugin.TagConnect}},
		{"reconnecting", fmt.Errorf("publish: %w", nats.ErrReconnectBufExceeded), plugin.Transient, []string{plugin.TagConnect}},
		{"draining", nats.ErrConnectionDraining, plugin.Transient, []string{plugin.TagConnect}},
		{"js closed", jetstream.ErrConnectionClosed, plugin.Transient, []string{plugin.TagConnect}},
		{"not connected", errNotConnected, plugin.Transient, []string{plugin.TagConnect}},
		{"permission", permission, plugin.Permanent, []string{tagPermissions}},
		{"no stream", jetstream.ErrNoStreamResponse, plugin.Permanent, []string{tagNoResponders}},
		{"no responders", nats.ErrNoResponders, plugin.Permanent, []string{tagNoResponders}},
		{"max payload", nats.ErrMaxPayload, plugin.Permanent, []string{tagMaxPayload}},
		{"bad subject", nats.ErrBadSubject, plugin.Permanent, nil},
		{"stalled", jetstream.ErrTooManyStalledMsgs, plugin.RateLimited, []string{plugin.Tag429}},
		{"stream mismatch", fmt.Errorf("nats: %w", &jetstream.APIError{Code: 400, ErrorCode: 10060, Description: "expected stream does not match"}),
			plugin.Permanent, []string{"jetstream:10060", "400", plugin.Tag4xx}},
		{"unavailable", &jetstream.APIError{Code: 503, ErrorCode: 10008, Description: "jetstream temporarily unavailable"},
			plugin.Transient, []string{"jetstream:10008", "503", plugin.Tag5xx}},
		{"throttled", &jetstream.APIError{Code: 429, ErrorCode: 10077}, plugin.RateLimited, []string{"jetstream:10077", "429", plugin.Tag4xx}},
		{"api timeout", &jetstream.APIError{Code: 408, ErrorCode: 10001}, plugin.Timeout, []string{"jetstream:10001", "408", plugin.Tag4xx, plugin.TagTimeout}},
		{"stream not found", jetstream.ErrStreamNotFound, plugin.Permanent, []string{"jetstream:10059", "404", plugin.Tag4xx}},
		{"no code", &jetstream.APIError{Description: "odd"}, plugin.Transient, nil},
		{"unknown", errors.New("boom"), plugin.Transient, nil},
		{"eof", io.EOF, plugin.Transient, []string{plugin.TagConnect}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := classify(tc.err)
			class, _ := plugin.Classify(err)
			assert.Equal(t, tc.class, class)
			assert.ElementsMatch(t, tc.tags, plugin.Tags(err))
			assert.ErrorIs(t, err, tc.err)
		})
	}
	assert.NoError(t, classify(nil))
	already := plugin.Permanentf("bad")
	assert.Same(t, already, classify(already))
}

func TestDeliverError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	<-ctx.Done()
	// A failure after the attempt's deadline is a timeout, whatever the
	// client called it.
	err := deliverError(ctx, nats.ErrConnectionClosed)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)
	// Unless it was already classified.
	err = deliverError(ctx, plugin.Permanentf("bad"))
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.NoError(t, deliverError(ctx, nil))
	// Before the deadline, the error decides.
	err = deliverError(context.Background(), nats.ErrConnectionClosed)
	assert.Equal(t, []string{plugin.TagConnect}, plugin.Tags(err))
}

func TestDenied(t *testing.T) {
	pub := fmt.Errorf("%w: %s", nats.ErrPermissionViolation, `Permissions Violation for Publish to "orders.secret"`)
	assert.True(t, deniedPublish(pub, nil, "orders.secret"))
	assert.False(t, deniedPublish(pub, pub, "orders.secret"), "an older refusal does not count")
	assert.False(t, deniedPublish(pub, nil, "orders.other"))
	assert.False(t, deniedPublish(errors.New("x"), nil, "orders.secret"))
	assert.False(t, deniedPublish(nil, nil, "orders.secret"))

	sub := fmt.Errorf("%w: %s", nats.ErrPermissionViolation, `Permissions Violation for Subscription to "orders.>" using queue "workers"`)
	assert.True(t, deniedSubscription(sub, "orders.>", "workers"))
	assert.True(t, deniedSubscription(sub, "orders.>", ""))
	assert.False(t, deniedSubscription(sub, "orders.>", "other"))
	assert.False(t, deniedSubscription(sub, "payments", ""))
	assert.False(t, deniedSubscription(pub, "orders.secret", ""))
}
