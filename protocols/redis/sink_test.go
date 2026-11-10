package redis

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func TestSinkXAdd(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	sink := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "events"})

	env := msg(`{"order":7}`)
	env.Subject = "/hooks/shop"
	env.Headers = map[string][]string{"X-Tenant": {"acme"}, "Accept": {"a", "b"}, "Authorization": {"Bearer x"}}
	res, err := sink.Deliver(ctx, env)
	require.NoError(t, err)
	entries, err := admin.XRange(ctx, "events", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, []string{entries[0].ID}, res.Headers[HeaderEntryID])
	assert.Equal(t, "redis://"+mr.Addr()+" XADD events", res.Endpoint)
	assert.Equal(t, map[string]any{
		"id": env.ID, "body": `{"order":7}`, "content_type": "application/json", "subject": "/hooks/shop",
		"h:X-Tenant": "acme", "h:Accept": "a, b",
	}, entries[0].Values)
}

func TestSinkAuth(t *testing.T) {
	mr, _ := startMini(t)
	ctx := context.Background()
	mr.RequireUserAuth("relay", "right")

	sink := newTestSink(t, config.Endpoint{URL: "redis://relay:stale@" + mr.Addr(), Password: "right", Stream: "events"})
	res, err := sink.Deliver(ctx, msg(`{}`))
	require.NoError(t, err, "password overrides the URL's")
	assert.Equal(t, "redis://relay:xxxxx@"+mr.Addr()+" XADD events", res.Endpoint, "the password stays out")

	stale := newTestSink(t, config.Endpoint{URL: "redis://relay:stale@" + mr.Addr(), Stream: "events"})
	_, err = stale.Deliver(ctx, msg(`{}`))
	class, tags := classOf(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Equal(t, []string{"redis:WRONGPASS"}, tags)
	assert.NotContains(t, err.Error(), "stale")

	none := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "events"})
	_, err = none.Deliver(ctx, msg(`{}`))
	class, tags = classOf(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Equal(t, []string{"redis:NOAUTH"}, tags)
}

func TestSinkMaxLen(t *testing.T) {
	mr, admin := startMini(t)
	sink := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "capped", MaxLen: 3})
	for range 5 {
		_, err := sink.Deliver(context.Background(), msg(`{}`))
		require.NoError(t, err)
	}
	n, err := admin.XLen(context.Background(), "capped").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(3), n, "MAXLEN ~ 3 (miniredis trims exactly; Redis trims whole nodes)")
}

func TestSinkTemplates(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	sink := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "events",
		Body:    `{"wrapped":{{ json .json }}}`,
		Headers: map[string]string{"X-Level": "{{ .json.level }}", "accept": "text/plain"}})
	env := msg(`{"level":"high"}`)
	env.Headers = map[string][]string{"Accept": {"*/*"}}
	_, err := sink.Deliver(ctx, env)
	require.NoError(t, err)
	entries, err := admin.XRange(ctx, "events", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, `{"wrapped":{"level":"high"}}`, entries[0].Values["body"])
	assert.Equal(t, "high", entries[0].Values["h:X-Level"])
	assert.Equal(t, "text/plain", entries[0].Values["h:accept"], "a configured header replaces the message's")
	assert.NotContains(t, entries[0].Values, "h:Accept")

	bad := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "events", Body: "{{ index .json.items 3 }}"})
	_, err = bad.Deliver(ctx, msg(`{"items":[1]}`))
	class, tags := classOf(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Equal(t, []string{"template"}, tags)
}

func TestSinkBatch(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	sink := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "events"})
	envs := []*envelope.Envelope{msg(`{"n":1}`), msg(`{"n":2}`), msg(`{"n":3}`)}
	res, err := sink.DeliverBatch(ctx, envs)
	require.NoError(t, err)
	entries, err := admin.XRange(ctx, "events", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 3)
	for i, e := range entries {
		assert.Equal(t, envs[i].ID, e.Values["id"], "in order")
		assert.Equal(t, e.ID, res.Headers[HeaderEntryID][i])
	}

	t.Run("a template error sends nothing", func(t *testing.T) {
		bad := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "other", Body: "{{ index .json.items 0 }}"})
		_, err := bad.DeliverBatch(ctx, []*envelope.Envelope{msg(`{"items":[1]}`), msg(`{}`)})
		class, _ := classOf(err)
		assert.Equal(t, plugin.Permanent, class)
		n, err := admin.Exists(ctx, "other").Result()
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("a server error fails the batch", func(t *testing.T) {
		require.NoError(t, admin.Set(ctx, "scalar", "x", 0).Err())
		wrong := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "scalar"})
		_, err := wrong.DeliverBatch(ctx, []*envelope.Envelope{msg(`{}`), msg(`{}`)})
		class, tags := classOf(err)
		assert.Equal(t, plugin.Permanent, class)
		assert.Equal(t, []string{"redis:WRONGTYPE"}, tags)
	})
}

func TestSinkPublish(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	sink := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Channel: "alerts.{{ .json.level }}"})

	res, err := sink.Deliver(ctx, msg(`{"level":"high"}`))
	require.NoError(t, err, "nobody listening is not an error")
	assert.Equal(t, []string{"0"}, res.Headers[HeaderReceivers])

	sub := admin.Subscribe(ctx, "alerts.high")
	defer func() { _ = sub.Close() }()
	_, err = sub.Receive(ctx) // the subscription reply
	require.NoError(t, err)
	res, err = sink.Deliver(ctx, msg(`{"level":"high"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"1"}, res.Headers[HeaderReceivers])
	assert.Equal(t, "redis://"+mr.Addr()+" PUBLISH alerts.high", res.Endpoint)
	got, err := sub.ReceiveMessage(ctx)
	require.NoError(t, err)
	assert.Equal(t, `{"level":"high"}`, got.Payload, "the raw body")

	res, err = sink.DeliverBatch(ctx, []*envelope.Envelope{msg(`{"level":"high","n":1}`), msg(`{"level":"low"}`)})
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "0"}, res.Headers[HeaderReceivers])
	assert.Equal(t, "redis://"+mr.Addr()+" PUBLISH", res.Endpoint, "a templated channel varies within a batch")
	got, err = sub.ReceiveMessage(ctx)
	require.NoError(t, err)
	assert.Equal(t, `{"level":"high","n":1}`, got.Payload)

	empty := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Channel: "{{ .json.channel }}"})
	_, err = empty.Deliver(ctx, msg(`{}`))
	class, tags := classOf(err)
	assert.Equal(t, plugin.Permanent, class, "a channel rendered empty")
	assert.Equal(t, []string{"template"}, tags)

	static := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Channel: "fixed", Body: "{{ .json.text }}"})
	res, err = static.DeliverBatch(ctx, []*envelope.Envelope{msg(`{"text":"hi"}`)})
	require.NoError(t, err)
	assert.Equal(t, "redis://"+mr.Addr()+" PUBLISH fixed", res.Endpoint)
}

// TestSinkServerErrors injects each error reply and checks its class and
// tags, as the retry policy will see them.
func TestSinkServerErrors(t *testing.T) {
	mr, _ := startMini(t)
	ctx := context.Background()
	sink := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "events"})
	_, err := sink.Deliver(ctx, msg(`{}`)) // open the connection first
	require.NoError(t, err)
	for reply, want := range map[string]struct {
		class      plugin.Class
		tags       []string
		retryAfter time.Duration
	}{
		"LOADING Redis is loading the dataset in memory": {plugin.Transient, []string{"redis:LOADING"}, 0},
		"BUSY Redis is busy running a script":            {plugin.Transient, []string{"redis:BUSY"}, 0},
		"TRYAGAIN Multiple keys request during rehash":   {plugin.Transient, []string{"redis:TRYAGAIN"}, 0},
		"CLUSTERDOWN The cluster is down":                {plugin.Transient, []string{"redis:CLUSTERDOWN"}, 0},
		"MASTERDOWN Link with MASTER is down":            {plugin.Transient, []string{"redis:MASTERDOWN"}, 0},
		"READONLY You can't write against a replica":     {plugin.Transient, []string{"redis:READONLY"}, 0},
		"OOM command not allowed when used memory":       {plugin.RateLimited, []string{"redis:OOM", "429"}, time.Second},
		"WRONGTYPE Operation against a key":              {plugin.Permanent, []string{"redis:WRONGTYPE"}, 0},
		"NOPERM User has no permissions":                 {plugin.Permanent, []string{"redis:NOPERM"}, 0},
		"NOAUTH Authentication required.":                {plugin.Permanent, []string{"redis:NOAUTH"}, 0},
		"WRONGPASS invalid username-password pair":       {plugin.Permanent, []string{"redis:WRONGPASS"}, 0},
		"ERR unknown command":                            {plugin.Permanent, []string{"redis:ERR"}, 0},
	} {
		t.Run(reply, func(t *testing.T) {
			mr.SetError(reply)
			defer mr.SetError("")
			_, err := sink.Deliver(ctx, msg(`{}`))
			require.Error(t, err)
			class, tags := classOf(err)
			assert.Equal(t, want.class, class)
			assert.Equal(t, want.tags, tags)
			_, retryAfter := plugin.Classify(err)
			assert.Equal(t, want.retryAfter, retryAfter)
			assert.Contains(t, err.Error(), reply)
		})
	}
}

func TestSinkConnectionErrors(t *testing.T) {
	ctx := context.Background()
	refused := newTestSink(t, config.Endpoint{URL: redisURL(closedAddr(t)), Channel: "c"})
	_, err := refused.Deliver(ctx, msg(`{}`))
	class, tags := classOf(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{plugin.TagConnect}, tags)
	assert.Error(t, refused.Health(ctx))

	// A server that accepts and never answers runs out the attempt's time.
	silent := newTestSink(t, config.Endpoint{URL: redisURL(silentServer(t)), Stream: "s"})
	actx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = silent.Deliver(actx, msg(`{}`))
	class, tags = classOf(err)
	assert.Equal(t, plugin.Timeout, class)
	assert.Equal(t, []string{plugin.TagTimeout}, tags)
}

// silentServer accepts connections and never answers.
func silentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	return ln.Addr().String()
}

func TestSinkReconnects(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	p := newProxy(t, mr.Addr())
	sink := newTestSink(t, config.Endpoint{URL: redisURL(p.Addr()), Stream: "events"})
	_, err := sink.Deliver(ctx, msg(`{"n":1}`))
	require.NoError(t, err)
	p.Drop()
	_, err = sink.Deliver(ctx, msg(`{"n":2}`))
	if err != nil {
		// The pool may hand out the dropped connection once: the relay
		// retries a connect failure.
		class, tags := classOf(err)
		assert.Equal(t, plugin.Transient, class)
		assert.Equal(t, []string{plugin.TagConnect}, tags)
		_, err = sink.Deliver(ctx, msg(`{"n":2}`))
	}
	require.NoError(t, err, "the next delivery dials again")
	n, err := admin.XLen(ctx, "events").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestSinkHealthAndClose(t *testing.T) {
	mr, _ := startMini(t)
	ctx := context.Background()
	sink := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "events"})
	require.NoError(t, sink.Health(ctx))
	require.NoError(t, sink.Close(ctx))
	require.NoError(t, sink.Close(ctx))
	assert.ErrorIs(t, sink.Health(ctx), goredis.ErrClosed)
	_, err := sink.Deliver(ctx, msg(`{}`))
	class, _ := classOf(err)
	assert.Equal(t, plugin.Transient, class)
}
