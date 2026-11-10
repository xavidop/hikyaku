package nats

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func jsonEnv(body string) *envelope.Envelope {
	env := envelope.New("in", "http", []byte(body))
	env.ContentType = contentJSON
	return env
}

func requireClass(t *testing.T, err error, class plugin.Class, tags ...string) {
	t.Helper()
	require.Error(t, err)
	got, _ := plugin.Classify(err)
	assert.Equal(t, class, got, "class of %v", err)
	for _, tag := range tags {
		assert.Contains(t, plugin.Tags(err), tag, "tags of %v", err)
	}
}

func TestSinkPublishesAndFlushes(t *testing.T) {
	ts := startServer(t)
	sub := ts.subscribe("orders.>")
	sink := newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "orders.{{ .json.region }}",
		Headers: map[string]string{"X-Tenant": "{{ .json.tenant }}", "Nats-TTL": "1h"}})
	assert.Equal(t, "out", sink.Name())

	env := jsonEnv(`{"region":"eu","tenant":"acme"}`)
	env.Headers = map[string][]string{
		"Authorization": {"Bearer caller-secret"}, "X-Request-Id": {"r1"}, "Nats-Expected-Stream": {"OTHER"},
		"content-type": {"text/plain"}, "Traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}
	res, err := sink.Deliver(ctxT(t), env)
	require.NoError(t, err)
	assert.Equal(t, ts.url(), res.Endpoint)

	// The flush means the server has it: it is already routed.
	m, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "orders.eu", m.Subject)
	assert.Equal(t, `{"region":"eu","tenant":"acme"}`, string(m.Data))
	assert.Equal(t, "acme", m.Header.Get("X-Tenant"))
	assert.Equal(t, "r1", m.Header.Get("X-Request-Id"))
	assert.Equal(t, env.ID, m.Header.Get("Idempotency-Key"))
	assert.Equal(t, contentJSON, m.Header.Get("Content-Type"))
	assert.Equal(t, []string{contentJSON}, m.Header.Values("Content-Type"), "one Content-Type")
	assert.Empty(t, m.Header.Values("content-type"))
	assert.Equal(t, "1h", m.Header.Get("Nats-TTL"), "the destination's headers may set Nats-*")
	assert.NotEmpty(t, m.Header.Get("Traceparent"))
	assert.Empty(t, m.Header.Get("Authorization"), "the caller's credentials are not forwarded")
	assert.Empty(t, m.Header.Get("Nats-Expected-Stream"), "NATS control headers are not forwarded")

	// The relay's own Idempotency-Key is kept; a body template replaces
	// the body.
	tmpl := newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "orders.fixed", Body: `{"wrapped":{{ json .json }}}`})
	env = jsonEnv(`{"a":1}`)
	env.SetHeader("Idempotency-Key", "relay-key")
	_, err = tmpl.Deliver(ctxT(t), env)
	require.NoError(t, err)
	m, err = sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, `{"wrapped":{"a":1}}`, string(m.Data))
	assert.Equal(t, "relay-key", m.Header.Get("Idempotency-Key"))
}

func TestSinkErrors(t *testing.T) {
	ts := startServer(t, func(o *server.Options) {
		o.MaxPayload = 1024
		o.Users = []*server.User{{Username: "app", Password: "pw", Permissions: &server.Permissions{
			Publish: &server.SubjectPermission{Deny: []string{"secret.>"}},
		}}}
	})
	sink := newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "{{ .json.subject }}", Username: "app", Password: "pw"})

	_, err := sink.Deliver(ctxT(t), jsonEnv(`{"subject":"orders."}`))
	requireClass(t, err, plugin.Permanent, tagTemplate)
	_, err = sink.Deliver(ctxT(t), jsonEnv(`{}`))
	requireClass(t, err, plugin.Permanent, tagTemplate)

	_, err = sink.Deliver(ctxT(t), jsonEnv(`{"subject":"secret.plans"}`))
	requireClass(t, err, plugin.Permanent, tagPermissions)
	_, err = sink.Deliver(ctxT(t), jsonEnv(`{"subject":"public.news"}`))
	require.NoError(t, err, "an older refusal does not fail the next publish")

	big := jsonEnv(fmt.Sprintf(`{"subject":"public.big","pad":%q}`, strings.Repeat("x", 2048)))
	_, err = sink.Deliver(ctxT(t), big)
	requireClass(t, err, plugin.Permanent, tagMaxPayload)

	down := newTestSink(t, config.Endpoint{URL: "nats://" + closedAddr(t), Subject: "x"})
	_, err = down.Deliver(ctxT(t), jsonEnv(`{}`))
	requireClass(t, err, plugin.Transient, plugin.TagConnect)
	requireClass(t, down.Health(ctxT(t)), plugin.Transient, plugin.TagConnect)
}

func TestSinkReconnectsAfterServerRestart(t *testing.T) {
	ts := startServer(t)
	sink := newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "orders"})
	require.NoError(t, sink.Start(ctxT(t)))
	require.NoError(t, sink.Health(ctxT(t)))
	_, err := sink.Deliver(ctxT(t), jsonEnv(`{}`))
	require.NoError(t, err)

	ts.stop()
	require.Eventually(t, func() bool {
		_, err := sink.Deliver(ctxT(t), jsonEnv(`{}`))
		if err == nil {
			return false
		}
		class, _ := plugin.Classify(err)
		return class == plugin.Transient && strings.Contains(strings.Join(plugin.Tags(err), ","), plugin.TagConnect)
	}, 10*time.Second, 10*time.Millisecond)

	ts.start()
	sub := ts.subscribe("orders")
	require.Eventually(t, func() bool {
		_, err := sink.Deliver(ctxT(t), jsonEnv(`{"after":true}`))
		return err == nil
	}, 15*time.Second, 50*time.Millisecond)
	m, err := sub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, `{"after":true}`, string(m.Data))
	_, err = sub.NextMsg(200 * time.Millisecond)
	assert.ErrorIs(t, err, nats.ErrTimeout, "nothing failed while down went out later")
}

func TestSinkClose(t *testing.T) {
	ts := startServer(t)
	sink := newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "orders"})
	require.NoError(t, sink.Close(context.Background()), "closing a destination that never connected")
	_, err := sink.Deliver(ctxT(t), jsonEnv(`{}`))
	requireClass(t, err, plugin.Transient, plugin.TagConnect)
	require.NoError(t, sink.Start(context.Background()), "starting a closed destination does nothing")

	sink = newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "orders"})
	_, err = sink.Deliver(ctxT(t), jsonEnv(`{}`))
	require.NoError(t, err)
	c := sink.conn
	require.NoError(t, sink.Close(ctxT(t)))
	assert.True(t, c.nc.IsClosed())
	require.NoError(t, sink.Close(ctxT(t)), "closing twice")
	_, err = sink.Deliver(ctxT(t), jsonEnv(`{}`))
	requireClass(t, err, plugin.Transient, plugin.TagConnect)
}

func TestStreamSinkPublishesWithMsgID(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	sink := newTestStreamSink(t, config.Endpoint{URL: ts.url(), Subject: "orders.new", Stream: "ORDERS"})
	require.NoError(t, sink.Health(ctxT(t)))

	env := jsonEnv(`{"n":1}`)
	res, err := sink.Deliver(ctxT(t), env)
	require.NoError(t, err)
	assert.Equal(t, []string{"ORDERS"}, res.Headers[resultStream])
	assert.Equal(t, []string{"1"}, res.Headers[resultSequence])
	assert.Equal(t, []string{"false"}, res.Headers[resultDuplicate])
	assert.Equal(t, ts.url(), res.Endpoint)

	// A retry of the same message is dropped by the stream.
	res, err = sink.Deliver(ctxT(t), env)
	require.NoError(t, err)
	assert.Equal(t, []string{"1"}, res.Headers[resultSequence])
	assert.Equal(t, []string{"true"}, res.Headers[resultDuplicate])

	stream, err := js.Stream(ctxT(t), "ORDERS")
	require.NoError(t, err)
	info, err := stream.Info(ctxT(t))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.State.Msgs)
	stored, err := stream.GetMsg(ctxT(t), 1)
	require.NoError(t, err)
	assert.Equal(t, env.ID, stored.Header.Get(jetstream.MsgIDHeader))
	assert.Equal(t, env.ID, stored.Header.Get("Idempotency-Key"))
	assert.Equal(t, contentJSON, stored.Header.Get("Content-Type"))

	// The destination's headers may set the Nats-Msg-Id instead.
	own := newTestStreamSink(t, config.Endpoint{URL: ts.url(), Subject: "orders.new",
		Headers: map[string]string{"nats-msg-id": "{{ .id }}-copy"}})
	res, err = own.Deliver(ctxT(t), env)
	require.NoError(t, err)
	assert.Equal(t, []string{"2"}, res.Headers[resultSequence])
	stored, err = stream.GetMsg(ctxT(t), 2)
	require.NoError(t, err)
	assert.Equal(t, env.ID+"-copy", stored.Header.Get(jetstream.MsgIDHeader))
	assert.Empty(t, stored.Header.Values("nats-msg-id"))
}

func TestStreamSinkErrors(t *testing.T) {
	ts := startServer(t, func(o *server.Options) {
		o.Users = []*server.User{{Username: "app", Password: "pw", Permissions: &server.Permissions{
			Publish: &server.SubjectPermission{Deny: []string{"orders.secret"}},
		}}}
	})
	admin, err := jetstream.New(ts.client(nats.UserInfo("app", "pw")))
	require.NoError(t, err)
	_, err = admin.CreateStream(ctxT(t), jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}})
	require.NoError(t, err)
	auth := func(e config.Endpoint) config.Endpoint { e.Username, e.Password = "app", "pw"; return e }

	nowhere := newTestStreamSink(t, auth(config.Endpoint{URL: ts.url(), Subject: "nowhere"}))
	_, err = nowhere.Deliver(ctxT(t), jsonEnv(`{}`))
	requireClass(t, err, plugin.Permanent, tagNoResponders)

	wrong := newTestStreamSink(t, auth(config.Endpoint{URL: ts.url(), Subject: "orders.new", Stream: "OTHER"}))
	_, err = wrong.Deliver(ctxT(t), jsonEnv(`{}`))
	requireClass(t, err, plugin.Permanent, "jetstream:10060", plugin.Tag4xx)

	sink := newTestStreamSink(t, auth(config.Endpoint{URL: ts.url(), Subject: "{{ .json.subject }}"}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = sink.Deliver(ctx, jsonEnv(`{"subject":"orders.secret"}`))
	requireClass(t, err, plugin.Permanent, tagPermissions)

	expired, cancel2 := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel2()
	<-expired.Done()
	_, err = sink.Deliver(expired, jsonEnv(`{"subject":"orders.new"}`))
	requireClass(t, err, plugin.Timeout, plugin.TagTimeout)

	down := newTestStreamSink(t, config.Endpoint{URL: "nats://" + closedAddr(t), Subject: "orders.new"})
	_, err = down.Deliver(ctxT(t), jsonEnv(`{}`))
	requireClass(t, err, plugin.Transient, plugin.TagConnect)
	_, err = down.DeliverBatch(ctxT(t), []*envelope.Envelope{jsonEnv(`{}`)})
	requireClass(t, err, plugin.Transient, plugin.TagConnect)
}

func TestStreamSinkBatch(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	sink := newTestStreamSink(t, config.Endpoint{URL: ts.url(), Subject: "{{ .json.subject }}"})
	var envs []*envelope.Envelope
	for i := range 5 {
		envs = append(envs, jsonEnv(fmt.Sprintf(`{"subject":"orders.new","n":%d}`, i)))
	}
	res, err := sink.DeliverBatch(ctxT(t), envs)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3", "4", "5"}, res.Headers[resultSequence])
	assert.Equal(t, []string{"ORDERS", "ORDERS", "ORDERS", "ORDERS", "ORDERS"}, res.Headers[resultStream])

	// The whole batch again (a retry): every message is a duplicate.
	res, err = sink.DeliverBatch(ctxT(t), envs)
	require.NoError(t, err)
	assert.Equal(t, []string{"true", "true", "true", "true", "true"}, res.Headers[resultDuplicate])

	// One message no stream takes fails the batch; the others are stored,
	// and a retry would find them already there.
	bad := append([]*envelope.Envelope{jsonEnv(`{"subject":"orders.new","n":5}`)}, jsonEnv(`{"subject":"nowhere"}`))
	_, err = sink.DeliverBatch(ctxT(t), bad)
	requireClass(t, err, plugin.Permanent, tagNoResponders)
	assert.Contains(t, err.Error(), bad[1].ID)

	// A message that cannot be built fails the batch before anything goes.
	_, err = sink.DeliverBatch(ctxT(t), []*envelope.Envelope{jsonEnv(`{"subject":"orders.new","n":6}`), jsonEnv(`{}`)})
	requireClass(t, err, plugin.Permanent, tagTemplate)

	stream, err := js.Stream(ctxT(t), "ORDERS")
	require.NoError(t, err)
	info, err := stream.Info(ctxT(t))
	require.NoError(t, err)
	assert.Equal(t, uint64(6), info.State.Msgs)
}

func TestStreamSinkHealth(t *testing.T) {
	ts := startServer(t)
	sink := newTestStreamSink(t, config.Endpoint{URL: ts.url(), Subject: "orders.new"})
	require.NoError(t, sink.Health(ctxT(t)))

	plain := startServer(t, func(o *server.Options) { o.JetStream = false })
	nojs := newTestStreamSink(t, config.Endpoint{URL: plain.url(), Subject: "orders.new"})
	err := nojs.Health(ctxT(t))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jetstream")

	require.NoError(t, sink.Close(ctxT(t)))
	requireClass(t, sink.Health(ctxT(t)), plugin.Transient, plugin.TagConnect)
}
