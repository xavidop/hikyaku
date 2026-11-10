package mqtt

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// sinkConfig is a destination publishing to out/<subject> on b.
func sinkConfig(b *broker, version string, mutate ...func(*config.Endpoint)) config.Endpoint {
	cfg := config.Endpoint{Type: kind, URL: b.url(), Topic: "out/{{ .subject }}", ProtocolVersion: version}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg
}

// newTestSink is a destination named out. Destinations on one broker need
// names of their own: their client IDs come from them, and a broker hands a
// client ID's session to its latest connection.
func newTestSink(t *testing.T, cfg config.Endpoint) *Sink {
	t.Helper()
	return newNamedSink(t, "out", cfg)
}

func newNamedSink(t *testing.T, name string, cfg config.Endpoint) *Sink {
	t.Helper()
	s, err := newSink(context.Background(), plugin.Spec{Name: name, Kind: kind, Config: &cfg, Env: plugin.Env{Relay: "test"}})
	require.NoError(t, err)
	s.reconnect = reconnectOptions{backoff: 10 * time.Millisecond, maxBackoff: 50 * time.Millisecond, jitter: "none"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		assert.NoError(t, s.Close(ctx))
	})
	return s
}

func testMessage(subject, body string) *envelope.Envelope {
	env := envelope.New("in", "http", []byte(body))
	env.Subject, env.ContentType = subject, contentJSON
	return env
}

// userProps lists a packet's user properties by key.
func userProps(pk packets.Packet) map[string][]string {
	out := map[string][]string{}
	for _, u := range pk.Properties.User {
		out[u.Key] = append(out[u.Key], u.Val)
	}
	return out
}

func deliverCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestSinkPublishes(t *testing.T) {
	for _, v := range versions {
		for _, qos := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("v%s/qos%d", v, qos), func(t *testing.T) {
				b := startBroker(t)
				got := b.subscribe("out/#")
				s := newTestSink(t, sinkConfig(b, v, func(c *config.Endpoint) {
					c.QoS = config.Ptr(qos)
					c.Headers = map[string]string{"x-region": "{{ .json.region }}"}
				}))
				env := testMessage("orders", `{"region":"eu"}`)
				env.SetHeader("X-Tenant", "acme")
				env.SetHeader("Authorization", "Bearer secret-token")
				env.SetHeader("Idempotency-Key", "stale")
				env.SetAttribute("idempotency_key", "order-42")

				res, err := s.Deliver(deliverCtx(t), env)
				require.NoError(t, err)
				assert.Equal(t, b.url(), res.Endpoint)
				pk := got.wait(t, 1)[0]
				assert.Equal(t, "out/orders", pk.TopicName)
				assert.JSONEq(t, `{"region":"eu"}`, string(pk.Payload))
				assert.Equal(t, byte(qos), pk.FixedHeader.Qos)
				assert.False(t, pk.FixedHeader.Retain)
				props := userProps(pk)
				if v == version311 {
					assert.Empty(t, props, "3.1.1 has no properties")
					assert.Empty(t, pk.Properties.ContentType)
					return
				}
				assert.Equal(t, contentJSON, pk.Properties.ContentType)
				assert.Equal(t, []string{"acme"}, props["X-Tenant"])
				assert.Equal(t, []string{"eu"}, props["X-Region"])
				assert.Equal(t, []string{"order-42"}, props["Idempotency-Key"])
				assert.NotContains(t, props, "Authorization")
				assert.NotContains(t, props, "Content-Type")
			})
		}
	}
}

func TestSinkRetainAndBodyTemplate(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSink(t, sinkConfig(b, v, func(c *config.Endpoint) {
				c.Topic, c.Retain, c.Body = "state/{{ .json.device }}", true, `{"on":{{ .json.on }}}`
			}))
			_, err := s.Deliver(deliverCtx(t), testMessage("x", `{"device":"lamp","on":true,"noise":1}`))
			require.NoError(t, err)
			// A new subscriber gets the retained message.
			pk := b.subscribe("state/#").wait(t, 1)[0]
			assert.Equal(t, "state/lamp", pk.TopicName)
			assert.JSONEq(t, `{"on":true}`, string(pk.Payload))
			assert.True(t, pk.FixedHeader.Retain)
		})
	}
}

func TestSinkRejectsBadPerMessageTopics(t *testing.T) {
	b := startBroker(t)
	s := newTestSink(t, sinkConfig(b, version5, func(c *config.Endpoint) { c.Topic = "{{ .json.room }}" }))
	for _, body := range []string{`{"room":"a/+"}`, `{"room":"#"}`, `{}`} { // {}: an empty topic
		_, err := s.Deliver(deliverCtx(t), testMessage("x", body))
		require.Error(t, err, body)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, body)
		assert.Contains(t, plugin.Tags(err), "template", body)
	}
	s = newTestSink(t, sinkConfig(b, version5, func(c *config.Endpoint) { c.Topic = "out/{{ .json.x.y }}" }))
	_, err := s.Deliver(deliverCtx(t), testMessage("x", `{"x":"str"}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
}

func TestSinkBrokerReasonCodes(t *testing.T) {
	b := startBroker(t)
	b.hook.set(func(h *testHook) { h.denyTopic = "out/denied" })
	for _, qos := range []int{1, 2} {
		s := newNamedSink(t, fmt.Sprintf("out-qos%d", qos), sinkConfig(b, version5, func(c *config.Endpoint) { c.QoS = config.Ptr(qos) }))
		_, err := s.Deliver(deliverCtx(t), testMessage("denied", "{}"))
		require.Error(t, err, "qos %d", qos)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, "qos %d", qos)
		assert.Equal(t, []string{"mqtt:0x87"}, plugin.Tags(err), "qos %d", qos)
		assert.Contains(t, err.Error(), "not authorized")
		_, err = s.Deliver(deliverCtx(t), testMessage("allowed", "{}"))
		require.NoError(t, err, "qos %d: the connection survives a refusal", qos)
	}

	quota := packets.ErrQuotaExceeded
	b.hook.set(func(h *testHook) { h.refuse = &quota })
	s := newNamedSink(t, "out-quota", sinkConfig(b, version5))
	_, err := s.Deliver(deliverCtx(t), testMessage("any", "{}"))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.RateLimited, class)
	assert.ElementsMatch(t, []string{"mqtt:0x97", plugin.Tag429}, plugin.Tags(err))
}

func TestSinkMQTT311RefusalDropsTheConnection(t *testing.T) {
	b := startBroker(t)
	b.hook.set(func(h *testHook) { h.denyTopic = "out/denied" })
	s := newTestSink(t, sinkConfig(b, version311))
	// 3.1.1 has no reason codes: the broker disconnects instead.
	_, err := s.Deliver(deliverCtx(t), testMessage("denied", "{}"))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Contains(t, plugin.Tags(err), plugin.TagConnect)
	require.Eventually(t, func() bool {
		_, err := s.Deliver(deliverCtx(t), testMessage("allowed", "{}"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond, "the destination reconnects")
}

// fakeClient is a client whose publishes fail as told.
type fakeClient struct {
	*ending
	lost      chan struct{}
	publishFn func(ctx context.Context, p *publication) error
	mu        sync.Mutex
	published []*publication
}

func newFakeClient(publish func(ctx context.Context, p *publication) error) *fakeClient {
	return &fakeClient{ending: newEnding(), lost: make(chan struct{}), publishFn: publish}
}

func (f *fakeClient) SessionPresent() bool                              { return false }
func (f *fakeClient) Subscribe(context.Context, []string, byte) error   { return nil }
func (f *fakeClient) Lost() <-chan struct{}                             { return f.lost }
func (f *fakeClient) Disconnect(context.Context) error                  { f.drop(errDisconnected); return nil }
func (f *fakeClient) Publish(ctx context.Context, p *publication) error { return f.publish(ctx, p) }

func (f *fakeClient) publish(ctx context.Context, p *publication) error {
	f.mu.Lock()
	f.published = append(f.published, p)
	f.mu.Unlock()
	if f.publishFn == nil {
		return nil
	}
	return f.publishFn(ctx, p)
}

func (f *fakeClient) drop(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !closed(f.lost) {
		close(f.lost)
	}
	f.finish(err)
}

// fakeSink is a destination whose connections are fake clients.
func fakeSink(t *testing.T, version string, clients ...*fakeClient) *Sink {
	t.Helper()
	s := newTestSink(t, config.Endpoint{Type: kind, URL: "mqtt://broker.invalid", Topic: "t", ProtocolVersion: version})
	var mu sync.Mutex
	s.connect = func(context.Context, server, connOptions, func(*message)) (client, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(clients) == 0 {
			return nil, errors.New("no more fake connections")
		}
		c := clients[0]
		clients = clients[1:]
		return c, nil
	}
	return s
}

func TestSinkReasonCodeMapping(t *testing.T) {
	for _, tc := range []struct {
		code  byte
		class plugin.Class
	}{
		{0x80, plugin.Transient}, {0x83, plugin.Permanent}, {0x87, plugin.Permanent},
		{0x88, plugin.Transient}, {0x89, plugin.Transient}, {0x90, plugin.Permanent},
		{0x91, plugin.Transient}, {0x95, plugin.Permanent}, {0x97, plugin.RateLimited},
		{0x99, plugin.Permanent}, {0x9f, plugin.Transient},
	} {
		code := tc.code
		s := fakeSink(t, version5, newFakeClient(func(context.Context, *publication) error {
			return &reasonError{code: code, reason: "because"}
		}))
		_, err := s.Deliver(deliverCtx(t), testMessage("x", "{}"))
		require.Error(t, err)
		class, _ := plugin.Classify(err)
		assert.Equal(t, tc.class, class, "0x%02x", code)
		tag := fmt.Sprintf("mqtt:0x%02x", code)
		assert.Contains(t, plugin.Tags(err), tag)
		assert.Contains(t, err.Error(), "because")
	}
}

func TestSinkPublishFailures(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		s := fakeSink(t, version5, newFakeClient(func(ctx context.Context, _ *publication) error {
			<-ctx.Done()
			return ctx.Err()
		}))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := s.Deliver(ctx, testMessage("x", "{}"))
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Timeout, class)
		assert.Contains(t, plugin.Tags(err), plugin.TagTimeout)
	})
	t.Run("unsupported", func(t *testing.T) {
		s := fakeSink(t, version5, newFakeClient(func(context.Context, *publication) error {
			return fmt.Errorf("%w: server maximum QoS is 1", errUnsupported)
		}))
		_, err := s.Deliver(deliverCtx(t), testMessage("x", "{}"))
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class)
	})
	t.Run("connection lost", func(t *testing.T) {
		var fc *fakeClient
		fc = newFakeClient(func(context.Context, *publication) error {
			fc.drop(errors.New("EOF"))
			return errors.New("PUBLISH transmitted but not fully acknowledged at time of shutdown")
		})
		s := fakeSink(t, version5, fc)
		_, err := s.Deliver(deliverCtx(t), testMessage("x", "{}"))
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Transient, class)
		assert.Equal(t, []string{plugin.TagConnect}, plugin.Tags(err))
	})
}

func TestSinkConnectionLostBeforeTheAckFailsFast(t *testing.T) {
	for _, v := range versions {
		for _, qos := range []int{1, 2} {
			t.Run(fmt.Sprintf("v%s/qos%d", v, qos), func(t *testing.T) {
				b := startBroker(t)
				hold, entered := make(chan struct{}), make(chan string, 1)
				defer close(hold) // before the broker closes
				b.hook.set(func(h *testHook) { h.hold, h.entered = hold, entered })
				s := newTestSink(t, sinkConfig(b, v, func(c *config.Endpoint) { c.QoS = config.Ptr(qos) }))

				errc := make(chan error, 1)
				start := time.Now()
				go func() {
					_, err := s.Deliver(deliverCtx(t), testMessage("x", "{}"))
					errc <- err
				}()
				id := <-entered // the broker holds the publish unacknowledged
				cl, ok := b.server().Clients.Get(id)
				require.True(t, ok)
				_ = b.server().DisconnectClient(cl, packets.ErrServerBusy)
				err := <-errc
				require.Error(t, err)
				assert.Less(t, time.Since(start), 2*time.Second, "failed at once, not at the deadline")
				class, _ := plugin.Classify(err)
				assert.Equal(t, plugin.Transient, class)
				assert.Contains(t, plugin.Tags(err), plugin.TagConnect)
			})
		}
	}
}

func TestSinkNotConnected(t *testing.T) {
	s := newTestSink(t, config.Endpoint{Type: kind, URL: "mqtt://" + closedAddr(t), Topic: "t"})
	logs := captureSinkLogs(s)
	_, err := s.Deliver(deliverCtx(t), testMessage("x", "{}"))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{plugin.TagConnect}, plugin.Tags(err))
	assert.Contains(t, err.Error(), "not connected to mqtt://")
	assert.Error(t, s.Health(deliverCtx(t)))
	require.Eventually(t, func() bool { return logs.String() != "" }, time.Second, 5*time.Millisecond, "the reconnects are logged")
}

func TestSinkReconnectsAfterBrokerRestart(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSink(t, sinkConfig(b, v))
			_, err := s.Deliver(deliverCtx(t), testMessage("before", "{}"))
			require.NoError(t, err)

			b.restart()
			got := b.subscribe("out/#")
			require.Eventually(t, func() bool {
				_, err := s.Deliver(deliverCtx(t), testMessage("after", "{}"))
				if err != nil {
					assert.Contains(t, plugin.Tags(err), plugin.TagConnect, "while down: %v", err)
				}
				return err == nil
			}, 5*time.Second, 10*time.Millisecond)
			assert.Equal(t, "out/after", got.wait(t, 1)[0].TopicName)
			assert.NoError(t, s.Health(deliverCtx(t)))
		})
	}
}

func TestSinkHealthAndClose(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSink(t, sinkConfig(b, v))
			id := defaultClientID("out")
			assert.False(t, b.connected(id), "connected lazily")
			require.NoError(t, s.Health(deliverCtx(t)), "Health connects")
			assert.True(t, b.connected(id))

			require.NoError(t, s.Close(deliverCtx(t)))
			require.Eventually(t, func() bool { return !b.connected(id) }, 5*time.Second, 5*time.Millisecond)
			_, err := s.Deliver(deliverCtx(t), testMessage("x", "{}"))
			require.ErrorIs(t, err, errClosed)
			assert.ErrorIs(t, s.Health(deliverCtx(t)), errClosed)
			assert.NoError(t, s.Close(deliverCtx(t)), "a second Close is a no-op")
		})
	}
}

func TestSinkCredentialsAreNotLogged(t *testing.T) {
	b := startBroker(t)
	b.hook.set(func(h *testHook) { h.user, h.pass = "relay", "pa55word" })
	s := newTestSink(t, sinkConfig(b, version5, func(c *config.Endpoint) { c.URL = "mqtt://relay:pa55word@" + b.addr }))
	res, err := s.Deliver(deliverCtx(t), testMessage("x", "{}"))
	require.NoError(t, err)
	assert.NotContains(t, res.Endpoint, "pa55word")

	bad := newTestSink(t, sinkConfig(b, version5, func(c *config.Endpoint) { c.URL = "mqtt://relay:wr0ng@" + b.addr }))
	logs := captureSinkLogs(bad)
	_, err = bad.Deliver(deliverCtx(t), testMessage("x", "{}"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad user name or password")
	assert.NotContains(t, err.Error(), "wr0ng")
	require.Eventually(t, func() bool { return logs.String() != "" }, time.Second, 5*time.Millisecond)
	assert.NotContains(t, logs.String(), "wr0ng")
}

func TestSinkUserPropertiesSkipWhatMQTTCannotCarry(t *testing.T) {
	fc := newFakeClient(nil)
	s := fakeSink(t, version5, fc)
	env := testMessage("x", "{}")
	env.Headers["X-Bad"] = []string{"nul\x00byte", "ok"}
	env.Headers["X-Invalid"] = []string{"\xff"}
	_, err := s.Deliver(deliverCtx(t), env)
	require.NoError(t, err)
	var keys []string
	for _, p := range fc.published[0].userProps {
		keys = append(keys, p.key+"="+p.value)
	}
	assert.True(t, slices.Contains(keys, "X-Bad=ok"))
	assert.False(t, slices.ContainsFunc(keys, func(k string) bool { return k == "X-Invalid=\xff" }))
}

// captureSinkLogs points a destination's logger at a buffer.
func captureSinkLogs(s *Sink) *logBuffer {
	l, buf := debugLogger()
	s.log = l
	return buf
}
