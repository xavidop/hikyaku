package mqtt

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// sourceConfig is a source of b's orders/# with fast reconnects.
func sourceConfig(b *broker, version string, mutate ...func(*config.Source)) config.Source {
	cfg := config.Source{
		URL: b.url(), Topic: "orders/#", ProtocolVersion: version,
		Reconnect: &config.Reconnect{Backoff: config.Duration(10 * time.Millisecond), MaxBackoff: config.Duration(50 * time.Millisecond)},
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg
}

// defaultClientID is the client ID of a test source or destination named n.
func defaultClientID(n string) string { return "hikyaku-test-" + n }

func newTestSource(t *testing.T, cfg config.Source) *Source {
	t.Helper()
	cfg.Name, cfg.Type = "in", kind
	s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: &cfg, Env: plugin.Env{Relay: "test"}})
	require.NoError(t, err)
	s.disconnectTimeout = time.Second
	return s
}

// run starts s. The returned stop (also run at cleanup) ends Start, checks
// it returned nil, and closes s.
func run(t *testing.T, s *Source) (<-chan *envelope.Envelope, func()) {
	t.Helper()
	out := make(chan *envelope.Envelope, 256)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Start(ctx, out) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-errc:
				assert.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Error("Start did not return after its context ended")
			}
			cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer ccancel()
			assert.NoError(t, s.Close(cctx))
		})
	}
	t.Cleanup(stop)
	return out, stop
}

func recv(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(5 * time.Second):
		t.Fatal("no envelope arrived")
		return nil
	}
}

func noRecv(t *testing.T, out <-chan *envelope.Envelope, d time.Duration) {
	t.Helper()
	select {
	case env := <-out:
		t.Fatalf("unexpected envelope %q on %s", env.Body, env.Subject)
	case <-time.After(d):
	}
}

func settle(env *envelope.Envelope, status envelope.AckStatus) {
	env.Ack(context.Background(), envelope.Outcome{Status: status})
}

// eventuallyInflight waits until the broker holds n unacknowledged messages
// for the client.
func eventuallyInflight(t *testing.T, b *broker, clientID string, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return b.inflight(clientID) == n }, 5*time.Second, 5*time.Millisecond,
		"the broker holds %d unacknowledged messages for %s, want %d", b.inflight(clientID), clientID, n)
}

func TestSourceReceives(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSource(t, sourceConfig(b, v))
			require.Error(t, s.Health(context.Background()), "not started")
			out, _ := run(t, s)
			b.waitSubscribed(defaultClientID("in"), "orders/#")
			require.Eventually(t, func() bool { return s.Health(context.Background()) == nil }, 5*time.Second, 5*time.Millisecond)

			b.publish("orders/eu/1", `{"id":1}`, 1, false)
			env := recv(t, out)
			assert.Equal(t, "orders/eu/1", env.Subject)
			assert.JSONEq(t, `{"id":1}`, string(env.Body))
			assert.Equal(t, contentJSON, env.ContentType)
			assert.Equal(t, kind, env.Protocol)
			assert.Equal(t, "in", env.Source)
			assert.Len(t, env.ID, 26, "a ULID")
			assert.Equal(t, "orders/eu/1", env.Attributes["mqtt.topic"])
			assert.Equal(t, 1, env.Attributes["mqtt.qos"])
			assert.Equal(t, false, env.Attributes["mqtt.retained"])
			assert.Equal(t, false, env.Attributes["mqtt.duplicate"])
			assert.Positive(t, env.Attributes["mqtt.packet_id"])
			assert.Equal(t, "orders/eu/1", env.Attributes["ordering_key"])
			settle(env, envelope.Delivered)

			b.publish("orders/us/2", "plain", 0, false)
			env = recv(t, out)
			assert.Equal(t, contentText, env.ContentType)
			assert.Equal(t, 0, env.Attributes["mqtt.qos"])
			assert.Equal(t, 0, env.Attributes["mqtt.packet_id"])

			b.publish("orders/us/3", "\xff\xfe", 0, false)
			assert.Equal(t, contentBinary, recv(t, out).ContentType)

			b.publish("other/topic", "ignored", 1, false)
			noRecv(t, out, 100*time.Millisecond)
		})
	}
}

func TestSourceAcksOnlyOnceSettledFinal(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSource(t, sourceConfig(b, v, func(c *config.Source) { c.QoS = config.Ptr(2) }))
			out, _ := run(t, s)
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")

			statuses := []envelope.AckStatus{envelope.Delivered, envelope.DeadLettered, envelope.Unrouted, envelope.Filtered}
			for i, st := range statuses {
				b.publish(fmt.Sprintf("orders/%d", i), st.String(), 1, false)
				env := recv(t, out)
				// Not acknowledged until the relay settles it.
				time.Sleep(50 * time.Millisecond)
				assert.Equal(t, 1, b.inflight(id), "%s: acknowledged before it was settled", st)
				settle(env, st)
				eventuallyInflight(t, b, id, 0)
			}
		})
	}
}

func TestSourceAckAccepted(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSource(t, sourceConfig(b, v, func(c *config.Source) { c.Ack = "accepted" }))
			out, _ := run(t, s)
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")
			b.publish("orders/1", "x", 1, false)
			env := recv(t, out)
			assert.Equal(t, 1, b.inflight(id))
			env.Queued() // the relay holds it
			eventuallyInflight(t, b, id, 0)
		})
	}
}

func TestSourceRedeliversAfterFailedSettle(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSource(t, sourceConfig(b, v))
			out, _ := run(t, s)
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")

			b.publish("orders/1", "first", 1, false)
			b.publish("orders/2", "second", 1, false)
			first, second := recv(t, out), recv(t, out)
			require.Equal(t, "first", string(first.Body))
			require.Equal(t, "second", string(second.Body))

			// The second settles first but must wait for the first, which
			// fails: neither is acknowledged, and the source reconnects.
			settle(second, envelope.Delivered)
			time.Sleep(50 * time.Millisecond)
			assert.Equal(t, 2, b.inflight(id))
			settle(first, envelope.Failed)

			got := map[string]*envelope.Envelope{}
			for len(got) < 2 {
				env := recv(t, out)
				got[string(env.Body)] = env
			}
			for body, env := range got {
				assert.Equal(t, true, env.Attributes["mqtt.duplicate"], "%s is a redelivery", body)
				settle(env, envelope.Delivered)
			}
			eventuallyInflight(t, b, id, 0)
			assert.NoError(t, s.Health(context.Background()))
		})
	}
}

func TestSourceDropsMessagesAfterAFailureUntilItReconnects(t *testing.T) {
	b := startBroker(t)
	cfg := sourceConfig(b, version5, func(c *config.Source) {
		// A long backoff keeps the failed connection up for a while.
		c.Reconnect = &config.Reconnect{Backoff: config.Duration(400 * time.Millisecond), Jitter: "none"}
	})
	s := newTestSource(t, cfg)
	out, _ := run(t, s)
	id := defaultClientID("in")
	b.waitSubscribed(id, "orders/#")

	b.publish("orders/1", "fails", 1, false)
	settle(recv(t, out), envelope.Failed)
	// Arrives on the failed connection: not handed over (it would be
	// redelivered anyway), and not acknowledged.
	b.publish("orders/2", "held back", 1, false)
	b.publish("orders/3", "qos0 still flows", 0, false)
	env := recv(t, out)
	assert.Equal(t, "qos0 still flows", string(env.Body))

	got := map[string]bool{}
	for len(got) < 2 {
		env := recv(t, out)
		got[string(env.Body)] = true
		settle(env, envelope.Delivered)
	}
	assert.Equal(t, map[string]bool{"fails": true, "held back": true}, got)
	eventuallyInflight(t, b, id, 0)
}

func TestSourcePersistentSessionKeepsMessagesWhileDown(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			cfg := sourceConfig(b, v)
			_, stop := run(t, newTestSource(t, cfg))
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")
			stop()
			require.Eventually(t, func() bool { return !b.connected(id) }, 5*time.Second, 5*time.Millisecond)

			for i := range 3 {
				b.publish(fmt.Sprintf("orders/%d", i), fmt.Sprintf("while down %d", i), 1, false)
			}
			out, _ := run(t, newTestSource(t, cfg))
			var bodies []string
			for range 3 {
				env := recv(t, out)
				bodies = append(bodies, string(env.Body))
				settle(env, envelope.Delivered)
			}
			assert.ElementsMatch(t, []string{"while down 0", "while down 1", "while down 2"}, bodies)
			eventuallyInflight(t, b, id, 0)
		})
	}
}

func TestSourceKeepsAResumedSessionWhenItCannotSubscribeAgain(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			cfg := sourceConfig(b, v)
			_, stop := run(t, newTestSource(t, cfg))
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")
			stop()
			require.Eventually(t, func() bool { return !b.connected(id) }, 5*time.Second, 5*time.Millisecond)

			b.hook.set(func(h *testHook) { h.refuseSubscribe = true })
			b.publish("orders/1", "while down", 1, false)
			s := newTestSource(t, cfg)
			logs := captureLogs(s)
			out, _ := run(t, s)
			env := recv(t, out)
			assert.Equal(t, "while down", string(env.Body))
			settle(env, envelope.Delivered)
			b.publish("orders/2", "live", 1, false)
			env = recv(t, out)
			assert.Equal(t, "live", string(env.Body), "the session's subscription still delivers")
			settle(env, envelope.Delivered)
			eventuallyInflight(t, b, id, 0)
			// The session's messages can reach the test before the refused
			// SUBSCRIBE is answered and logged.
			require.Eventually(t, func() bool { return strings.Contains(logs.String(), "could not subscribe again") },
				5*time.Second, 5*time.Millisecond)

			// Without a session, a refused subscription is a failed connect.
			fresh := newTestSource(t, sourceConfig(b, v, func(c *config.Source) {
				c.ClientID = "fresh"
				c.Reconnect = &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 1}
			}))
			err := fresh.Start(context.Background(), make(chan *envelope.Envelope))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "subscribe to")
		})
	}
}

func TestSourceCleanSessionForgetsWhileDown(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			cfg := sourceConfig(b, v, func(c *config.Source) { c.CleanSession = config.Ptr(true) })
			_, stop := run(t, newTestSource(t, cfg))
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")
			stop()
			require.Eventually(t, func() bool { return !b.connected(id) }, 5*time.Second, 5*time.Millisecond)

			b.publish("orders/1", "lost", 1, false)
			out, _ := run(t, newTestSource(t, cfg))
			b.waitSubscribed(id, "orders/#")
			b.publish("orders/2", "after", 1, false)
			env := recv(t, out)
			assert.Equal(t, "after", string(env.Body))
			settle(env, envelope.Delivered)
		})
	}
}

func TestSourceSharedSubscriptionSplitsMessages(t *testing.T) {
	b := startBroker(t)
	var outs []<-chan *envelope.Envelope
	for _, id := range []string{"worker-a", "worker-b"} {
		s := newTestSource(t, sourceConfig(b, version5, func(c *config.Source) {
			c.ClientID, c.Group, c.Topic = id, "workers", "jobs/#"
		}))
		out, _ := run(t, s)
		outs = append(outs, out)
		b.waitSubscribed(id, "$share/workers/jobs/#")
	}
	const n = 100
	for i := range n {
		b.publish(fmt.Sprintf("jobs/%d", i), fmt.Sprint(i), 1, false)
	}
	seen := map[string]int{}
	per := make([]int, len(outs))
	deadline := time.After(10 * time.Second)
	for len(seen) < n {
		select {
		case env := <-outs[0]:
			seen[string(env.Body)]++
			per[0]++
			settle(env, envelope.Delivered)
		case env := <-outs[1]:
			seen[string(env.Body)]++
			per[1]++
			settle(env, envelope.Delivered)
		case <-deadline:
			t.Fatalf("only %d of %d messages arrived", len(seen), n)
		}
	}
	for body, c := range seen {
		assert.Equal(t, 1, c, "message %s delivered %d times", body, c)
	}
	assert.Positive(t, per[0], "worker-a got nothing")
	assert.Positive(t, per[1], "worker-b got nothing")
}

func TestSourceMQTT5Properties(t *testing.T) {
	b := startBroker(t)
	s := newTestSource(t, sourceConfig(b, version5))
	out, _ := run(t, s)
	b.waitSubscribed(defaultClientID("in"), "orders/#")
	pub := publisher(t, b)
	ctx := context.Background()

	_, err := pub.Publish(ctx, &paho.Publish{Topic: "orders/1", QoS: 1, Payload: []byte("<order/>"), Properties: &paho.PublishProperties{
		ContentType:     "application/xml",
		CorrelationData: []byte("corr-1"),
		User:            paho.UserProperties{{Key: "Idempotency-Key", Value: "order-42"}, {Key: "x-tenant", Value: "acme"}, {Key: "x-tenant", Value: "beta"}},
	}})
	require.NoError(t, err)
	env := recv(t, out)
	assert.Equal(t, "order-42", env.ID)
	assert.Equal(t, "application/xml", env.ContentType)
	assert.Equal(t, []string{"acme", "beta"}, env.Headers["X-Tenant"])
	assert.Equal(t, []string{"order-42"}, env.Headers["Idempotency-Key"])
	settle(env, envelope.Delivered)

	_, err = pub.Publish(ctx, &paho.Publish{Topic: "orders/2", QoS: 1, Payload: []byte("{}"),
		Properties: &paho.PublishProperties{CorrelationData: []byte("corr-2")}})
	require.NoError(t, err)
	env = recv(t, out)
	assert.Equal(t, "corr-2", env.ID)
	assert.Equal(t, contentJSON, env.ContentType)
	settle(env, envelope.Delivered)

	_, err = pub.Publish(ctx, &paho.Publish{Topic: "orders/3", QoS: 1, Payload: []byte("{}"),
		Properties: &paho.PublishProperties{CorrelationData: []byte{0xff, 0x00}}})
	require.NoError(t, err)
	env = recv(t, out)
	assert.Len(t, env.ID, 26, "binary correlation data is no ID")
	settle(env, envelope.Delivered)
}

func TestSourceRetained(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			b.publish("orders/config", "retained", 1, true)
			s := newTestSource(t, sourceConfig(b, v))
			out, _ := run(t, s)
			env := recv(t, out)
			assert.Equal(t, "retained", string(env.Body))
			assert.Equal(t, true, env.Attributes["mqtt.retained"])
			settle(env, envelope.Delivered)

			b.publish("orders/live", "live", 1, false)
			env = recv(t, out)
			assert.Equal(t, false, env.Attributes["mqtt.retained"])
			settle(env, envelope.Delivered)
		})
	}
}

func TestSourceDropsAndAcksOversizeMessages(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSource(t, sourceConfig(b, v, func(c *config.Source) { c.MaxBodyBytes = 8 }))
			out, _ := run(t, s)
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")

			b.publish("orders/big", strings.Repeat("x", 9), 1, false)
			b.publish("orders/small", "ok", 1, false)
			env := recv(t, out)
			assert.Equal(t, "ok", string(env.Body))
			settle(env, envelope.Delivered)
			eventuallyInflight(t, b, id, 0) // the big one was acknowledged too
			noRecv(t, out, 50*time.Millisecond)
		})
	}
}

func TestSourceReconnectsAfterBrokerRestart(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSource(t, sourceConfig(b, v))
			out, _ := run(t, s)
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")

			b.restart() // every session is gone: the source subscribes again
			b.waitSubscribed(id, "orders/#")
			require.Eventually(t, func() bool { return s.Health(context.Background()) == nil }, 5*time.Second, 5*time.Millisecond)
			b.publish("orders/1", "after restart", 1, false)
			env := recv(t, out)
			assert.Equal(t, "after restart", string(env.Body))
			settle(env, envelope.Delivered)
			eventuallyInflight(t, b, id, 0)
		})
	}
}

func TestSourceHealthReportsTheConnection(t *testing.T) {
	b := startBroker(t)
	s := newTestSource(t, sourceConfig(b, version5))
	assert.ErrorIs(t, s.Health(context.Background()), errNotStarted)
	run(t, s)
	b.waitSubscribed(defaultClientID("in"), "orders/#")
	require.Eventually(t, func() bool { return s.Health(context.Background()) == nil }, 5*time.Second, 5*time.Millisecond)
	b.stop()
	require.Eventually(t, func() bool { return s.Health(context.Background()) != nil }, 5*time.Second, 5*time.Millisecond)
}

func TestSourceCloseAcknowledgesInFlightFirst(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			s := newTestSource(t, sourceConfig(b, v))
			out := make(chan *envelope.Envelope, 8)
			ctx, cancel := context.WithCancel(context.Background())
			errc := make(chan error, 1)
			go func() { errc <- s.Start(ctx, out) }()
			id := defaultClientID("in")
			b.waitSubscribed(id, "orders/#")
			b.publish("orders/1", "in flight", 1, false)
			env := recv(t, out)

			cancel() // the relay stops reading; the message is still in flight
			require.NoError(t, <-errc)
			assert.True(t, b.connected(id), "the connection stays up for the acknowledgement")
			go func() {
				time.Sleep(100 * time.Millisecond)
				settle(env, envelope.Delivered)
			}()
			cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer ccancel()
			require.NoError(t, s.Close(cctx))
			// The broker reads the PUBACK before the DISCONNECT; it may not
			// have yet when Close returns. Unsent, it would stay at 1.
			eventuallyInflight(t, b, id, 0)
			require.Eventually(t, func() bool { return !b.connected(id) }, 5*time.Second, 5*time.Millisecond)
			assert.ErrorIs(t, s.Health(context.Background()), errStopped)
			assert.NoError(t, s.Close(cctx), "a second Close is a no-op")
		})
	}
}

func TestSourceCloseLeavesUnsettledForRedelivery(t *testing.T) {
	b := startBroker(t)
	cfg := sourceConfig(b, version5)
	s := newTestSource(t, cfg)
	out := make(chan *envelope.Envelope, 8)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Start(ctx, out) }()
	id := defaultClientID("in")
	b.waitSubscribed(id, "orders/#")
	b.publish("orders/1", "never settled", 1, false)
	recv(t, out)
	cancel()
	require.NoError(t, <-errc)
	cctx, ccancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer ccancel()
	_ = s.Close(cctx)
	assert.Equal(t, 1, b.inflight(id))

	out2, _ := run(t, newTestSource(t, cfg))
	env := recv(t, out2)
	assert.Equal(t, "never settled", string(env.Body))
	settle(env, envelope.Delivered)
	eventuallyInflight(t, b, id, 0)
}

func TestSourceFailsOverAndGivesUp(t *testing.T) {
	dead := closedAddr(t)
	t.Run("failover", func(t *testing.T) {
		b := startBroker(t)
		s := newTestSource(t, sourceConfig(b, version5, func(c *config.Source) {
			c.URL, c.FailoverURLs = "mqtt://"+dead, []string{b.url()}
		}))
		out, _ := run(t, s)
		b.waitSubscribed(defaultClientID("in"), "orders/#")
		b.publish("orders/1", "via failover", 0, false)
		assert.Equal(t, "via failover", string(recv(t, out).Body))
	})
	t.Run("max_attempts", func(t *testing.T) {
		for _, v := range versions {
			s := newTestSource(t, config.Source{URL: "mqtt://" + dead, Topic: "t", ProtocolVersion: v,
				Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 2}})
			err := s.Start(context.Background(), make(chan *envelope.Envelope))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "giving up after 2 failed connects")
			assert.Error(t, s.Health(context.Background()))
		}
	})
}

func TestSourceCredentials(t *testing.T) {
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			b := startBroker(t)
			b.hook.set(func(h *testHook) { h.user, h.pass = "relay", "s3cret-pw" })

			// The fields, then the URL's user info.
			for _, cfg := range []config.Source{
				sourceConfig(b, v, func(c *config.Source) { c.Username, c.Password = "relay", "s3cret-pw" }),
				sourceConfig(b, v, func(c *config.Source) { c.URL = "mqtt://relay:s3cret-pw@" + b.addr }),
			} {
				s := newTestSource(t, cfg)
				_, stop := run(t, s)
				b.waitSubscribed(defaultClientID("in"), "orders/#")
				stop()
			}

			s := newTestSource(t, sourceConfig(b, v, func(c *config.Source) {
				c.URL = "mqtt://relay:wrong-pw@" + b.addr
				c.Reconnect = &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 1}
			}))
			logs := captureLogs(s)
			err := s.Start(context.Background(), make(chan *envelope.Envelope))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "refused the connection")
			assert.NotContains(t, err.Error(), "wrong-pw")
			assert.NotContains(t, logs.String(), "wrong-pw")
		})
	}
}

// closedAddr is an address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}
