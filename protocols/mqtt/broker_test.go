package mqtt

import (
	"bytes"
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// versions are the protocol versions every broker test runs with.
var versions = []string{version5, version311}

// broker is an embedded MQTT broker (3.1.1 and 5) on a loopback port.
type broker struct {
	t    *testing.T
	hook *testHook
	tls  *tls.Config
	ws   bool // a WebSocket listener instead of TCP

	mu      sync.Mutex
	srv     *mochi.Server
	addr    string
	stopped bool // srv is closed (mochi's Close must run once)
}

// startBroker runs a broker on a free loopback port.
func startBroker(t *testing.T) *broker {
	t.Helper()
	b := &broker{t: t, hook: &testHook{}}
	b.start("127.0.0.1:0")
	t.Cleanup(b.stop)
	return b
}

// startTLSBroker runs a broker that only speaks TLS.
func startTLSBroker(t *testing.T, cfg *tls.Config) *broker {
	t.Helper()
	b := &broker{t: t, hook: &testHook{}, tls: cfg}
	b.start("127.0.0.1:0")
	t.Cleanup(b.stop)
	return b
}

// startWSBroker runs a broker behind a WebSocket listener (wss with cfg).
func startWSBroker(t *testing.T, cfg *tls.Config) *broker {
	t.Helper()
	b := &broker{t: t, hook: &testHook{}, tls: cfg, ws: true}
	b.start(closedAddr(t)) // mochi's WebSocket listener needs a real port
	t.Cleanup(b.stop)
	return b
}

func (b *broker) start(addr string) {
	b.t.Helper()
	srv := mochi.New(&mochi.Options{InlineClient: true, Logger: slog.New(slog.DiscardHandler)})
	require.NoError(b.t, srv.AddHook(b.hook, nil))
	var l listeners.Listener = listeners.NewTCP(listeners.Config{ID: "tcp", Address: addr, TLSConfig: b.tls})
	if b.ws {
		l = listeners.NewWebsocket(listeners.Config{ID: "ws", Address: addr, TLSConfig: b.tls})
	}
	require.NoError(b.t, srv.AddListener(l))
	require.NoError(b.t, srv.Serve())
	b.mu.Lock()
	b.srv, b.addr, b.stopped = srv, l.Address(), false
	b.mu.Unlock()
	if b.ws { // it starts listening in the background
		require.Eventually(b.t, func() bool {
			c, err := net.Dial("tcp", l.Address())
			if err == nil {
				_ = c.Close()
			}
			return err == nil
		}, 5*time.Second, 5*time.Millisecond)
	}
}

func (b *broker) stop() {
	b.mu.Lock()
	srv, stopped := b.srv, b.stopped
	b.stopped = true
	b.mu.Unlock()
	if !stopped {
		_ = srv.Close()
	}
}

// restart replaces the broker with a new one on the same address: every
// connection drops and every session is gone.
func (b *broker) restart() {
	b.t.Helper()
	b.stop()
	b.mu.Lock()
	addr := b.addr
	b.mu.Unlock()
	require.Eventually(b.t, func() bool {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return false
		}
		_ = l.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)
	b.start(addr)
}

func (b *broker) server() *mochi.Server {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.srv
}

func (b *broker) url() string {
	switch {
	case b.ws && b.tls != nil:
		return "wss://" + b.addr + "/mqtt"
	case b.ws:
		return "ws://" + b.addr + "/mqtt"
	case b.tls != nil:
		return "mqtts://" + b.addr
	}
	return "mqtt://" + b.addr
}

// publish publishes from the broker's inline client.
func (b *broker) publish(topic, payload string, qos byte, retain bool) {
	b.t.Helper()
	require.NoError(b.t, b.server().Publish(topic, []byte(payload), retain, qos))
}

// waitSubscribed waits until client is connected and subscribed to filter.
func (b *broker) waitSubscribed(clientID, filter string) {
	b.t.Helper()
	require.Eventually(b.t, func() bool {
		cl, ok := b.server().Clients.Get(clientID)
		if !ok || cl.Closed() {
			return false
		}
		_, ok = cl.State.Subscriptions.Get(filter)
		return ok
	}, 5*time.Second, 5*time.Millisecond, "client %s never subscribed to %s", clientID, filter)
}

// inflight is how many QoS 1 and 2 messages the broker holds unacknowledged
// for a client; -1 when it has no session for it.
func (b *broker) inflight(clientID string) int {
	cl, ok := b.server().Clients.Get(clientID)
	if !ok {
		return -1
	}
	return cl.State.Inflight.Len()
}

// connected reports whether the client has a live connection.
func (b *broker) connected(clientID string) bool {
	cl, ok := b.server().Clients.Get(clientID)
	return ok && !cl.Closed()
}

// subscribe records what the broker routes to filter, through an inline
// subscription.
func (b *broker) subscribe(filter string) *received {
	b.t.Helper()
	r := &received{}
	require.NoError(b.t, b.server().Subscribe(filter, int(subIDs.Add(1)), func(_ *mochi.Client, _ packets.Subscription, pk packets.Packet) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.packets = append(r.packets, pk.Copy(true))
	}))
	return r
}

var subIDs atomic.Int64

// received collects the packets of an inline subscription.
type received struct {
	mu      sync.Mutex
	packets []packets.Packet
}

func (r *received) wait(t *testing.T, n int) []packets.Packet {
	t.Helper()
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.packets) >= n
	}, 5*time.Second, 5*time.Millisecond, "fewer than %d messages arrived", n)
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]packets.Packet(nil), r.packets...)
}

// testHook authenticates, authorizes and can refuse publishes.
type testHook struct {
	mochi.HookBase
	mu         sync.Mutex
	user, pass string
	denyTopic  string        // publishing here is not authorized
	refuse     *packets.Code // every publish is answered with this
	// hold, when set, keeps every publish unanswered until it closes; the
	// publishing client's ID is sent on entered first.
	hold    chan struct{}
	entered chan string
	// refuseSubscribe answers every SUBSCRIBE with a failure.
	refuseSubscribe bool
}

func (h *testHook) OnSubscribe(_ *mochi.Client, pk packets.Packet) packets.Packet {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.refuseSubscribe {
		pk.Filters = append(packets.Subscriptions(nil), pk.Filters...)
		for i := range pk.Filters {
			pk.Filters[i].Filter = "#/invalid" // refused as an invalid filter
		}
	}
	return pk
}

func (h *testHook) ID() string { return "test" }

func (h *testHook) Provides(b byte) bool {
	return bytes.Contains([]byte{mochi.OnConnectAuthenticate, mochi.OnACLCheck, mochi.OnPublish, mochi.OnSubscribe}, []byte{b})
}

func (h *testHook) OnConnectAuthenticate(_ *mochi.Client, pk packets.Packet) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.user == "" {
		return true
	}
	return string(pk.Connect.Username) == h.user && string(pk.Connect.Password) == h.pass
}

func (h *testHook) OnACLCheck(_ *mochi.Client, topic string, write bool) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !write || topic != h.denyTopic
}

func (h *testHook) OnPublish(cl *mochi.Client, pk packets.Packet) (packets.Packet, error) {
	h.mu.Lock()
	refuse, hold, entered := h.refuse, h.hold, h.entered
	h.mu.Unlock()
	if hold != nil && !cl.Net.Inline {
		select {
		case entered <- cl.ID:
		default:
		}
		<-hold
	}
	if refuse != nil {
		return pk, *refuse
	}
	return pk, nil
}

func (h *testHook) set(f func(h *testHook)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f(h)
}

// logBuffer is a goroutine-safe log sink.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// debugLogger logs everything to a buffer.
func debugLogger() (*slog.Logger, *logBuffer) {
	buf := &logBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// captureLogs points a source's logger at a buffer.
func captureLogs(s *Source) *logBuffer {
	l, buf := debugLogger()
	s.log = l
	return buf
}

// publisher is a plain MQTT 5 client that publishes with properties.
func publisher(t *testing.T, b *broker) *paho.Client {
	t.Helper()
	conn, err := net.Dial("tcp", b.addr)
	require.NoError(t, err)
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.Connect(ctx, &paho.Connect{ClientID: "test-publisher", CleanStart: true, KeepAlive: 30})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })
	return c
}
