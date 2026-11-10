//go:build integration

package azure

// The Azure emulators: Service Bus (with Azure SQL Edge) and Event Hubs
// (with Azurite). The images are several GB and take minutes to start, so
// besides the integration tag the suites need HIKYAKU_AZURE_EMULATORS=1:
//
//	HIKYAKU_AZURE_EMULATORS=1 GOWORK=off go test -race -tags integration -run Emulator -timeout 30m ./...

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azure/eventhubs"
	"github.com/testcontainers/testcontainers-go/modules/azure/servicebus"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

const (
	sbEmulatorImage = "mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.1"
	ehEmulatorImage = "mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.2.1"
	sqlEdgeImage    = "mcr.microsoft.com/azure-sql-edge:latest"
	slow            = time.Minute
)

func requireEmulators(t *testing.T) {
	t.Helper()
	if os.Getenv("HIKYAKU_AZURE_EMULATORS") != "1" {
		t.Skip("set HIKYAKU_AZURE_EMULATORS=1 to run the Azure emulators (several GB of images, minutes to start)")
	}
}

// logs records what a source or destination logs.
type logs struct {
	mu      sync.Mutex
	records []string
}

func (l *logs) Enabled(context.Context, slog.Level) bool { return true }
func (l *logs) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	line := r.Level.String() + " " + r.Message
	r.Attrs(func(a slog.Attr) bool { line += " " + a.String(); return true })
	l.records = append(l.records, line)
	return nil
}
func (l *logs) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logs) WithGroup(string) slog.Handler      { return l }

func (l *logs) find(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, r := range l.records {
		if strings.Contains(r, sub) {
			out = append(out, r)
		}
	}
	return out
}

func within(t *testing.T, out <-chan *envelope.Envelope, d time.Duration) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(d):
		t.Fatalf("no envelope within %s", d)
		return nil
	}
}

func nothing(t *testing.T, out <-chan *envelope.Envelope, d time.Duration) {
	t.Helper()
	select {
	case env := <-out:
		t.Fatalf("unexpected envelope %s: %s", env.ID, env.Body)
	case <-time.After(d):
	}
}

func delivered(env *envelope.Envelope) {
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
}

func TestServiceBusEmulator(t *testing.T) {
	requireEmulators(t)
	ctx := context.Background()
	cfg, err := os.ReadFile("testdata/servicebus.json")
	require.NoError(t, err)
	c, err := servicebus.Run(ctx, sbEmulatorImage, servicebus.WithAcceptEULA(), servicebus.WithMSSQL(sqlEdgeImage),
		servicebus.WithConfig(bytes.NewReader(cfg)))
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	cs, err := c.ConnectionString(ctx)
	require.NoError(t, err)
	az := &config.Azure{ConnectionString: cs}
	log := &logs{}

	sink := func(t *testing.T, ep config.Endpoint) *sbSink {
		ep.Azure = az
		s, err := newSBSink(ctx, plugin.Spec{Name: "out", Kind: kindServiceBus, Config: &ep, Env: plugin.Env{Logger: slog.New(log)}})
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(context.Background()) })
		return s
	}
	source := func(t *testing.T, src config.Source) (*sbSource, <-chan *envelope.Envelope, func() error) {
		src.Azure = az
		s, err := newSBSource(ctx, plugin.Spec{Name: "in", Kind: kindServiceBus, Config: &src, Env: plugin.Env{Logger: slog.New(log)}})
		require.NoError(t, err)
		out, stop := run(t, s, 100)
		return s, out, stop
	}
	closeSource := func(t *testing.T, s *sbSource, stop func() error) {
		require.NoError(t, stop())
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, s.Close(cctx))
	}

	t.Run("round trip", func(t *testing.T) {
		snd := sink(t, config.Endpoint{Queue: "orders", Headers: map[string]string{"X-Team": "core"}})
		require.NoError(t, snd.Health(ctx))
		env := envelope.New("test", "http", []byte(`{"n":1}`))
		env.ID, env.Subject, env.ContentType = "rt-1", "orders.created", "application/json"
		env.SetHeader("X-Trace", "abc")
		res, err := snd.Deliver(ctx, env)
		require.NoError(t, err)
		assert.Contains(t, res.Endpoint, "/orders")

		src, out, stop := source(t, config.Source{Queue: "orders"})
		got := within(t, out, slow)
		assert.Equal(t, "rt-1", got.ID)
		assert.Equal(t, "orders.created", got.Subject)
		assert.Equal(t, "application/json", got.ContentType)
		assert.JSONEq(t, `{"n":1}`, string(got.Body))
		assert.Equal(t, "abc", got.Header("X-Trace"))
		assert.Equal(t, "core", got.Header("X-Team"))
		assert.Equal(t, "rt-1", got.Header("Idempotency-Key"))
		attrs := got.AttributesCopy()
		assert.Equal(t, "rt-1", attrs["servicebus.message_id"])
		assert.Equal(t, int64(1), attrs["servicebus.delivery_count"])
		assert.Equal(t, "orders", attrs["servicebus.entity"])
		assert.Contains(t, attrs, "servicebus.sequence_number")
		assert.Contains(t, attrs, "servicebus.enqueued_time")
		require.NoError(t, src.Health(ctx))
		delivered(got)
		nothing(t, out, 7*time.Second) // past the 5s lock: a completed message does not come back
		closeSource(t, src, stop)
	})

	t.Run("a failed message is redelivered", func(t *testing.T) {
		snd := sink(t, config.Endpoint{Queue: "orders"})
		env := envelope.New("test", "http", []byte("retry me"))
		env.ID = "fail-1"
		_, err := snd.Deliver(ctx, env)
		require.NoError(t, err)

		src, out, stop := source(t, config.Source{Queue: "orders"})
		got := within(t, out, slow)
		require.Equal(t, "fail-1", got.ID)
		got.Ack(ctx, envelope.Outcome{Status: envelope.Failed})
		again := within(t, out, slow)
		assert.Equal(t, "fail-1", again.ID, "abandoned: Service Bus delivers it again")
		count, _ := again.Attribute("servicebus.delivery_count")
		assert.Equal(t, int64(2), count)
		delivered(again)
		nothing(t, out, 7*time.Second)
		closeSource(t, src, stop)
	})

	t.Run("a slow message keeps its lock", func(t *testing.T) {
		snd := sink(t, config.Endpoint{Queue: "orders"})
		env := envelope.New("test", "http", []byte("slow"))
		env.ID = "slow-1"
		_, err := snd.Deliver(ctx, env)
		require.NoError(t, err)

		src, out, stop := source(t, config.Source{Queue: "orders"})
		got := within(t, out, slow)
		require.Equal(t, "slow-1", got.ID)
		nothing(t, out, 13*time.Second) // more than twice the 5s lock
		delivered(got)
		nothing(t, out, 7*time.Second)
		closeSource(t, src, stop)
		assert.Empty(t, log.find("complete failed"), "the lock was renewed, so the message completed")
	})

	t.Run("duplicate detection", func(t *testing.T) {
		snd := sink(t, config.Endpoint{Queue: "orders"})
		for range 2 {
			env := envelope.New("test", "http", []byte("once"))
			env.ID = "dup-1"
			_, err := snd.Deliver(ctx, env)
			require.NoError(t, err)
		}
		src, out, stop := source(t, config.Source{Queue: "orders"})
		delivered(within(t, out, slow))
		nothing(t, out, 3*time.Second)
		closeSource(t, src, stop)
	})

	t.Run("batches", func(t *testing.T) {
		snd := sink(t, config.Endpoint{Queue: "batches"})
		var envs []*envelope.Envelope
		for i := range 50 {
			env := envelope.New("test", "http", []byte(fmt.Sprintf(`{"i":%d}`, i)))
			env.ID = fmt.Sprintf("b-%02d", i)
			envs = append(envs, env)
		}
		_, err := snd.DeliverBatch(ctx, envs)
		require.NoError(t, err)

		src, out, stop := source(t, config.Source{Queue: "batches", Prefetch: 16})
		var ids []string
		for range 50 {
			got := within(t, out, slow)
			ids = append(ids, got.ID)
			delivered(got)
		}
		slices.Sort(ids)
		assert.Equal(t, "b-00", ids[0])
		assert.Equal(t, "b-49", ids[49])
		assert.Len(t, slices.Compact(ids), 50)
		closeSource(t, src, stop)
	})

	t.Run("topic subscription", func(t *testing.T) {
		snd := sink(t, config.Endpoint{Topic: "events"})
		env := envelope.New("test", "http", []byte("published"))
		env.ID = "topic-1"
		_, err := snd.Deliver(ctx, env)
		require.NoError(t, err)
		src, out, stop := source(t, config.Source{Topic: "events", Subscription: "audit"})
		got := within(t, out, slow)
		assert.Equal(t, "topic-1", got.ID)
		assert.Equal(t, "events", got.Subject)
		entity, _ := got.Attribute("servicebus.entity")
		assert.Equal(t, "events/subscriptions/audit", entity)
		delivered(got)
		closeSource(t, src, stop)
	})

	t.Run("sessions", func(t *testing.T) {
		snd := sink(t, config.Endpoint{Queue: "sessions", GroupID: "{{ .json.s }}"})
		var envs []*envelope.Envelope
		for i, s := range []string{"a", "b", "a", "b", "a"} {
			env := envelope.New("test", "http", []byte(fmt.Sprintf(`{"s":%q,"i":%d}`, s, i)))
			env.ID = fmt.Sprintf("s-%d", i)
			envs = append(envs, env)
		}
		_, err := snd.DeliverBatch(ctx, envs)
		require.NoError(t, err)

		src, out, stop := source(t, config.Source{Queue: "sessions"})
		bySession := map[string][]string{}
		for range 5 {
			got := within(t, out, slow)
			sid, _ := got.Attribute("servicebus.session_id")
			key, _ := got.Attribute("ordering_key")
			assert.Equal(t, sid, key)
			bySession[sid.(string)] = append(bySession[sid.(string)], got.ID)
			delivered(got)
		}
		assert.Equal(t, map[string][]string{"a": {"s-0", "s-2", "s-4"}, "b": {"s-1", "s-3"}}, bySession,
			"each session in order")
		assert.NotEmpty(t, log.find("requires sessions"))
		nothing(t, out, 3*time.Second)
		closeSource(t, src, stop)
	})

	t.Run("a missing entity", func(t *testing.T) {
		snd := sink(t, config.Endpoint{Queue: "missing"})
		_, err := snd.Deliver(ctx, envelope.New("test", "http", []byte("x")))
		require.Error(t, err)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, "%v (tags %v)", err, plugin.Tags(err))
		assert.Error(t, snd.Health(ctx))
		assert.NotContains(t, err.Error(), "SAS_KEY_VALUE")
	})
}

func TestEventHubsEmulator(t *testing.T) {
	requireEmulators(t)
	ctx := context.Background()
	cfg, err := os.ReadFile("testdata/eventhubs.json")
	require.NoError(t, err)
	c, err := eventhubs.Run(ctx, ehEmulatorImage, eventhubs.WithAcceptEULA(), eventhubs.WithConfig(bytes.NewReader(cfg)))
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	cs, err := c.ConnectionString(ctx)
	require.NoError(t, err)
	az := &config.Azure{ConnectionString: cs}
	mem := store.NewMemory("checkpoints")
	env := plugin.Env{Logger: slog.New(&logs{}), Store: func(string) (store.Store, error) { return mem, nil }}

	snd, err := newEHSink(ctx, plugin.Spec{Name: "out", Kind: kindEventHubs, Env: env,
		Config: &config.Endpoint{Topic: "telemetry", Key: "{{ .json.device }}", Azure: az}})
	require.NoError(t, err)
	defer func() { _ = snd.Close(ctx) }()
	require.NoError(t, snd.Health(ctx))
	send := func(from, to int) {
		var envs []*envelope.Envelope
		for i := from; i < to; i++ {
			e := envelope.New("test", "http", []byte(fmt.Sprintf(`{"device":"d%d","i":%d}`, i%3, i)))
			e.ID = fmt.Sprintf("ev-%02d", i)
			e.ContentType = "application/json"
			envs = append(envs, e)
		}
		_, err := snd.Deliver(ctx, envs[0])
		require.NoError(t, err)
		_, err = snd.DeliverBatch(ctx, envs[1:])
		require.NoError(t, err)
	}
	send(0, 20)

	newSource := func() *ehSource {
		s, err := newEHSource(ctx, plugin.Spec{Name: "in", Kind: kindEventHubs, Env: env,
			Config: &config.Source{Topic: "telemetry", Group: "relay", Start: "earliest", Azure: az}})
		require.NoError(t, err)
		return s
	}
	src := newSource()
	out, stop := run(t, src, 100)
	partitionOf := map[string]string{}
	last := map[string]int64{}
	var ids []string
	for range 20 {
		got := within(t, out, slow)
		ids = append(ids, got.ID)
		assert.Equal(t, "telemetry", got.Subject)
		assert.Equal(t, "application/json", got.ContentType)
		p, _ := got.Attribute("eventhubs.partition")
		key, _ := got.Attribute("ordering_key")
		assert.Equal(t, p, key)
		device := string(got.Body[len(`{"device":"`) : len(`{"device":"`)+2])
		if prev, ok := partitionOf[device]; ok {
			assert.Equal(t, prev, p, "one partition key, one partition")
		}
		partitionOf[device] = p.(string)
		seq, _ := got.Attribute("eventhubs.sequence_number")
		last[p.(string)] = max(last[p.(string)], seq.(int64))
		assert.Equal(t, got.ID, got.Header("Idempotency-Key"))
		delivered(got)
	}
	slices.Sort(ids)
	assert.Len(t, slices.Compact(ids), 20)
	require.Eventually(t, func() bool {
		for p, seq := range last {
			if v, ok := checkpointOf(t, src, p); !ok || v < seq-1 {
				return false
			}
		}
		return true
	}, slow, 50*time.Millisecond, "the checkpoints close in on the last events")
	require.NoError(t, src.Health(ctx))
	require.NoError(t, stop())
	require.NoError(t, src.Close(ctx))
	for p, seq := range last {
		v, _ := checkpointOf(t, src, p)
		assert.Equal(t, seq, v, "Close writes partition %s's last event", p)
	}

	// A restart of the same replica reads only what came after.
	send(20, 25)
	src2 := newSource()
	src2.leases.id = src.leases.id
	out, stop = run(t, src2, 100)
	ids = nil
	for range 5 {
		got := within(t, out, slow)
		ids = append(ids, got.ID)
		delivered(got)
	}
	slices.Sort(ids)
	assert.Equal(t, []string{"ev-20", "ev-21", "ev-22", "ev-23", "ev-24"}, ids)
	nothing(t, out, 3*time.Second)
	require.NoError(t, stop())
	require.NoError(t, src2.Close(ctx))
}
