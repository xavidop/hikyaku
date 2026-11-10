package azure

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

func newTestEHSource(t *testing.T, src config.Source, hub ehConsumer, st store.Store) *ehSource {
	t.Helper()
	if src.Azure == nil {
		src.Azure = connStr()
	}
	if src.Topic == "" {
		src.Topic = "telemetry"
	}
	spec := plugin.Spec{Name: "hub", Kind: kindEventHubs, Config: &src,
		Env: plugin.Env{Store: func(string) (store.Store, error) { return st, nil }}}
	opts, err := parseEHSource(spec)
	require.NoError(t, err)
	s := buildEHSource(spec, opts, hub)
	s.flushEvery, s.leaseTick, s.batchWait = 10*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond
	return s
}

// checkpointOf reads a partition's checkpoint as a new owner would.
func checkpointOf(t *testing.T, s *ehSource, partition string) (int64, bool) {
	t.Helper()
	return read(t, s.cps, partition)
}

func ackAll(envs []*envelope.Envelope, status envelope.AckStatus) {
	for _, env := range envs {
		env.Ack(context.Background(), envelope.Outcome{Status: status})
	}
}

func collect(t *testing.T, out <-chan *envelope.Envelope, n int) []*envelope.Envelope {
	t.Helper()
	envs := make([]*envelope.Envelope, 0, n)
	for range n {
		envs = append(envs, next(t, out))
	}
	return envs
}

func TestEHSourceReadsAndResumes(t *testing.T) {
	mem := store.NewMemory("m")
	hub := newFakeHub("0", "1")
	hub.publish("0", "a", "b", "c")
	hub.publish("1", "d")
	s := newTestEHSource(t, config.Source{Start: "earliest"}, hub, mem)
	out, stop := run(t, s, 10)

	envs := collect(t, out, 4)
	bodies := map[string]bool{}
	for _, env := range envs {
		bodies[string(env.Body)] = true
	}
	assert.Equal(t, map[string]bool{"a": true, "b": true, "c": true, "d": true}, bodies)
	ackAll(envs, envelope.Delivered)
	require.Eventually(t, func() bool {
		v0, _ := checkpointOf(t, s, "0")
		return v0 == 1
	}, waitFor, tick, "while it runs, the checkpoint closes in on the last final event")

	require.NoError(t, stop())
	require.NoError(t, s.Close(context.Background()))
	v0, _ := checkpointOf(t, s, "0")
	v1, _ := checkpointOf(t, s, "1")
	assert.Equal(t, []int64{2, 0}, []int64{v0, v1}, "Close writes each partition's last final event")
	hub.mu.Lock()
	assert.True(t, hub.closed)
	hub.mu.Unlock()
	for _, op := range hub.openings() {
		assert.NotNil(t, op.opts.StartPosition.Earliest, "no checkpoint and start: earliest")
	}

	// The same replica restarts: it reads on after the checkpoints.
	hub.publish("0", "e")
	s2 := newTestEHSource(t, config.Source{Start: "earliest"}, hub, mem)
	s2.leases.id = s.leases.id
	out, stop = run(t, s2, 10)
	env := next(t, out)
	assert.Equal(t, "e", string(env.Body), "nothing settled is read again")
	select {
	case env := <-out:
		t.Fatalf("read %q again", env.Body)
	case <-time.After(50 * time.Millisecond):
	}
	starts := map[string]int64{}
	for _, op := range hub.openings()[2:] {
		require.NotNil(t, op.opts.StartPosition.SequenceNumber)
		assert.False(t, op.opts.StartPosition.Inclusive)
		starts[op.partition] = *op.opts.StartPosition.SequenceNumber
		require.NotNil(t, op.opts.OwnerLevel)
	}
	assert.Equal(t, map[string]int64{"0": 2, "1": 0}, starts)
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	require.NoError(t, stop())
	require.NoError(t, s2.Close(context.Background()))
	v, _ := checkpointOf(t, s2, "0")
	assert.Equal(t, int64(3), v, "Close writes the last checkpoint")
}

// hubView is one replica's connection to a shared fake hub: it knows the
// partitions this replica has open.
type hubView struct {
	*fakeHub
	mu      sync.Mutex
	clients []*fakePartition
}

func (v *hubView) newPartitionClient(p string, o *azeventhubs.PartitionClientOptions) (ehPartition, error) {
	pc, err := v.fakeHub.newPartitionClient(p, o)
	if err == nil {
		v.mu.Lock()
		v.clients = append(v.clients, pc.(*fakePartition))
		v.mu.Unlock()
	}
	return pc, err
}

func (v *hubView) reading() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []string
	for _, pc := range v.clients {
		if !pc.closed.Load() && !slices.Contains(out, pc.partition) {
			out = append(out, pc.partition)
		}
	}
	slices.Sort(out)
	return out
}

func TestEHSourceReplicasSharePartitions(t *testing.T) {
	mem := store.NewMemory("m")
	hub := newFakeHub("0", "1", "2", "3")
	a, b := &hubView{fakeHub: hub}, &hubView{fakeHub: hub}
	srcA := newTestEHSource(t, config.Source{}, a, mem)
	srcB := newTestEHSource(t, config.Source{}, b, mem)
	for _, s := range []*ehSource{srcA, srcB} {
		s.leases.epoch = 300 * time.Millisecond
	}
	_, stopA := run(t, srcA, 10)
	defer func() { _ = stopA() }()
	require.Eventually(t, func() bool { return len(a.reading()) == 4 }, waitFor, tick, "a lone replica reads every partition")

	_, stopB := run(t, srcB, 10)
	require.Eventually(t, func() bool {
		ra, rb := a.reading(), b.reading()
		return len(ra) == 2 && len(rb) == 2 && !slices.ContainsFunc(ra, func(p string) bool { return slices.Contains(rb, p) })
	}, 10*time.Second, 10*time.Millisecond, "two replicas read two partitions each, never the same one")

	require.NoError(t, stopB())
	require.NoError(t, srcB.Close(context.Background()))
	require.Eventually(t, func() bool { return len(a.reading()) == 4 }, 10*time.Second, 10*time.Millisecond,
		"the partitions of a replica that stopped move to the others")
}

func TestEHSourceOwnerLevelsGrow(t *testing.T) {
	mem := store.NewMemory("m")
	hub := newFakeHub("0")
	s := newTestEHSource(t, config.Source{}, hub, mem)
	_, stop := run(t, s, 10)
	require.Eventually(t, func() bool { return len(hub.openings()) == 1 }, waitFor, tick)
	require.NoError(t, stop())
	require.NoError(t, s.Close(context.Background()))

	s2 := newTestEHSource(t, config.Source{}, hub, mem)
	s2.leases.id = s.leases.id
	_, stop = run(t, s2, 10)
	require.Eventually(t, func() bool { return len(hub.openings()) == 2 }, waitFor, tick)
	require.NoError(t, stop())
	require.NoError(t, s2.Close(context.Background()))
	ops := hub.openings()
	assert.Greater(t, *ops[1].opts.OwnerLevel, *ops[0].opts.OwnerLevel,
		"a new owner opens the partition above the previous one, so Event Hubs disconnects it")
}

func TestEHSourceStartPositions(t *testing.T) {
	ctx := context.Background()
	t.Run("latest is pinned at the first read", func(t *testing.T) {
		mem := store.NewMemory("m")
		hub := newFakeHub("0")
		hub.publish("0", "old1", "old2")
		s := newTestEHSource(t, config.Source{}, hub, mem)
		out, stop := run(t, s, 10)
		require.Eventually(t, func() bool { return len(hub.openings()) == 1 }, waitFor, tick)
		op := hub.openings()[0]
		require.NotNil(t, op.opts.StartPosition.SequenceNumber)
		assert.Equal(t, int64(1), *op.opts.StartPosition.SequenceNumber, "after the last event")
		hub.publish("0", "new")
		env := next(t, out)
		assert.Equal(t, "new", string(env.Body))
		require.NoError(t, stop())
		cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		require.NoError(t, s.Close(cctx))

		// It crashed before "new" was settled: a restart still starts after
		// old2, not at the latest event, so "new" is read again.
		s2 := newTestEHSource(t, config.Source{}, hub, mem)
		s2.leases.id = s.leases.id
		out, stop = run(t, s2, 10)
		assert.Equal(t, "new", string(next(t, out).Body))
		require.NoError(t, stop())
	})
	t.Run("a checkpoint past the last event is reset", func(t *testing.T) {
		mem := store.NewMemory("m")
		hub := newFakeHub("0")
		hub.publish("0", "x", "y", "z")
		s := newTestEHSource(t, config.Source{}, hub, mem)
		cp, err := s.cps.alloc(ctx, "0")
		require.NoError(t, err)
		require.NoError(t, cp.begin(ctx, 100))
		_, stop := run(t, s, 10)
		require.Eventually(t, func() bool { return len(hub.openings()) == 1 }, waitFor, tick)
		assert.Equal(t, int64(2), *hub.openings()[0].opts.StartPosition.SequenceNumber, "start: latest")
		require.NoError(t, stop())
	})
	t.Run("an empty partition starts at the beginning", func(t *testing.T) {
		hub := newFakeHub("0")
		s := newTestEHSource(t, config.Source{}, hub, store.NewMemory("m"))
		out, stop := run(t, s, 10)
		require.Eventually(t, func() bool { return len(hub.openings()) == 1 }, waitFor, tick)
		assert.NotNil(t, hub.openings()[0].opts.StartPosition.Earliest)
		hub.publish("0", "first")
		assert.Equal(t, "first", string(next(t, out).Body))
		require.NoError(t, stop())
	})
}

func TestEHSourceFailedEventHoldsCheckpoint(t *testing.T) {
	hub := newFakeHub("0")
	hub.publish("0", "e0", "e1", "e2", "e3")
	s := newTestEHSource(t, config.Source{Start: "earliest"}, hub, store.NewMemory("m"))
	out, stop := run(t, s, 10)
	envs := collect(t, out, 4)
	ctx := context.Background()
	envs[0].Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	envs[1].Ack(ctx, envelope.Outcome{Status: envelope.Failed})
	envs[2].Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	envs[3].Ack(ctx, envelope.Outcome{Status: envelope.DeadLettered})

	// Events after it go on being read.
	hub.publish("0", "e4")
	e4 := next(t, out)
	assert.Equal(t, "e4", string(e4.Body))
	e4.Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	time.Sleep(50 * time.Millisecond)
	v, _ := checkpointOf(t, s, "0")
	assert.LessOrEqual(t, v, int64(0))
	require.NoError(t, stop())
	require.NoError(t, s.Close(ctx))
	v, _ = checkpointOf(t, s, "0")
	assert.Equal(t, int64(0), v, "the failed event holds the checkpoint, so it is read again after a restart")
}

func TestEHSourceEnvelope(t *testing.T) {
	hub := newFakeHub("7")
	at := time.Date(2026, 11, 9, 1, 2, 3, 0, time.UTC)
	hub.mu.Lock()
	hub.logs["7"] = []*azeventhubs.ReceivedEventData{
		{EventData: azeventhubs.EventData{Body: []byte(`{"t":1}`), MessageID: ptr("mid"), Properties: map[string]any{"site": "lab", "n": 3},
			CorrelationID: "corr"}, SequenceNumber: 0, Offset: "100", EnqueuedTime: &at, PartitionKey: ptr("dev-1")},
		{EventData: azeventhubs.EventData{Body: []byte("x"), ContentType: ptr("text/x"), Properties: map[string]any{"Idempotency-Key": "k-9"}},
			SequenceNumber: 1, Offset: "200"},
		{EventData: azeventhubs.EventData{Body: []byte{0xff, 0x00}}, SequenceNumber: 2, Offset: "300"},
		{RawAMQPMessage: &azeventhubs.AMQPAnnotatedMessage{Body: azeventhubs.AMQPAnnotatedMessageBody{Value: map[string]any{"v": 1}}},
			SequenceNumber: 3, Offset: "400"},
	}
	hub.mu.Unlock()
	s := newTestEHSource(t, config.Source{Start: "earliest"}, hub, store.NewMemory("m"))
	out, stop := run(t, s, 10)
	defer func() { _ = stop() }()
	envs := collect(t, out, 4)

	env := envs[0]
	assert.Equal(t, "mid", env.ID)
	assert.Equal(t, "telemetry", env.Subject)
	assert.Equal(t, kindEventHubs, env.Protocol)
	assert.Equal(t, contentJSON, env.ContentType)
	assert.Equal(t, map[string][]string{"site": {"lab"}, "n": {"3"}}, env.Headers)
	assert.Equal(t, map[string]any{
		"eventhubs.partition": "7", "eventhubs.sequence_number": int64(0), "eventhubs.offset": "100",
		"eventhubs.enqueued_time": "2026-11-09T01:02:03Z", "eventhubs.partition_key": "dev-1",
		"eventhubs.correlation_id": "corr", "ordering_key": "7",
	}, env.Attributes)

	assert.Equal(t, "k-9", envs[1].ID, "no MessageId: the Idempotency-Key")
	assert.Equal(t, "text/x", envs[1].ContentType)
	assert.Equal(t, "7-2", envs[2].ID, "neither: <partition>-<sequence number>")
	assert.Equal(t, contentBinary, envs[2].ContentType)
	assert.JSONEq(t, `{"v":1}`, string(envs[3].Body))
}

func TestEHSourceSkipsOversizeEvents(t *testing.T) {
	hub := newFakeHub("0")
	hub.publish("0", "tiny", "this one is too large", "ok")
	s := newTestEHSource(t, config.Source{Start: "earliest", MaxBodyBytes: 4}, hub, store.NewMemory("m"))
	out, stop := run(t, s, 10)
	envs := collect(t, out, 2)
	assert.Equal(t, "tiny", string(envs[0].Body))
	assert.Equal(t, "ok", string(envs[1].Body))
	ackAll(envs, envelope.Delivered)
	require.NoError(t, stop())
	require.NoError(t, s.Close(context.Background()))
	v, _ := checkpointOf(t, s, "0")
	assert.Equal(t, int64(2), v, "the checkpoint passes the oversized event, or it would come back forever")
}

func TestEHSourceAckAccepted(t *testing.T) {
	hub := newFakeHub("0")
	hub.publish("0", "a", "b", "c")
	s := newTestEHSource(t, config.Source{Start: "earliest", Ack: "accepted"}, hub, store.NewMemory("m"))
	out, stop := run(t, s, 10)
	for _, env := range collect(t, out, 3) {
		env.Queued()
	}
	require.Eventually(t, func() bool { v, _ := checkpointOf(t, s, "0"); return v == 1 }, waitFor, tick,
		"an event the relay holds is final")
	require.NoError(t, stop())
	require.NoError(t, s.Close(context.Background()))
	v, _ := checkpointOf(t, s, "0")
	assert.Equal(t, int64(2), v)
}

func TestEHSourceOwnershipLost(t *testing.T) {
	hub := newFakeHub("0")
	s := newTestEHSource(t, config.Source{}, hub, store.NewMemory("m"))
	s.leases.epoch = 200 * time.Millisecond
	_, stop := run(t, s, 10)
	defer func() { _ = stop() }()
	require.Eventually(t, func() bool { return len(hub.openings()) == 1 }, waitFor, tick)
	hub.mu.Lock()
	pc := hub.clients[0]
	hub.mu.Unlock()
	pc.errs <- &azeventhubs.Error{Code: azeventhubs.ErrorCodeOwnershipLost}
	require.Eventually(t, pc.closed.Load, waitFor, tick, "the reader stops")

	// It is not read again before the next epoch, then it is, above the
	// owner level that took it.
	require.Eventually(t, func() bool { return len(hub.openings()) == 2 }, waitFor, tick)
	ops := hub.openings()
	assert.Greater(t, *ops[1].opts.OwnerLevel, *ops[0].opts.OwnerLevel)
}

func TestEHSourceReconnectsAfterErrors(t *testing.T) {
	hub := newFakeHub("0")
	s := newTestEHSource(t, config.Source{Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond),
		MaxBackoff: config.Duration(2 * time.Millisecond)}}, hub, store.NewMemory("m"))
	out, stop := run(t, s, 10)
	defer func() { _ = stop() }()
	require.Eventually(t, func() bool { return len(hub.openings()) == 1 }, waitFor, tick)
	hub.publish("0", "a")
	first := next(t, out)
	hub.mu.Lock()
	pc := hub.clients[0]
	hub.mu.Unlock()
	pc.errs <- &azeventhubs.Error{Code: azeventhubs.ErrorCodeConnectionLost}
	require.Eventually(t, func() bool { return len(hub.openings()) == 2 }, waitFor, tick, "the partition is opened again")
	op := hub.openings()[1]
	require.NotNil(t, op.opts.StartPosition.SequenceNumber)
	assert.Equal(t, int64(0), *op.opts.StartPosition.SequenceNumber, "after the last event received")
	assert.Equal(t, *hub.openings()[0].opts.OwnerLevel, *op.opts.OwnerLevel, "the same owner")
	hub.publish("0", "b")
	assert.Equal(t, "b", string(next(t, out).Body))
	first.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	require.NoError(t, s.Health(context.Background()))
}

func TestEHSourceHealthAndGivingUp(t *testing.T) {
	ctx := context.Background()
	hub := newFakeHub("0")
	s := newTestEHSource(t, config.Source{Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond),
		MaxBackoff: config.Duration(time.Millisecond), MaxAttempts: 2}}, hub, store.NewMemory("m"))
	assert.ErrorIs(t, s.Health(ctx), errNotStarted)
	hub.propsErr = errors.New("hub unreachable")
	err := s.Start(ctx, make(chan *envelope.Envelope))
	assert.ErrorContains(t, err, "giving up after 2 failures")
	assert.ErrorContains(t, s.Health(ctx), "hub unreachable")
	assert.ErrorContains(t, s.Start(ctx, nil), "already started")
	require.NoError(t, s.Close(ctx))
	assert.ErrorIs(t, s.Health(ctx), errStopped)

	hub = newFakeHub("0")
	s = newTestEHSource(t, config.Source{}, hub, store.NewMemory("m"))
	_, stop := run(t, s, 10)
	require.Eventually(t, func() bool { return len(hub.openings()) == 1 }, waitFor, tick)
	require.NoError(t, s.Health(ctx))
	hub.mu.Lock()
	hub.propsErr = errors.New("gone")
	hub.mu.Unlock()
	assert.ErrorContains(t, s.Health(ctx), "gone")
	require.NoError(t, stop())

	// A store that is down: the partition is not read (where would it
	// start?), and Health says why.
	hub = newFakeHub("0")
	down := store.NewMemory("m")
	down.SetError(errors.New("store down"))
	s = newTestEHSource(t, config.Source{}, hub, down)
	_, stop = run(t, s, 10)
	require.Eventually(t, func() bool {
		err := s.Health(ctx)
		return err != nil && strings.Contains(err.Error(), "store down")
	}, waitFor, tick)
	assert.Empty(t, hub.openings())
	require.NoError(t, stop())
}
