package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func TestRegister(t *testing.T) {
	Register()
	_, ok := plugin.LookupSource(Kind)
	assert.True(t, ok)
	_, ok = plugin.LookupSink(Kind)
	assert.True(t, ok)
}

func TestSourceCommitsOnlyOnceDelivered(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	produce(t, cl, record("orders", 0, "a"), record("orders", 0, "b"), record("orders", 0, "c"))

	r := startSource(t, c, config.Source{Topic: "orders"})
	var envs []*envelope.Envelope
	for i := range 3 {
		env := r.next(t)
		assert.Equal(t, int64(i), offsetOf(t, env), "records come in partition order")
		envs = append(envs, env)
	}
	time.Sleep(200 * time.Millisecond) // ten commit intervals
	assert.Equal(t, int64(-1), committed(t, cl, "g", "orders", 0), "nothing is committed while in flight")

	ack(envs[0], envelope.Delivered)
	eventuallyCommitted(t, cl, "g", "orders", 0, 1)
	ack(envs[1], envelope.DeadLettered)
	ack(envs[2], envelope.Filtered)
	eventuallyCommitted(t, cl, "g", "orders", 0, 3)
}

func TestSourceHoldsTheCommitAtAFailedRecordAndRedeliversAfterARestart(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	for i := range 5 {
		produce(t, cl, record("orders", 0, fmt.Sprint(i)))
	}

	r := startSource(t, c, config.Source{Topic: "orders"})
	statuses := []envelope.AckStatus{envelope.Delivered, envelope.Unrouted, envelope.Failed, envelope.Delivered, envelope.DeadLettered}
	for _, st := range statuses {
		ack(r.next(t), st)
	}
	eventuallyCommitted(t, cl, "g", "orders", 0, 2)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(2), committed(t, cl, "g", "orders", 0), "the failed record holds the commit")
	r.stop(t)
	assert.Equal(t, int64(2), committed(t, cl, "g", "orders", 0), "Close commits no further")

	again := startSource(t, c, config.Source{Topic: "orders"})
	for want := int64(2); want < 5; want++ {
		env := again.next(t)
		assert.Equal(t, want, offsetOf(t, env), "the failed record and every one after it come again")
		assert.Equal(t, fmt.Sprintf("orders-0-%d", want), env.ID, "the ID is stable across redeliveries")
		ack(env, envelope.Delivered)
	}
	eventuallyCommitted(t, cl, "g", "orders", 0, 5)
	again.none(t, 100*time.Millisecond)
}

func TestSourceOutOfOrderSettlesCommitTheContiguousPrefix(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	for i := range 4 {
		produce(t, cl, record("orders", 0, fmt.Sprint(i)))
	}
	r := startSource(t, c, config.Source{Topic: "orders"})
	envs := make([]*envelope.Envelope, 4)
	for i := range envs {
		envs[i] = r.next(t)
	}
	ack(envs[3], envelope.Delivered)
	ack(envs[1], envelope.Delivered)
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, int64(-1), committed(t, cl, "g", "orders", 0), "offset 0 is still in flight")
	ack(envs[0], envelope.Delivered)
	eventuallyCommitted(t, cl, "g", "orders", 0, 2)
	ack(envs[2], envelope.Delivered)
	eventuallyCommitted(t, cl, "g", "orders", 0, 4)
}

func TestSourceRestartResumesFromTheCommittedOffset(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	produce(t, cl, record("orders", 0, "a"), record("orders", 0, "b"))
	r := startSource(t, c, config.Source{Topic: "orders"})
	ack(r.next(t), envelope.Delivered)
	ack(r.next(t), envelope.Delivered)
	eventuallyCommitted(t, cl, "g", "orders", 0, 2)
	r.stop(t)

	produce(t, cl, record("orders", 0, "c"), record("orders", 0, "d"))
	again := startSource(t, c, config.Source{Topic: "orders", Start: "earliest"})
	first := again.next(t)
	assert.Equal(t, int64(2), offsetOf(t, first), "start applies only to a group without a commit")
	assert.Equal(t, "c", string(first.Body))
	ack(first, envelope.Delivered)
	ack(again.next(t), envelope.Delivered)
	eventuallyCommitted(t, cl, "g", "orders", 0, 4)
}

func TestSourceCloseCommitsTheFinalPrefix(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	produce(t, cl, record("orders", 0, "a"), record("orders", 0, "b"))
	src := newTestSource(t, c, config.Source{Topic: "orders"})
	src.commitEvery = time.Hour // only Close commits
	r := run(t, src)
	ack(r.next(t), envelope.Delivered)
	second := r.next(t)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(-1), committed(t, cl, "g", "orders", 0))
	r.stop(t)
	assert.Equal(t, int64(1), committed(t, cl, "g", "orders", 0), "Close committed the final prefix")
	ack(second, envelope.Delivered) // too late: ignored
	assert.Equal(t, int64(1), committed(t, cl, "g", "orders", 0))
	assert.ErrorIs(t, src.Health(context.Background()), errClosed)
}

func TestSourceStartsAtTheEndByDefault(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	produce(t, cl, record("orders", 0, "old"))
	r := startSource(t, c, config.Source{Topic: "orders", Start: "latest"})
	var env *envelope.Envelope
	require.Eventually(t, func() bool {
		produce(t, cl, record("orders", 0, "new"))
		select {
		case env = <-r.out:
			return true
		case <-time.After(100 * time.Millisecond):
			return false
		}
	}, wait, time.Millisecond)
	assert.Equal(t, "new", string(env.Body), "a new group skips what was there")
}

func TestSourceEnvelope(t *testing.T) {
	c := cluster(t, 2, "orders")
	cl := client(t, c)
	ts := time.Date(2026, 11, 9, 10, 30, 0, 123456789, time.UTC)
	produce(t, cl, &kgo.Record{
		Topic: "orders", Partition: 1, Key: []byte("cust-7"), Value: []byte(`{"id":1}`), Timestamp: ts,
		Headers: []kgo.RecordHeader{
			{Key: "tag", Value: []byte("a")}, {Key: "tag", Value: []byte("b")},
			{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
		},
	})
	produce(t, cl,
		&kgo.Record{Topic: "orders", Partition: 1, Value: []byte("<x/>"), Headers: []kgo.RecordHeader{
			{Key: "Content-Type", Value: []byte("application/xml")}, {Key: "Idempotency-Key", Value: []byte("evt_42")},
		}},
		&kgo.Record{Topic: "orders", Partition: 1, Value: []byte("plain text")},
		&kgo.Record{Topic: "orders", Partition: 1, Value: []byte{0xff, 0x00}},
	)

	r := startSource(t, c, config.Source{Topic: "orders"})
	env := r.next(t)
	assert.Equal(t, "orders-1-0", env.ID)
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, Kind, env.Protocol)
	assert.Equal(t, "orders", env.Subject)
	assert.Equal(t, `{"id":1}`, string(env.Body))
	assert.Equal(t, "application/json", env.ContentType, "detected")
	assert.Equal(t, []string{"a", "b"}, env.Headers["tag"], "a repeated header keeps every value")
	assert.Equal(t, map[string]any{
		"kafka.topic":     "orders",
		"kafka.partition": int64(1),
		"kafka.offset":    int64(0),
		"kafka.key":       "cust-7",
		"kafka.timestamp": "2026-11-09T10:30:00.123Z",
		"ordering_key":    "orders/1",
	}, env.AttributesCopy())
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", env.Trace.TraceID().String(), "traceparent continues the trace")

	keyed := r.next(t)
	assert.Equal(t, "evt_42", keyed.ID, "the Idempotency-Key header is the ID")
	assert.Equal(t, "evt_42", must(keyed.Attribute("idempotency_key")))
	assert.Equal(t, "application/xml", keyed.ContentType, "from the content-type header")
	assert.Equal(t, "", must(keyed.Attribute("kafka.key")), "a record without a key")

	assert.Equal(t, "text/plain; charset=utf-8", r.next(t).ContentType)
	assert.Equal(t, "application/octet-stream", r.next(t).ContentType)
}

func must(v any, _ bool) any { return v }

func TestSourceSkipsRecordsOverMaxBodyBytes(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	produce(t, cl, record("orders", 0, strings.Repeat("x", 64)), record("orders", 0, "small"))
	r := startSource(t, c, config.Source{Topic: "orders", MaxBodyBytes: 16})
	env := r.next(t)
	assert.Equal(t, "small", string(env.Body), "the oversize record is skipped")
	assert.Equal(t, int64(1), offsetOf(t, env))
	ack(env, envelope.Delivered)
	eventuallyCommitted(t, cl, "g", "orders", 0, 2)
}

func TestSourceAckAcceptedCommitsOnceQueued(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	produce(t, cl, record("orders", 0, "a"))
	r := startSource(t, c, config.Source{Topic: "orders", Ack: "accepted"})
	env := r.next(t)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(-1), committed(t, cl, "g", "orders", 0), "not yet taken by the relay")
	env.Queued()
	eventuallyCommitted(t, cl, "g", "orders", 0, 1)
	ack(env, envelope.Failed) // the outcome no longer matters
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(1), committed(t, cl, "g", "orders", 0))
}

func TestSourcePrefetchBoundsRecordsInFlight(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	for i := range 5 {
		produce(t, cl, record("orders", 0, fmt.Sprint(i)))
	}
	r := startSource(t, c, config.Source{Topic: "orders", Prefetch: 2})
	a, b := r.next(t), r.next(t)
	r.none(t, 300*time.Millisecond)
	ack(a, envelope.Delivered)
	third := r.next(t)
	assert.Equal(t, int64(2), offsetOf(t, third))
	r.none(t, 200*time.Millisecond)
	ack(b, envelope.Failed) // a failed record frees its slot too
	assert.Equal(t, int64(3), offsetOf(t, r.next(t)))
}

func TestSourceBlocksOnBackpressure(t *testing.T) {
	c := cluster(t, 1, "orders")
	cl := client(t, c)
	for i := range 3 {
		produce(t, cl, record("orders", 0, fmt.Sprint(i)))
	}
	src := newTestSource(t, c, config.Source{Topic: "orders"})
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *envelope.Envelope) // a relay that takes nothing
	done := make(chan error, 1)
	go func() { done <- src.Start(ctx, out) }()
	env := <-out
	assert.Equal(t, int64(0), offsetOf(t, env))
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err, "Start returns nil once its context ends, even while blocked on the relay")
	case <-time.After(wait):
		t.Fatal("Start is stuck")
	}
	ack(env, envelope.Delivered)
	ctx2, cancel2 := context.WithTimeout(context.Background(), wait)
	defer cancel2()
	require.NoError(t, src.Close(ctx2))
	assert.Equal(t, int64(1), committed(t, cl, "g", "orders", 0), "the records never handed over are redelivered")
}

// Two members of a group split the partitions. The member that gives
// partitions up commits what it finished first, so the new owner starts
// after it.
func TestSourceRebalanceSplitsPartitionsAndCommitsOnRevoke(t *testing.T) {
	c := cluster(t, 4, "orders")
	cl := client(t, c)
	for p := range int32(4) {
		produce(t, cl, record("orders", p, "first"))
	}
	srcA := newTestSource(t, c, config.Source{Topic: "orders"})
	srcA.commitEvery = time.Hour // only the revoke commits
	a := run(t, srcA)
	for range 4 {
		ack(a.next(t), envelope.Delivered)
	}
	b := startSource(t, c, config.Source{Topic: "orders"})

	// Produce a round of one record per partition until a round shows the
	// split: each member got two partitions, and no partition went to both.
	drain := func(name string, r *sourceRun, into map[int32]int) {
		for {
			select {
			case env := <-r.out:
				assert.NotEqual(t, "first", string(env.Body), "%s: a revoked partition resumes after what its owner finished", name)
				into[partitionOf(t, env)]++
				ack(env, envelope.Delivered)
			case <-time.After(150 * time.Millisecond):
				return
			}
		}
	}
	split := func() bool {
		for p := range int32(4) {
			produce(t, cl, record("orders", p, "more"))
		}
		ga, gb := map[int32]int{}, map[int32]int{}
		drain("a", a, ga)
		drain("b", b, gb)
		if len(ga) != 2 || len(gb) != 2 {
			return false
		}
		for p := range gb {
			if ga[p] > 0 {
				return false
			}
		}
		return true
	}
	require.Eventually(t, split, wait, time.Millisecond, "the two members split the four partitions")
}

func TestSourceHealth(t *testing.T) {
	c := cluster(t, 1, "orders")
	src := newTestSource(t, c, config.Source{Topic: "orders"})
	src.leaveTimeout = 200 * time.Millisecond // the cluster is gone at Close
	assert.ErrorIs(t, src.Health(context.Background()), errNotStarted)
	r := run(t, src)
	require.Eventually(t, func() bool { return src.Health(context.Background()) == nil }, wait, 10*time.Millisecond)
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.Error(t, src.Health(ctx), "no broker answers")
	r.cancel()
}

func TestSourceGivesUpOnBadCredentials(t *testing.T) {
	c := clusterWith(t, kfake.SeedTopics(1, "orders"), kfake.EnableSASL(), kfake.Superuser("SCRAM-SHA-256", "relay", "s3cret"))
	refuseSASL(c)
	src := newTestSource(t, c, config.Source{Topic: "orders",
		SASL:      &config.SASL{Mechanism: "scram-sha-256", Username: "relay", Password: "wrong"},
		Reconnect: &config.Reconnect{Backoff: config.Duration(10 * time.Millisecond), MaxAttempts: 2}})
	r := run(t, src)
	select {
	case err := <-r.done:
		r.done <- nil
		require.Error(t, err)
		assert.ErrorIs(t, err, kerr.SaslAuthenticationFailed)
		assert.Contains(t, err.Error(), "giving up after 2 refused connections in a row")
	case <-time.After(wait):
		t.Fatal("Start keeps retrying bad credentials")
	}
}

func TestSourceKeepsRetryingBadCredentialsByDefault(t *testing.T) {
	c := clusterWith(t, kfake.SeedTopics(1, "orders"), kfake.EnableSASL(), kfake.Superuser("SCRAM-SHA-256", "relay", "s3cret"))
	refuseSASL(c)
	src := newTestSource(t, c, config.Source{Topic: "orders",
		SASL: &config.SASL{Mechanism: "scram-sha-256", Username: "relay", Password: "wrong"}})
	r := run(t, src)
	select {
	case err := <-r.done:
		r.done <- err
		t.Fatalf("Start gave up: %v", err)
	case <-time.After(3 * time.Second):
	}
	assert.Error(t, src.Health(context.Background()), "health shows the refusal")
}

// The group's ACLs refuse the member: Start gives up after max_attempts
// polls that say so.
func TestSourceGivesUpWhenTheGroupIsNotAuthorized(t *testing.T) {
	c := cluster(t, 1, "orders")
	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.FindCoordinator, kmsg.JoinGroup}, Group: "g", Err: kerr.GroupAuthorizationFailed, Count: -1})
	src := newTestSource(t, c, config.Source{Topic: "orders",
		Reconnect: &config.Reconnect{Backoff: config.Duration(10 * time.Millisecond), MaxAttempts: 2}})
	r := run(t, src)
	select {
	case err := <-r.done:
		r.done <- nil
		require.Error(t, err)
		assert.ErrorIs(t, err, kerr.GroupAuthorizationFailed)
	case <-time.After(wait):
		t.Fatal("Start keeps retrying a refused group")
	}
}

func TestSourceSASL(t *testing.T) {
	for _, mech := range []struct{ kfake, spec string }{
		{"PLAIN", "plain"}, {"SCRAM-SHA-256", "scram-sha-256"}, {"SCRAM-SHA-512", "scram-sha-512"},
	} {
		t.Run(mech.spec, func(t *testing.T) {
			c := clusterWith(t, kfake.SeedTopics(1, "orders"), kfake.EnableSASL(), kfake.Superuser(mech.kfake, "relay", "s3cret"))
			cl := client(t, c, kgo.SASL(saslMech(t, mech.spec, "relay", "s3cret")))
			produce(t, cl, record("orders", 0, "a"))
			r := startSource(t, c, config.Source{Topic: "orders", SASL: &config.SASL{Mechanism: mech.spec, Username: "relay", Password: "s3cret"}})
			env := r.next(t)
			assert.Equal(t, "a", string(env.Body))
			ack(env, envelope.Delivered)
			eventuallyCommitted(t, cl, "g", "orders", 0, 1)
		})
	}
}

func TestSourceConsumesSeveralTopics(t *testing.T) {
	c := cluster(t, 1, "a", "b")
	cl := client(t, c)
	produce(t, cl, record("a", 0, "from a"), record("b", 0, "from b"))
	r := startSource(t, c, config.Source{Topics: []string{"b", "a", "b"}})
	subjects := map[string]bool{}
	for range 2 {
		env := r.next(t)
		subjects[env.Subject] = true
		ack(env, envelope.Delivered)
	}
	assert.Equal(t, map[string]bool{"a": true, "b": true}, subjects)
	eventuallyCommitted(t, cl, "g", "a", 0, 1)
	eventuallyCommitted(t, cl, "g", "b", 0, 1)
}

// Settles that come after their partition was revoked are ignored, and the
// records of a revoked partition that were not handed over yet are dropped.
func TestSourceIgnoresSettlesOfRevokedPartitions(t *testing.T) {
	src := newTestSource(t, cluster(t, 1, "orders"), config.Source{Topic: "orders"})
	k := tp{"orders", 0}
	ps := newPartState(k)
	src.parts[k] = ps
	ps.add(0, 0)
	ps.add(1, 0)
	src.inflight = 2
	src.mu.Lock()
	ps.dead = true
	delete(src.parts, k)
	src.mu.Unlock()
	src.settle(ps, 0, true)
	src.settle(ps, 1, false)
	assert.Equal(t, 0, src.inflight, "the slots are freed")
	assert.Equal(t, int64(-1), ps.next, "the dead state did not move")
	assert.False(t, ps.held)
}

func TestSourceStartTwice(t *testing.T) {
	c := cluster(t, 1, "orders")
	r := startSource(t, c, config.Source{Topic: "orders"})
	require.Eventually(t, r.src.started.Load, wait, time.Millisecond)
	assert.ErrorContains(t, r.src.Start(context.Background(), make(chan *envelope.Envelope)), "already started")
}

func TestSourceCloseBeforeStart(t *testing.T) {
	c := cluster(t, 1, "orders")
	src := newTestSource(t, c, config.Source{Topic: "orders"})
	require.NoError(t, src.Close(context.Background()))
	require.NoError(t, src.Close(context.Background()), "Close is idempotent")
	assert.ErrorIs(t, src.Start(context.Background(), make(chan *envelope.Envelope)), errClosed)
}
