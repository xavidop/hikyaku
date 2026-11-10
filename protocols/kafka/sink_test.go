package kafka

import (
	"context"
	"errors"
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

func newTestSink(t *testing.T, c *kfake.Cluster, ep config.Endpoint) *Sink {
	t.Helper()
	if ep.Brokers == nil {
		ep.Brokers = c.ListenAddrs()
	}
	s, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: Kind, Config: &ep, Env: plugin.Env{Relay: "test", Logger: testLogger()}})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s
}

func message(body string) *envelope.Envelope {
	env := envelope.New("in", "http", []byte(body))
	env.ContentType = "application/json"
	return env
}

func deliverCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	t.Cleanup(cancel)
	return ctx
}

func headersOf(r *kgo.Record) map[string][]string {
	out := map[string][]string{}
	for _, h := range r.Headers {
		out[h.Key] = append(out[h.Key], string(h.Value))
	}
	return out
}

func TestSinkProducesARecordWithKeyHeadersAndIdempotencyKey(t *testing.T) {
	c := cluster(t, 3, "billing.events")
	s := newTestSink(t, c, config.Endpoint{Topic: "billing.events", Key: "{{ .json.customer }}",
		Headers: map[string]string{"X-Relay": "hikyaku", "X-Kind": "{{ .json.type }}"}})
	assert.Equal(t, "out", s.Name())
	env := message(`{"type":"invoice.paid","customer":"c-9"}`)
	env.Headers = map[string][]string{
		"Authorization":   {"Bearer secret"},
		"X-Request-Id":    {"r-1"},
		"X-Tag":           {"a", "b"},
		"Content-Type":    {"text/plain"},
		"Idempotency-Key": {"stale"},
		"X-Kind":          {"overridden"},
	}

	res, err := s.Deliver(deliverCtx(t), env)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Status)
	require.Len(t, res.Headers[resultPartition], 1)
	assert.Equal(t, []string{"0"}, res.Headers[resultOffset])
	assert.Equal(t, "kafka://"+c.ListenAddrs()[0]+"/billing.events", res.Endpoint)

	got := consumed(t, c, "billing.events", 1)[0]
	assert.Equal(t, res.Headers[resultPartition][0], itoa(got.Partition))
	assert.Equal(t, "c-9", string(got.Key))
	assert.Equal(t, `{"type":"invoice.paid","customer":"c-9"}`, string(got.Value))
	assert.Equal(t, map[string][]string{
		"X-Request-Id":    {"r-1"},
		"X-Tag":           {"a", "b"},
		"X-Relay":         {"hikyaku"},
		"X-Kind":          {"invoice.paid"},
		"Idempotency-Key": {env.ID},
		"content-type":    {"application/json"},
	}, headersOf(got), "credentials stay behind; a configured header replaces the message's")

	// The same key lands on the same partition.
	res2, err := s.Deliver(deliverCtx(t), message(`{"customer":"c-9"}`))
	require.NoError(t, err)
	assert.Equal(t, res.Headers[resultPartition], res2.Headers[resultPartition])
}

func itoa(p int32) string { return string(rune('0' + p)) }

func TestSinkUsesTheSourcesIdempotencyKey(t *testing.T) {
	c := cluster(t, 1, "t")
	s := newTestSink(t, c, config.Endpoint{Topic: "t"})
	env := message(`{}`)
	env.Attributes["idempotency_key"] = "evt_1"
	_, err := s.Deliver(deliverCtx(t), env)
	require.NoError(t, err)
	assert.Equal(t, []string{"evt_1"}, headersOf(consumed(t, c, "t", 1)[0])["Idempotency-Key"])
}

func TestSinkTemplatedTopicAndBody(t *testing.T) {
	c := cluster(t, 1, "orders-eu", "orders-us")
	s := newTestSink(t, c, config.Endpoint{Topic: "orders-{{ .json.region }}", Body: `{"id":{{ .json.id }}}`})
	_, err := s.Deliver(deliverCtx(t), message(`{"region":"us","id":7,"pii":"x"}`))
	require.NoError(t, err)
	got := consumed(t, c, "orders-us", 1)[0]
	assert.Equal(t, `{"id":7}`, string(got.Value))
	assert.Nil(t, got.Key, "no key template: no key")
}

func TestSinkTemplateFailuresArePermanent(t *testing.T) {
	c := cluster(t, 1, "t")
	cases := map[string]config.Endpoint{
		"topic renders empty":   {Topic: "{{ .json.missing }}"},
		"topic renders invalid": {Topic: "{{ .json.region }} events"},
		"key fails":             {Topic: "t", Key: "{{ .json.region.x }}"},
		"header fails":          {Topic: "t", Headers: map[string]string{"X": "{{ .json.region.x }}"}},
		"body fails":            {Topic: "t", Body: "{{ .json.region.x }}"},
	}
	for name, ep := range cases {
		s := newTestSink(t, c, ep)
		_, err := s.Deliver(deliverCtx(t), message(`{"region":"eu"}`))
		require.Error(t, err, name)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, name)
		assert.Equal(t, []string{tagTemplate}, plugin.Tags(err), name)
	}
}

func TestSinkDeliverBatch(t *testing.T) {
	c := cluster(t, 2, "t")
	s := newTestSink(t, c, config.Endpoint{Topic: "t", Key: "{{ .json.k }}"})
	envs := []*envelope.Envelope{message(`{"k":"a","n":1}`), message(`{"k":"b","n":2}`), message(`{"k":"a","n":3}`)}
	res, err := s.DeliverBatch(deliverCtx(t), envs)
	require.NoError(t, err)
	assert.Len(t, res.Headers[resultPartition], 3, "one partition per message, in order")
	assert.Len(t, res.Headers[resultOffset], 3)
	assert.Equal(t, res.Headers[resultPartition][0], res.Headers[resultPartition][2], "same key, same partition")

	got := consumed(t, c, "t", 3)
	ids := map[string]bool{}
	for _, r := range got {
		ids[headersOf(r)["Idempotency-Key"][0]] = true
	}
	assert.Equal(t, map[string]bool{envs[0].ID: true, envs[1].ID: true, envs[2].ID: true}, ids)

	res, err = s.DeliverBatch(deliverCtx(t), nil)
	require.NoError(t, err)
	assert.Empty(t, res.Headers[resultPartition])
}

func TestSinkBatchFailsWhole(t *testing.T) {
	c := cluster(t, 1, "t")
	cl := client(t, c)
	s := newTestSink(t, c, config.Endpoint{Topic: "t", Key: "{{ .json.k.x }}"})
	_, err := s.DeliverBatch(deliverCtx(t), []*envelope.Envelope{message(`{"k":{"x":"1"}}`), message(`{"k":"flat"}`)})
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
	ends, err := kadmEnd(t, cl, "t")
	require.NoError(t, err)
	assert.Equal(t, int64(0), ends, "a batch with a bad message writes nothing")

	// A broker error on any record fails the batch.
	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: "t", Err: kerr.InvalidRecord})
	s2 := newTestSink(t, c, config.Endpoint{Topic: "t"})
	_, err = s2.DeliverBatch(deliverCtx(t), []*envelope.Envelope{message(`1`), message(`2`)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "message 1 of 2")
	assert.Equal(t, []string{"kafka:INVALID_RECORD"}, plugin.Tags(err))
}

func TestSinkBrokerErrorsAreClassified(t *testing.T) {
	cases := []struct {
		err   *kerr.Error
		class plugin.Class
		tags  []string
	}{
		{kerr.InvalidRecord, plugin.Permanent, []string{"kafka:INVALID_RECORD"}},
		{kerr.TopicAuthorizationFailed, plugin.Permanent, []string{"kafka:TOPIC_AUTHORIZATION_FAILED"}},
		{kerr.MessageTooLarge, plugin.Permanent, []string{"kafka:MESSAGE_TOO_LARGE"}},
		{kerr.PolicyViolation, plugin.Permanent, []string{"kafka:POLICY_VIOLATION"}},
		{kerr.CorruptMessage, plugin.Transient, []string{"kafka:CORRUPT_MESSAGE"}},
	}
	for _, tc := range cases {
		t.Run(tc.err.Message, func(t *testing.T) {
			c := cluster(t, 1, "t")
			c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: "t", Err: tc.err, Count: -1})
			s := newTestSink(t, c, config.Endpoint{Topic: "t"})
			_, err := s.Deliver(deliverCtx(t), message(`{}`))
			require.Error(t, err)
			var ke *kerr.Error
			require.True(t, errors.As(err, &ke), "the Kafka error is kept: %v", err)
			class, _ := plugin.Classify(err)
			assert.Equal(t, tc.class, class)
			assert.Equal(t, tc.tags, plugin.Tags(err))
		})
	}
}

func TestSinkUnknownTopicIsPermanent(t *testing.T) {
	c := cluster(t, 1, "t")
	s := newTestSink(t, c, config.Endpoint{Topic: "missing"})
	start := time.Now()
	_, err := s.Deliver(deliverCtx(t), message(`{}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "%v", err)
	assert.Equal(t, []string{"kafka:UNKNOWN_TOPIC_OR_PARTITION"}, plugin.Tags(err))
	t.Logf("a missing topic failed after %s", time.Since(start))
}

func TestSinkRetriableBrokerErrorsRunIntoTheAttemptTimeout(t *testing.T) {
	c := cluster(t, 1, "t")
	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: "t", Err: kerr.NotEnoughReplicas, Count: -1})
	s := newTestSink(t, c, config.Endpoint{Topic: "t"})
	s.flushTimeout = 10 * time.Millisecond // the record never goes out
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := s.Deliver(ctx, message(`{}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class, "the producer retries inside the attempt; the relay sees its timeout")
	assert.Contains(t, plugin.Tags(err), plugin.TagTimeout)
}

// A delivery that times out must not fail the other deliveries producing to
// the same partition: franz-go fails every record buffered for a partition
// when the first record of its next batch is cancelled.
func TestSinkTimeoutDoesNotFailOtherDeliveries(t *testing.T) {
	c := cluster(t, 1, "t")
	// acks: leader keeps one request in flight, so the next batch waits.
	s := newTestSink(t, c, config.Endpoint{Topic: "t", Acks: "leader"})
	_, err := s.Deliver(deliverCtx(t), message(`"warm-up"`))
	require.NoError(t, err)

	held, release := make(chan struct{}), make(chan struct{})
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.DropControl() // hold this one request only
		close(held)
		c.SleepControl(func() { <-release })
		return nil, nil, false
	})
	deliver := func(ctx context.Context, body string) <-chan error {
		ch := make(chan error, 1)
		go func() {
			_, err := s.Deliver(ctx, message(body))
			ch <- err
		}()
		return ch
	}
	first := deliver(deliverCtx(t), `"first"`)
	<-held // its request is in flight; the next records wait in one batch
	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	slow := deliver(short, `"slow"`)
	time.Sleep(20 * time.Millisecond)
	other := deliver(deliverCtx(t), `"other"`)

	err = <-slow
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class, "%v", err)
	close(release)
	assert.NoError(t, <-first)
	assert.NoError(t, <-other, "the other delivery is unaffected by the timed-out one in front of it")
}

func TestSinkAcksNoneHasNoOffsets(t *testing.T) {
	c := cluster(t, 1, "t")
	s := newTestSink(t, c, config.Endpoint{Topic: "t", Acks: "none"})
	res, err := s.Deliver(deliverCtx(t), message(`{}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"0"}, res.Headers[resultPartition])
	assert.NotContains(t, res.Headers, resultOffset)
	assert.Len(t, consumed(t, c, "t", 1), 1)
}

func TestSinkAcksAndCompression(t *testing.T) {
	c := cluster(t, 1, "t")
	for _, ep := range []config.Endpoint{
		{Topic: "t", Acks: "leader", Compression: "gzip"},
		{Topic: "t", Acks: "all", Compression: "snappy"},
		{Topic: "t", Compression: "lz4"},
		{Topic: "t", Compression: "zstd"},
	} {
		s := newTestSink(t, c, ep)
		_, err := s.Deliver(deliverCtx(t), message(`{"compress":"me me me me me me me me me me me me me me"}`))
		require.NoError(t, err, "%+v", ep)
	}
	assert.Len(t, consumed(t, c, "t", 4), 4)
}

func TestSinkSASL(t *testing.T) {
	c := clusterWith(t, kfake.SeedTopics(1, "t"), kfake.EnableSASL(), kfake.Superuser("SCRAM-SHA-512", "relay", "s3cret"))
	s := newTestSink(t, c, config.Endpoint{Topic: "t", SASL: &config.SASL{Mechanism: "scram-sha-512", Username: "relay", Password: "s3cret"}})
	_, err := s.Deliver(deliverCtx(t), message(`{}`))
	require.NoError(t, err)

	refuseSASL(c)
	bad := newTestSink(t, c, config.Endpoint{Topic: "t", SASL: &config.SASL{Mechanism: "scram-sha-512", Username: "relay", Password: "nope"}})
	bad.flushTimeout = 10 * time.Millisecond // its record never goes out
	start := time.Now()
	_, err = bad.Deliver(deliverCtx(t), message(`{}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "%v", err)
	assert.Equal(t, []string{"kafka:SASL_AUTHENTICATION_FAILED"}, plugin.Tags(err))
	assert.Less(t, time.Since(start), 5*time.Second, "a refused connection fails the delivery at once")
	assert.ErrorIs(t, bad.Health(deliverCtx(t)), kerr.SaslAuthenticationFailed)
}

func TestSinkHealthAndClose(t *testing.T) {
	c := cluster(t, 1, "t")
	s := newTestSink(t, c, config.Endpoint{Topic: "t"})
	require.NoError(t, s.Health(deliverCtx(t)))
	_, err := s.Deliver(deliverCtx(t), message(`{}`))
	require.NoError(t, err)
	require.NoError(t, s.Close(deliverCtx(t)))
	require.NoError(t, s.Close(deliverCtx(t)), "Close is idempotent")
	assert.ErrorIs(t, s.Health(deliverCtx(t)), errClosed)
	_, err = s.Deliver(deliverCtx(t), message(`{}`))
	require.ErrorIs(t, err, errClosed)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)

	down := newTestSink(t, c, config.Endpoint{Topic: "t"})
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.Error(t, down.Health(ctx))
}

func TestSinkCancelledDelivery(t *testing.T) {
	c := cluster(t, 1, "t")
	s := newTestSink(t, c, config.Endpoint{Topic: "t"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Deliver(ctx, message(`{}`))
	require.ErrorIs(t, err, context.Canceled)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class, "a cancelled delivery is the relay shutting down")

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err = s.Deliver(ctx, message(`{}`))
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)
}

// kadmEnd is the end offset of partition 0 of a topic.
func kadmEnd(t *testing.T, cl *kgo.Client, topic string) (int64, error) {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = 0
	rp.Timestamp = -1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(deliverCtx(t), cl)
	if err != nil {
		return 0, err
	}
	p := resp.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
		return 0, err
	}
	return p.Offset, nil
}
