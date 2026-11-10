package aws

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// runQueue starts a queue source on a fake queue. stop cancels it and
// closes it, as the engine does.
type runningQueue struct {
	src    *queueSource
	fake   *fakeSQS
	out    chan *envelope.Envelope
	cancel context.CancelFunc
	done   chan error
	logs   *logBuffer
}

func runQueue(t *testing.T, kind string, fake *fakeSQS, src *config.Source, tune func(*queueSource)) *runningQueue {
	t.Helper()
	if src.QueueURL == "" {
		src.QueueURL = queueURL
	}
	spec := sourceSpec(kind, src)
	logs := &logBuffer{}
	spec.Env.Logger = logs.logger()
	_, opts, err := parseQueueSource(spec)
	require.NoError(t, err)
	s := buildQueueSource(spec, opts, fake)
	s.rnd = func() float64 { return 0 }
	s.deleteEvery = 10 * time.Millisecond
	if tune != nil {
		tune(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningQueue{src: s, fake: fake, out: make(chan *envelope.Envelope, 16), cancel: cancel, done: make(chan error, 1), logs: logs}
	go func() { r.done <- s.Start(ctx, r.out) }()
	t.Cleanup(func() { r.stop(t) })
	return r
}

func (r *runningQueue) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		require.NoError(t, err)
		r.done <- nil // stop is idempotent
	case <-time.After(5 * time.Second):
		require.FailNow(t, "source did not stop")
	}
	require.NoError(t, r.src.Close(context.Background()))
}

func ack(env *envelope.Envelope, status envelope.AckStatus) {
	env.Queued()
	env.Ack(context.Background(), envelope.Outcome{Status: status})
}

func TestQueueSourceEnvelope(t *testing.T) {
	fake := newFakeSQS()
	sent := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	fake.putRaw(&fakeMsg{
		id: "m-1", body: `{"order":1}`, sent: sent, group: "customer-7",
		attrs: map[string]types.MessageAttributeValue{
			"trace":           {DataType: aws.String("String"), StringValue: aws.String("abc")},
			"count":           {DataType: aws.String("Number"), StringValue: aws.String("3")},
			"blob":            {DataType: aws.String("Binary"), BinaryValue: []byte{1, 2}},
			"Idempotency-Key": {DataType: aws.String("String"), StringValue: aws.String("key-1")},
		},
	})
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)
	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, "key-1", env.ID, "an Idempotency-Key attribute is the ID")
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, kindSQS, env.Protocol)
	assert.Equal(t, "orders", env.Subject, "the queue name")
	assert.Equal(t, `{"order":1}`, string(env.Body))
	assert.Equal(t, contentJSON, env.ContentType, "detected")
	assert.Equal(t, map[string][]string{"trace": {"abc"}, "count": {"3"}, "Idempotency-Key": {"key-1"}}, env.Headers)
	assert.Equal(t, "m-1", env.Attributes["sqs.message_id"])
	assert.Equal(t, 1, env.Attributes["sqs.receive_count"])
	assert.Equal(t, "2026-11-09T10:00:00Z", env.Attributes["sqs.sent_timestamp"])
	assert.Equal(t, "customer-7", env.Attributes["sqs.group_id"])
	assert.Equal(t, "customer-7", env.Attributes["ordering_key"], "a FIFO group is the ordering key")
	ack(env, envelope.Delivered)
}

func TestQueueSourceDecodesBase64AndReadsContentType(t *testing.T) {
	fake := newFakeSQS()
	fake.put(base64.StdEncoding.EncodeToString([]byte{0xff, 0x00, 0x01}), map[string]string{
		"content-transfer-encoding": "base64", "content-type": "application/x-thing",
	})
	fake.put("plain text", nil)
	fake.put("%%%", map[string]string{"content-transfer-encoding": "base64"})
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)

	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, []byte{0xff, 0x00, 0x01}, env.Body)
	assert.Equal(t, "application/x-thing", env.ContentType)
	assert.Empty(t, env.Headers, "content-type and content-transfer-encoding are consumed")
	assert.Equal(t, "msg-1", env.ID, "else the SQS message ID")
	assert.NotContains(t, env.Attributes, "ordering_key", "standard queues have no order")

	env = recv(t, r.out, 2*time.Second)
	assert.Equal(t, contentText, env.ContentType)

	env = recv(t, r.out, 2*time.Second)
	assert.Equal(t, "%%%", string(env.Body), "a body that is not base64 passes as it is")
	assert.Contains(t, r.logs.String(), "is not")
}

func TestQueueSourceDeletesFinalMessagesInBatches(t *testing.T) {
	fake := newFakeSQS()
	for range 12 {
		fake.put("m", nil)
	}
	r := runQueue(t, kindSQS, fake, &config.Source{}, func(s *queueSource) { s.deleteEvery = time.Hour })
	envs := make([]*envelope.Envelope, 12)
	for i := range envs {
		envs[i] = recv(t, r.out, 2*time.Second)
	}
	// Every final outcome deletes; ten settled deletes are one call at once.
	statuses := []envelope.AckStatus{envelope.Delivered, envelope.DeadLettered, envelope.Unrouted, envelope.Filtered}
	for i, env := range envs[:10] {
		ack(env, statuses[i%len(statuses)])
	}
	require.Eventually(t, func() bool { return fake.count("DeleteMessageBatch") == 1 }, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 2, fake.remaining())
	// The last two wait for the timer, or for Close.
	ack(envs[10], envelope.Delivered)
	ack(envs[11], envelope.Delivered)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 2, fake.remaining())
	r.stop(t)
	assert.Equal(t, 0, fake.remaining(), "Close flushes the pending deletes")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Len(t, fake.deleteCalls, 2)
	assert.Len(t, fake.deleteCalls[0], 10)
	assert.Len(t, fake.deleteCalls[1], 2)
}

func TestQueueSourceRedeliversFailedMessagesAfterABackoff(t *testing.T) {
	fake := newFakeSQS()
	fake.put("retry me", nil)
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)

	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, 1, env.Attributes["sqs.receive_count"])
	ack(env, envelope.Failed)
	calls := fake.visibilityCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, int32(1), calls[0].secs, "1s backoff on the first receive")
	assert.False(t, calls[0].batch)

	env = recv(t, r.out, 3*time.Second)
	assert.Equal(t, "retry me", string(env.Body))
	assert.Equal(t, 2, env.Attributes["sqs.receive_count"], "SQS redelivered it")
	assert.Equal(t, "msg-1", env.ID, "with the same ID")
	ack(env, envelope.Delivered)
	require.Eventually(t, func() bool { return fake.remaining() == 0 }, 2*time.Second, 5*time.Millisecond)
}

func TestQueueSourceBackoffIsCappedByTheVisibilityTimeout(t *testing.T) {
	fake := newFakeSQS()
	fake.putRaw(&fakeMsg{body: "x", receives: 9})
	r := runQueue(t, kindSQS, fake, &config.Source{VisibilityTimeout: config.Duration(3 * time.Second)},
		func(s *queueSource) { s.rnd = func() float64 { return 0.999 } })
	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, 10, env.Attributes["sqs.receive_count"])
	ack(env, envelope.Failed)
	calls := fake.visibilityCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, int32(3), calls[0].secs, "512s of backoff, capped at the 3s visibility timeout")
}

func TestQueueSourceExtendsMessagesInFlight(t *testing.T) {
	fake := newFakeSQS()
	fake.put("slow", nil)
	r := runQueue(t, kindSQS, fake, &config.Source{VisibilityTimeout: config.Duration(time.Second)}, nil)
	env := recv(t, r.out, 2*time.Second)

	// Held for 2.5 visibility timeouts: without the heartbeat SQS would hand
	// the message out again.
	noMessage(t, r.out, 2500*time.Millisecond)
	calls := fake.visibilityCalls()
	require.NotEmpty(t, calls, "the heartbeat extended the message")
	for _, c := range calls {
		assert.True(t, c.batch)
		assert.Equal(t, int32(1), c.secs)
	}
	fake.mu.Lock()
	receives := fake.msgs[0].receives
	fake.mu.Unlock()
	assert.Equal(t, 1, receives)

	ack(env, envelope.Delivered)
	require.Eventually(t, func() bool { return fake.remaining() == 0 }, 2*time.Second, 5*time.Millisecond)
	time.Sleep(300 * time.Millisecond) // a heartbeat already on its way lands
	n := len(fake.visibilityCalls())
	time.Sleep(700 * time.Millisecond)
	assert.Len(t, fake.visibilityCalls(), n, "a settled message is no longer extended")
}

func TestQueueSourceReadsTheQueueVisibilityTimeout(t *testing.T) {
	fake := newFakeSQS()
	fake.visibility = 45 * time.Second
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)
	require.Eventually(t, func() bool { return r.src.visibility.Load() == 45 }, 2*time.Second, 5*time.Millisecond)

	fake = newFakeSQS()
	fake.attrsErr = apiError("AccessDenied", 403, smithy.FaultClient)
	r = runQueue(t, kindSQS, fake, &config.Source{}, nil)
	require.Eventually(t, func() bool { return r.src.visibility.Load() == 30 }, 2*time.Second, 5*time.Millisecond)
	assert.Contains(t, r.logs.String(), "assuming 30s")

	fake = newFakeSQS()
	r = runQueue(t, kindSQS, fake, &config.Source{VisibilityTimeout: config.Duration(7 * time.Second)}, nil)
	require.Eventually(t, func() bool { return r.src.visibility.Load() == 7 }, 2*time.Second, 5*time.Millisecond)
	assert.Zero(t, fake.count("GetQueueAttributes"), "a configured timeout needs no lookup")
}

func TestQueueSourceDropsOversizeMessages(t *testing.T) {
	fake := newFakeSQS()
	fake.put("this body is far too long", nil)
	fake.put("ok", nil)
	r := runQueue(t, kindSQS, fake, &config.Source{MaxBodyBytes: 10}, nil)
	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, "ok", string(env.Body))
	ack(env, envelope.Delivered)
	require.Eventually(t, func() bool { return fake.remaining() == 0 }, 2*time.Second, 5*time.Millisecond)
	assert.Contains(t, r.logs.String(), "over max_body_bytes")
}

func TestQueueSourceAckAccepted(t *testing.T) {
	fake := newFakeSQS()
	fake.put("x", nil)
	r := runQueue(t, kindSQS, fake, &config.Source{Ack: "accepted"}, nil)
	env := recv(t, r.out, 2*time.Second)
	env.Queued()
	require.Eventually(t, func() bool { return fake.remaining() == 0 }, 2*time.Second, 5*time.Millisecond)
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	assert.Empty(t, fake.visibilityCalls(), "settled once, when queued")
}

func TestQueueSourceReleasesUnhandedMessagesAtShutdown(t *testing.T) {
	fake := newFakeSQS()
	fake.put("a", nil)
	fake.put("b", nil)
	spec := sourceSpec(kindSQS, &config.Source{QueueURL: queueURL})
	_, opts, err := parseQueueSource(spec)
	require.NoError(t, err)
	s := buildQueueSource(spec, opts, fake)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	// Nobody reads: the source blocks handing the first message over.
	go func() { done <- s.Start(ctx, make(chan *envelope.Envelope)) }()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.inflight) == 2
	}, 2*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.NoError(t, s.Close(context.Background()))
	calls := fake.visibilityCalls()
	require.Len(t, calls, 2, "both messages go back to the queue at once")
	for _, c := range calls {
		assert.Zero(t, c.secs)
	}
	assert.Equal(t, 2, fake.remaining())
}

func TestQueueSourceRetriesReceivesAndGivesUp(t *testing.T) {
	fake := newFakeSQS()
	fake.receiveErrs = []error{apiError("InternalError", 500, smithy.FaultServer), nil}
	fake.put("after the error", nil)
	r := runQueue(t, kindSQS, fake, &config.Source{Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond)}}, nil)
	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, "after the error", string(env.Body))
	assert.Contains(t, r.logs.String(), "receive failed; retrying")
	require.Eventually(t, func() bool { return r.src.Health(context.Background()) == nil }, time.Second, 5*time.Millisecond)

	denied := apiError("AccessDenied", 403, smithy.FaultClient)
	fake = newFakeSQS()
	fake.receiveErrs = []error{denied, denied, denied}
	spec := sourceSpec(kindSQS, &config.Source{QueueURL: queueURL, Reconnect: &config.Reconnect{
		Backoff: config.Duration(time.Millisecond), MaxAttempts: 3,
	}})
	_, opts, err := parseQueueSource(spec)
	require.NoError(t, err)
	s := buildQueueSource(spec, opts, fake)
	err = s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 3 failed receives")
	assert.Contains(t, err.Error(), "AccessDenied")
	assert.Error(t, s.Health(context.Background()))
	require.NoError(t, s.Close(context.Background()))
}

func TestQueueSourceRetriesFailedDeletes(t *testing.T) {
	fake := newFakeSQS()
	fake.put("x", nil)
	fake.deleteErr = apiError("InternalError", 500, smithy.FaultServer)
	r := runQueue(t, kindSQS, fake, &config.Source{}, func(s *queueSource) { s.deleteEvery = 200 * time.Millisecond })
	ack(recv(t, r.out, 2*time.Second), envelope.Delivered)
	require.Eventually(t, func() bool { return fake.count("DeleteMessageBatch") >= 1 }, 2*time.Second, 5*time.Millisecond)
	fake.mu.Lock()
	fake.deleteErr = nil
	fake.mu.Unlock()
	require.Eventually(t, func() bool { return fake.remaining() == 0 }, 2*time.Second, 5*time.Millisecond)
	assert.Contains(t, r.logs.String(), "cannot delete settled messages")
}

func TestQueueSourceHealthBeforeStart(t *testing.T) {
	fake := newFakeSQS()
	spec := sourceSpec(kindSQS, &config.Source{QueueURL: queueURL})
	_, opts, err := parseQueueSource(spec)
	require.NoError(t, err)
	s := buildQueueSource(spec, opts, fake)
	require.NoError(t, s.Health(context.Background()))
	assert.Equal(t, 1, fake.count("GetQueueAttributes"))

	fake.attrsErr = apiError("AWS.SimpleQueueService.NonExistentQueue", 400, smithy.FaultClient)
	err = s.Health(context.Background())
	class, tags := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, tags, "sqs:AWS.SimpleQueueService.NonExistentQueue")
	require.NoError(t, s.Close(context.Background()), "closing a source that never ran")
	assert.Equal(t, "in", s.Name())
}

func TestQueueSourceStartsOnce(t *testing.T) {
	fake := newFakeSQS()
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)
	require.Eventually(t, func() bool { return fake.count("ReceiveMessage") > 0 }, time.Second, 5*time.Millisecond)
	assert.ErrorContains(t, r.src.Start(context.Background(), r.out), "already started")
}

func TestQueueSourceDeletesAfterCloseAtOnce(t *testing.T) {
	fake := newFakeSQS()
	fake.put("late", nil)
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)
	env := recv(t, r.out, 2*time.Second)
	r.stop(t)
	// The engine abandoned it past its shutdown grace, then it finished.
	ack(env, envelope.Delivered)
	assert.Equal(t, 0, fake.remaining())
}

func TestQueueSourceConcurrentSettles(t *testing.T) {
	fake := newFakeSQS()
	for range 40 {
		fake.put("m", nil)
	}
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)
	var wg sync.WaitGroup
	for range 40 {
		env := recv(t, r.out, 2*time.Second)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ack(env, envelope.Delivered)
		}()
	}
	wg.Wait()
	require.Eventually(t, func() bool { return fake.remaining() == 0 }, 2*time.Second, 5*time.Millisecond)
}
