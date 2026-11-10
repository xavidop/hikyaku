package aws

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

func TestQueueSourceSettleFailures(t *testing.T) {
	fake := newFakeSQS()
	fake.put("a", nil)
	fake.visErr = apiError("InternalError", 500, smithy.FaultServer)
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)
	ack(recv(t, r.out, 2*time.Second), envelope.Failed)
	assert.Contains(t, r.logs.String(), "SQS redelivers it once its visibility timeout ends")
}

func TestQueueSourceDeleteEntryFailures(t *testing.T) {
	// An entry that failed for another reason is tried three times, then
	// left to SQS: the message comes back, and its ID dedupes it.
	fake := newFakeSQS()
	fake.put("a", nil)
	fake.deleteFail = "InternalError"
	r := runQueue(t, kindSQS, fake, &config.Source{}, nil)
	ack(recv(t, r.out, 2*time.Second), envelope.Delivered)
	require.Eventually(t, func() bool { return fake.count("DeleteMessageBatch") == maxSettleTries }, 2*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, maxSettleTries, fake.count("DeleteMessageBatch"), "then it gives up")
	assert.Contains(t, r.logs.String(), "cannot delete a settled message")

	// A receipt handle that expired, or a message already gone, is done.
	fake = newFakeSQS()
	fake.put("b", nil)
	fake.deleteFail = "ReceiptHandleIsInvalid"
	r = runQueue(t, kindSQS, fake, &config.Source{}, nil)
	ack(recv(t, r.out, 2*time.Second), envelope.Delivered)
	require.Eventually(t, func() bool { return fake.count("DeleteMessageBatch") == 1 }, 2*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, fake.count("DeleteMessageBatch"))
}

func TestQueueSourceHeartbeatFailuresAreLogged(t *testing.T) {
	fake := newFakeSQS()
	fake.put("slow", nil)
	r := runQueue(t, kindSQS, fake, &config.Source{VisibilityTimeout: config.Duration(time.Second)}, nil)
	env := recv(t, r.out, 2*time.Second)
	fake.mu.Lock()
	fake.visErr = apiError("InternalError", 500, smithy.FaultServer)
	fake.mu.Unlock()
	require.Eventually(t, func() bool { return strings.Contains(r.logs.String(), "cannot change the visibility") },
		3*time.Second, 5*time.Millisecond)
	fake.mu.Lock()
	fake.visErr = nil
	fake.mu.Unlock()
	ack(env, envelope.Delivered)
}

func TestReceiveCountDefaultsToOne(t *testing.T) {
	assert.Equal(t, 1, receiveCount(types.Message{}))
	assert.Equal(t, 1, receiveCount(types.Message{Attributes: map[string]string{"ApproximateReceiveCount": "x"}}))
	assert.Equal(t, 4, receiveCount(types.Message{Attributes: map[string]string{"ApproximateReceiveCount": "4"}}))
}

func TestIdempotencyKey(t *testing.T) {
	env := newEnv("x")
	assert.Equal(t, env.ID, idempotencyKey(env))
	env.SetAttribute("idempotency_key", "from-attribute")
	assert.Equal(t, "from-attribute", idempotencyKey(env))
	env.SetHeader("idempotency-key", "from-header")
	assert.Equal(t, "from-header", idempotencyKey(env), "the key the engine set on this delivery")
}

func TestFIFOIDs(t *testing.T) {
	assert.Equal(t, "abc-123_!~", fifoID("abc-123_!~"))
	for _, bad := range []string{"", "with space", "ünïcode", strings.Repeat("x", 129)} {
		got := fifoID(bad)
		assert.Len(t, got, 64, bad)
		assert.Equal(t, got, fifoID(bad), "stable")
	}
	g, err := groupID(nil, envelope.New("", "http", nil), "relay")
	require.NoError(t, err)
	assert.Equal(t, "relay", g, "no source: the relay")
}

func TestChunk(t *testing.T) {
	size := func(n int) int { return n }
	assert.Equal(t, [][]int{{1, 1}, {1}}, chunk([]int{1, 1, 1}, size, 2, 100))
	assert.Equal(t, [][]int{{60}, {60}, {200}, {10, 10}}, chunk([]int{60, 60, 200, 10, 10}, size, 10, 100))
	assert.Nil(t, chunk([]int{}, size, 10, 100))
}

func TestS3MetadataLimits(t *testing.T) {
	logs := &logBuffer{}
	b := &metadataBuilder{dest: "out", log: logs.logger()}
	env := newEnv("x")
	for i := range 40 {
		env.SetHeader("X-Header-"+strconv.Itoa(100+i), strings.Repeat("v", 60))
	}
	env.Headers["bad header"] = []string{"v"}
	meta, err := b.build(env, true)
	require.NoError(t, err)
	used := 0
	for k, v := range meta {
		used += len(k) + len(v)
	}
	assert.LessOrEqual(t, used, 2<<10, "within the 2KiB S3 takes")
	assert.Equal(t, env.ID, meta["idempotency-key"], "the idempotency key goes first")
	assert.Contains(t, logs.String(), "over the 2KiB")
	assert.Contains(t, logs.String(), "bad header")
	_, err = b.build(env, true)
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(logs.String(), "over the 2KiB"), "logged once")

	b.headers, err = compileHeaders(map[string]string{"x": "{{ index .json 1 }}"})
	require.NoError(t, err)
	_, err = b.build(newEnv(`{}`), true)
	class, _ := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
}

// failingKV is a store whose reads fail.
type failingKV struct{ store.Store }

func (failingKV) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, assert.AnError
}

func TestS3SourceKeepWithAFailingStoreSkips(t *testing.T) {
	f := newFakeS3(t)
	f.put(t, "a.json", `{"n":1}`, "", nil)
	r := runS3(t, f, &config.Source{After: "keep"}, failingKV{store.NewMemory("m")})
	noMessage(t, r.out, 300*time.Millisecond)
	assert.Contains(t, r.logs.String(), "cannot tell whether an object was read already")
	r.src.mu.Lock()
	assert.Empty(t, r.src.inflight)
	r.src.mu.Unlock()
}
