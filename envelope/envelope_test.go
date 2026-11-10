package envelope

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSetsIDAndMaps(t *testing.T) {
	e := New("webhooks", "http", []byte(`{}`))
	assert.Len(t, e.ID, 26, "a ULID is 26 characters")
	assert.NotNil(t, e.Headers)
	assert.NotNil(t, e.Attributes)
	assert.False(t, e.ReceivedAt.IsZero())
	assert.NotEqual(t, e.ID, New("webhooks", "http", nil).ID)
}

func TestEnvelopeJSONCachedAndInvalidated(t *testing.T) {
	e := New("s", "http", []byte(`{"type":"invoice.paid","n":1}`))
	v, err := e.JSON()
	require.NoError(t, err)
	assert.Equal(t, "invoice.paid", v.(map[string]any)["type"])

	// Cached: the same map comes back.
	v2, _ := e.JSON()
	v.(map[string]any)["marker"] = true
	assert.Equal(t, true, v2.(map[string]any)["marker"])

	e.SetBody([]byte(`{"type":"bug"}`))
	v3, err := e.JSON()
	require.NoError(t, err)
	assert.Equal(t, "bug", v3.(map[string]any)["type"])
	assert.NotContains(t, v3.(map[string]any), "marker")
}

func TestEnvelopeJSONInvalidBody(t *testing.T) {
	e := New("s", "http", []byte(`not json`))
	v, err := e.JSON()
	assert.Error(t, err)
	assert.Nil(t, v)
	assert.Nil(t, e.JSONOrNil())

	empty := New("s", "http", nil)
	v, err = empty.JSON()
	assert.NoError(t, err)
	assert.Nil(t, v)
}

func TestHeadersCaseInsensitive(t *testing.T) {
	e := New("s", "http", nil)
	e.Headers["x-signature-256"] = []string{"abc"}
	assert.Equal(t, "abc", e.Header("X-Signature-256"))
	e.SetHeader("X-SIGNATURE-256", "def")
	assert.Equal(t, "def", e.Header("x-signature-256"))
	assert.Len(t, e.Headers, 1)
	e.DelHeader("x-Signature-256")
	assert.Empty(t, e.Headers)
}

func TestCloneIsIndependentButSharesAck(t *testing.T) {
	var acks atomic.Int32
	e := New("s", "http", []byte("body"))
	e.Headers["A"] = []string{"1"}
	e.Attributes["k"] = "v"
	e.OnAck(func(context.Context, Outcome) { acks.Add(1) })

	c := e.Clone()
	c.Headers["A"][0] = "2"
	c.Body[0] = 'B'
	c.Attributes["k"] = "changed"
	assert.Equal(t, "1", e.Headers["A"][0])
	assert.Equal(t, "body", string(e.Body))
	assert.Equal(t, "v", e.Attributes["k"])

	c.Ack(context.Background(), Outcome{Status: Delivered})
	e.Ack(context.Background(), Outcome{Status: Failed})
	assert.Equal(t, int32(1), acks.Load())
}

func TestAckOnce(t *testing.T) {
	var got []AckStatus
	var mu sync.Mutex
	e := New("s", "http", nil)
	e.OnAck(func(_ context.Context, o Outcome) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, o.Status)
	})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.Ack(context.Background(), Outcome{Status: Delivered}) }()
	}
	wg.Wait()
	assert.Equal(t, []AckStatus{Delivered}, got)

	// No callback: Ack is a no-op.
	New("s", "http", nil).Ack(context.Background(), Outcome{})
}

func TestAckStatus(t *testing.T) {
	for s, want := range map[AckStatus]string{Delivered: "delivered", DeadLettered: "dead_lettered", Unrouted: "unrouted", Filtered: "filtered", Failed: "failed", AckStatus(99): "unknown"} {
		assert.Equal(t, want, s.String())
	}
	assert.True(t, DeadLettered.Acked())
	assert.False(t, Failed.Acked())
}

func TestConcurrentAttributes(t *testing.T) {
	e := New("s", "http", nil)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e.SetAttribute("k", i)
			_, _ = e.Attribute("k")
			_ = e.AttributesCopy()
		}(i)
	}
	wg.Wait()
	_, ok := e.Attribute("k")
	assert.True(t, ok)
}

func TestWireRoundTripJSONBody(t *testing.T) {
	e := New("webhooks", "http", []byte(`{"a":1,"b":"x"}`))
	e.Subject = "/ingest"
	e.ContentType = "application/json"
	e.Headers["X-Id"] = []string{"7"}
	e.Attributes["dlq"] = map[string]any{"attempts": float64(3)}
	b, err := json.Marshal(e)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"body":{"a":1,"b":"x"}`, "compact JSON is embedded as is")

	var back Envelope
	require.NoError(t, json.Unmarshal(b, &back))
	assert.Equal(t, e.ID, back.ID)
	assert.Equal(t, e.Body, back.Body)
	assert.Equal(t, e.Subject, back.Subject)
	assert.Equal(t, e.Headers, back.Headers)
	assert.Equal(t, e.Attributes, back.Attributes)
	assert.True(t, e.ReceivedAt.Equal(back.ReceivedAt))
}

func TestWireRoundTripText(t *testing.T) {
	for _, body := range []string{"plain text", `{"a": 1}`, `{"html":"<b>"}`, "line1\nline2"} {
		e := New("s", "stdin", []byte(body))
		b, err := json.Marshal(e)
		require.NoError(t, err)
		var back Envelope
		require.NoError(t, json.Unmarshal(b, &back))
		assert.Equal(t, body, string(back.Body), "body %q must round-trip byte for byte", body)
	}
}

func TestWireRoundTripBinary(t *testing.T) {
	body := []byte{0xff, 0x00, 0xfe, 0x10}
	e := New("s", "ws", body)
	b, err := json.Marshal(e)
	require.NoError(t, err)
	assert.Contains(t, string(b), "body_base64")
	var back Envelope
	require.NoError(t, json.Unmarshal(b, &back))
	assert.Equal(t, body, back.Body)
}

func TestWireRejectsMissingID(t *testing.T) {
	var e Envelope
	assert.Error(t, json.Unmarshal([]byte(`{"body":{}}`), &e))
	assert.Error(t, json.Unmarshal([]byte(`{"id":"x","body_base64":"%%%"}`), &e))
}

func TestOnQueuedRunsOnceAndClonesShareIt(t *testing.T) {
	e := New("src", "test", []byte(`{}`))
	n := 0
	e.OnQueued(func() { n++ })
	c := e.Clone()
	c.Queued()
	e.Queued()
	assert.Equal(t, 1, n, "the callback runs once, whichever copy reports it")
	New("src", "test", nil).Queued() // no callback: a no-op
}
