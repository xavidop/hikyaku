package azure

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func newTestSBSink(t *testing.T, ep config.Endpoint) (*sbSink, *fakeSB) {
	t.Helper()
	if ep.Azure == nil {
		ep.Azure = connStr()
	}
	spec := plugin.Spec{Name: "out", Kind: kindServiceBus, Config: &ep}
	opts, err := parseSBSink(spec)
	require.NoError(t, err)
	fake := newFakeSB()
	s, err := buildSBSink(spec, opts, fake)
	require.NoError(t, err)
	return s, fake
}

func TestSBSinkDeliver(t *testing.T) {
	s, fake := newTestSBSink(t, config.Endpoint{Queue: "orders", GroupID: "{{ .json.customer }}",
		Headers: map[string]string{"X-Team": "core"}})
	env := envelope.New("in", "http", []byte(`{"customer":"c-42"}`))
	env.ID = "msg-1"
	env.Subject = "/hooks/orders"
	env.ContentType = "application/json"
	env.Headers = map[string][]string{"Idempotency-Key": {"msg-1"}, "X-Trace": {"t1"}, "Authorization": {"Bearer s"}}

	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, "ns.servicebus.windows.net/orders", res.Endpoint)
	require.Len(t, fake.sender.sent, 1)
	m := fake.sender.sent[0]
	assert.Equal(t, "msg-1", *m.MessageID, "the envelope ID is the MessageId: duplicate detection")
	assert.Equal(t, "c-42", *m.SessionID)
	assert.Equal(t, "/hooks/orders", *m.Subject)
	assert.Equal(t, "application/json", *m.ContentType)
	assert.Equal(t, env.Body, m.Body)
	assert.Equal(t, map[string]any{"Idempotency-Key": "msg-1", "X-Trace": "t1", "X-Team": "core"}, m.ApplicationProperties)

	// No group_id, subject or content type: none set.
	s, fake = newTestSBSink(t, config.Endpoint{Topic: "events"})
	_, err = s.Deliver(context.Background(), envelope.New("in", "http", []byte("x")))
	require.NoError(t, err)
	m = fake.sender.sent[0]
	assert.Nil(t, m.SessionID)
	assert.Nil(t, m.Subject)
	assert.Nil(t, m.ContentType)
	assert.Contains(t, m.ApplicationProperties, "Idempotency-Key")
}

func TestSBSinkLongIDs(t *testing.T) {
	long := strings.Repeat("k", 200)
	assert.Equal(t, "short", messageID("short"))
	assert.Len(t, messageID(long), 64)
	assert.Equal(t, messageID(long), messageID(long), "the same key hashes the same: duplicate detection still works")

	s, _ := newTestSBSink(t, config.Endpoint{Queue: "q", GroupID: "{{ .json.s }}"})
	_, err := s.Deliver(context.Background(), envelope.New("in", "http", []byte(`{"s":"`+long+`"}`)))
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.ErrorContains(t, err, "session ID of 200 characters")

	s, _ = newTestSBSink(t, config.Endpoint{Queue: "q", GroupID: `{{ index .json "a" "b" }}`})
	_, err = s.Deliver(context.Background(), envelope.New("in", "http", []byte(`{}`)))
	assert.Equal(t, []string{"template"}, plugin.Tags(err))
}

func TestSBSinkBatches(t *testing.T) {
	s, fake := newTestSBSink(t, config.Endpoint{Queue: "q", GroupID: "{{ .json.s }}"})
	fake.sender.maxPerBatch = 2
	var envs []*envelope.Envelope
	for i, sid := range []string{"a", "b", "a", "a", "", "b", "a"} {
		env := envelope.New("in", "http", []byte(`{"s":"`+sid+`"}`))
		env.ID = sid + string(rune('0'+i))
		envs = append(envs, env)
	}
	res, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	assert.Equal(t, "ns.servicebus.windows.net/q", res.Endpoint)

	var got [][]string
	for _, b := range fake.sender.batches {
		var ids []string
		for _, m := range b {
			ids = append(ids, *m.MessageID)
		}
		got = append(got, ids)
	}
	assert.Equal(t, [][]string{{"a0", "a2"}, {"a3", "a6"}, {"b1", "b5"}, {"4"}}, got,
		"one session at a time, in order; a full batch is sent and a new one started")

	// A message too large for an empty batch fails the whole batch.
	big := envelope.New("in", "http", []byte(strings.Repeat("x", 2000)))
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{envs[0], big})
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Permanent, pe.Class)
	assert.Equal(t, []string{"servicebus:message_too_large"}, pe.Tags)
	assert.ErrorIs(t, err, azservicebus.ErrMessageTooLarge)

	// A template error fails it too, before anything is sent.
	sent := len(fake.sender.sent)
	bad := envelope.New("in", "http", []byte(`not json`))
	s2, fake2 := newTestSBSink(t, config.Endpoint{Queue: "q", GroupID: `{{ index .json "a" "b" }}`})
	_, err = s2.DeliverBatch(context.Background(), []*envelope.Envelope{bad})
	assert.Error(t, err)
	assert.Empty(t, fake2.sender.sent)
	assert.Equal(t, sent, len(fake.sender.sent))

	// Send and batch failures are classified.
	fake.sender.sendErr = &amqp.Error{Condition: "com.microsoft:server-busy"}
	_, err = s.DeliverBatch(context.Background(), envs[:1])
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.RateLimited, pe.Class)
	assert.Equal(t, throttleWait, pe.RetryAfter)
	fake.sender.sendErr = nil
	fake.sender.batchErr = &azservicebus.Error{Code: azservicebus.CodeNotFound}
	_, err = s.DeliverBatch(context.Background(), envs[:1])
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Permanent, pe.Class)
	assert.Equal(t, []string{"servicebus:notfound"}, pe.Tags)
}

func TestSBSinkErrorsHealthClose(t *testing.T) {
	s, fake := newTestSBSink(t, config.Endpoint{Queue: "q"})
	ctx := context.Background()
	env := envelope.New("in", "http", []byte("x"))

	fake.sender.sendErr = &azservicebus.Error{Code: azservicebus.CodeConnectionLost}
	_, err := s.Deliver(ctx, env)
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Transient, pe.Class)
	assert.Equal(t, []string{"servicebus:connlost", "connect"}, pe.Tags)
	assert.ErrorContains(t, err, "send to ns.servicebus.windows.net/q")
	assert.NotContains(t, err.Error(), "SharedAccessKey")

	fake.sender.sendErr = &azservicebus.Error{Code: azservicebus.CodeUnauthorizedAccess}
	_, err = s.Deliver(ctx, env)
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Permanent, pe.Class)
	fake.sender.sendErr = nil

	require.NoError(t, s.Health(ctx))
	fake.sender.batchErr = &azservicebus.Error{Code: azservicebus.CodeUnauthorizedAccess}
	assert.ErrorContains(t, s.Health(ctx), "unauthorized")
	fake.sender.batchErr = nil

	require.NoError(t, s.Close(ctx))
	require.NoError(t, s.Close(ctx))
	assert.Equal(t, 1, fake.sender.closed)
	assert.True(t, fake.closed.Load())
	assert.ErrorIs(t, s.Health(ctx), errClosed)
	_, err = s.Deliver(ctx, env)
	assert.ErrorIs(t, err, errClosed)
	_, err = s.DeliverBatch(ctx, []*envelope.Envelope{env})
	assert.ErrorIs(t, err, errClosed)
}

type failingSB struct{ *fakeSB }

func (failingSB) newSender(string) (sbSender, error) { return nil, errors.New("no sender") }

func TestSBSinkSenderFailure(t *testing.T) {
	ep := config.Endpoint{Queue: "q", Azure: connStr()}
	spec := plugin.Spec{Name: "out", Config: &ep}
	opts, err := parseSBSink(spec)
	require.NoError(t, err)
	fake := failingSB{newFakeSB()}
	_, err = buildSBSink(spec, opts, fake)
	assert.ErrorContains(t, err, "no sender")
	assert.True(t, fake.closed.Load(), "the client is closed")
}
