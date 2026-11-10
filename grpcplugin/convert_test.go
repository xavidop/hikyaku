package grpcplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
	"github.com/xavidop/hikyaku/plugin"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	ts, err := trace.ParseTraceState("vendor=value")
	require.NoError(t, err)
	env := envelope.New("in", "http", []byte{0xff, 0x00, 'x'})
	env.Subject = "/a"
	env.ContentType = "application/octet-stream"
	env.Headers["X-Multi"] = []string{"1", "2"}
	env.SetAttribute("s", "v")
	env.SetAttribute("m", map[string]any{"k": 1})
	env.Trace = trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, TraceState: ts,
	})

	// Through the wire bytes, as the call carries it.
	b, err := proto.Marshal(envelopeToPB(env))
	require.NoError(t, err)
	var pe pluginpb.Envelope
	require.NoError(t, proto.Unmarshal(b, &pe))
	got, err := envelopeFromPB(&pe)
	require.NoError(t, err)

	assert.Equal(t, env.ID, got.ID)
	assert.Equal(t, "in", got.Source)
	assert.Equal(t, "http", got.Protocol)
	assert.Equal(t, "/a", got.Subject)
	assert.Equal(t, env.ContentType, got.ContentType)
	assert.Equal(t, env.Body, got.Body, "bodies are bytes, kept as they are")
	assert.Equal(t, env.Headers, got.Headers)
	assert.Equal(t, map[string]any{"s": "v", "m": map[string]any{"k": float64(1)}}, got.Attributes, "attributes as their JSON wire form reads them")
	assert.True(t, env.ReceivedAt.Equal(got.ReceivedAt))
	assert.Equal(t, env.Trace.TraceID(), got.Trace.TraceID())
	assert.Equal(t, env.Trace.SpanID(), got.Trace.SpanID())
	assert.Equal(t, env.Trace.TraceFlags(), got.Trace.TraceFlags())
	assert.Equal(t, "vendor=value", got.Trace.TraceState().String())
}

func TestEnvelopeEdgeCases(t *testing.T) {
	env := &envelope.Envelope{ID: "id", Headers: map[string][]string{"Bin": {"a\xffb"}}, Subject: "s\xfe"}
	env.SetAttribute("ok", 1)
	env.SetAttribute("func", func() {})
	pe := envelopeToPB(env)
	_, err := proto.Marshal(pe)
	require.NoError(t, err, "invalid UTF-8 does not fail the call")
	assert.Equal(t, "a�b", pe.GetHeaders()["Bin"].GetValues()[0])
	assert.Equal(t, "s�", pe.GetSubject())
	assert.JSONEq(t, `{"ok":1}`, pe.GetAttributesJson(), "what JSON cannot carry is dropped, the rest kept")
	assert.Nil(t, pe.GetReceivedAt(), "no receive time, none on the wire")
	assert.Nil(t, pe.GetTrace())

	got, err := envelopeFromPB(&pluginpb.Envelope{Id: "x", AttributesJson: "{not json"})
	assert.ErrorContains(t, err, "attributes_json")
	require.NotNil(t, got, "the message is still usable")
	assert.Equal(t, "x", got.ID)
	assert.NotNil(t, got.Headers)
	assert.NotNil(t, got.Attributes)
	assert.True(t, got.ReceivedAt.IsZero())
	assert.False(t, got.Trace.IsValid())
}

func TestResultRoundTrip(t *testing.T) {
	r := &plugin.Result{Status: 201, Headers: map[string][]string{"A": {"b"}}, Body: []byte("ok"), ContentType: "text/plain", Endpoint: "file.ndjson", Reroute: []string{"x"}}
	assert.Equal(t, r, resultFromPB(resultToPB(r)))
	assert.Nil(t, resultToPB(nil))
	assert.Nil(t, resultFromPB(nil))
	assert.Nil(t, resultFromPB(&pluginpb.Result{}).Headers, "no headers stay nil")
}

func TestErrorRoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		class      plugin.Class
		retryAfter time.Duration
		tags       []string
	}{
		{"classified", &plugin.Error{Class: plugin.RateLimited, RetryAfter: 2500 * time.Millisecond, Tags: []string{"kafka:THROTTLED"}, Err: errors.New("slow down")}, plugin.RateLimited, 2500 * time.Millisecond, []string{"kafka:THROTTLED", "429"}},
		{"status", plugin.StatusError(503, 0, "busy"), plugin.Transient, 0, []string{"503", "5xx"}},
		{"permanent", plugin.Permanentf("bad"), plugin.Permanent, 0, nil},
		{"deadline", fmt.Errorf("write: %w", context.DeadlineExceeded), plugin.Timeout, 0, []string{"timeout"}},
		{"refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, plugin.Transient, 0, []string{"connect"}},
		{"plain", errors.New("boom"), plugin.Transient, 0, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := proto.Marshal(errorToPB(c.err))
			require.NoError(t, err)
			var pe pluginpb.Error
			require.NoError(t, proto.Unmarshal(b, &pe))
			got := errorFromPB(&pe)
			var e *plugin.Error
			require.ErrorAs(t, got, &e)
			assert.Equal(t, c.class, e.Class)
			assert.Equal(t, c.retryAfter, e.RetryAfter)
			assert.Equal(t, c.tags, e.Tags)
			assert.Equal(t, c.err.Error(), got.Error())
		})
	}
	assert.Nil(t, errorToPB(nil))
	assert.NoError(t, errorFromPB(nil))
	unknown := errorFromPB(&pluginpb.Error{Class: pluginpb.ErrorClass(42), RetryAfterMs: -5})
	var e *plugin.Error
	require.ErrorAs(t, unknown, &e)
	assert.Equal(t, plugin.Transient, e.Class, "a class this side does not know is retried")
	assert.Zero(t, e.RetryAfter)
	assert.Equal(t, "transient", unknown.Error())
}

func TestFailure(t *testing.T) {
	assert.NoError(t, failure(nil))
	assert.EqualError(t, failure(&pluginpb.Error{Message: "disk full"}), "disk full")
	assert.EqualError(t, failure(&pluginpb.Error{Class: pluginpb.ErrorClass_ERROR_CLASS_PERMANENT}), "permanent")
}
