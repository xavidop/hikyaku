package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
)

func TestClassifyHTTPStatus(t *testing.T) {
	cases := []struct {
		code  int
		class Class
		tags  []string
	}{
		{429, RateLimited, []string{"429", Tag4xx}},
		{503, Transient, []string{"503", Tag5xx}},
		{500, Transient, []string{"500", Tag5xx}},
		{404, Permanent, []string{"404", Tag4xx}},
		{400, Permanent, []string{"400", Tag4xx}},
		{408, Timeout, []string{"408", Tag4xx, TagTimeout}},
		{504, Timeout, []string{"504", Tag5xx, TagTimeout}},
	}
	for _, c := range cases {
		t.Run(fmt.Sprint(c.code), func(t *testing.T) {
			err := StatusError(c.code, 0, "")
			class, _ := Classify(err)
			assert.Equal(t, c.class, class)
			assert.ElementsMatch(t, c.tags, Tags(err))
		})
	}

	err := StatusError(429, 2*time.Second, "slow down")
	class, after := Classify(fmt.Errorf("wrapped: %w", err))
	assert.Equal(t, RateLimited, class)
	assert.Equal(t, 2*time.Second, after)
	assert.Contains(t, err.Error(), "429 Too Many Requests: slow down")
}

func TestTagsNetErrors(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	assert.Equal(t, []string{TagConnect}, Tags(dial))
	class, _ := Classify(dial)
	assert.Equal(t, Transient, class)

	assert.Equal(t, []string{TagTimeout}, Tags(context.DeadlineExceeded))
	class, _ = Classify(fmt.Errorf("post: %w", context.DeadlineExceeded))
	assert.Equal(t, Timeout, class)

	assert.Nil(t, Tags(errors.New("something odd")))
	class, _ = Classify(errors.New("something odd"))
	assert.Equal(t, Transient, class)

	assert.Equal(t, []string{TagConnect}, Tags(&net.DNSError{Err: "no such host", Name: "x"}))
}

func TestErrorHelpers(t *testing.T) {
	err := Errorf(Transient, []string{"ws:close_1011"}, "closed: %d", 1011)
	assert.Equal(t, []string{"ws:close_1011"}, Tags(err))
	assert.Equal(t, "closed: 1011", err.Error())

	base := errors.New("boom")
	w := Wrap(Permanent, nil, base)
	assert.ErrorIs(t, w, base)
	assert.Nil(t, Wrap(Permanent, nil, nil))

	p := Permanentf("bad payload")
	class, _ := Classify(p)
	assert.Equal(t, Permanent, class)

	assert.Equal(t, "rate_limited", RateLimited.String())
	assert.Equal(t, "unknown", Class(9).String())
	assert.Equal(t, "timeout", (&Error{Class: Timeout}).Error())
	assert.Contains(t, Tags(&Error{Class: Timeout}), TagTimeout)
	assert.Contains(t, Tags(&Error{Class: RateLimited}), Tag429)
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, 3*time.Second, ParseRetryAfter("3", now))
	assert.Equal(t, 1500*time.Millisecond, ParseRetryAfter("1.5", now))
	assert.Equal(t, time.Duration(0), ParseRetryAfter("", now))
	assert.Equal(t, time.Duration(0), ParseRetryAfter("-1", now))
	assert.Equal(t, time.Duration(0), ParseRetryAfter("soon", now))
	date := now.Add(90 * time.Second).Format(http.TimeFormat)
	assert.Equal(t, 90*time.Second, ParseRetryAfter(date, now))
	past := now.Add(-time.Minute).Format(http.TimeFormat)
	assert.Equal(t, time.Duration(0), ParseRetryAfter(past, now))
}

type nopSink struct{ name string }

func (s nopSink) Name() string { return s.name }
func (nopSink) Deliver(context.Context, *envelope.Envelope) (*Result, error) {
	return &Result{Status: 200}, nil
}
func (nopSink) Health(context.Context) error { return nil }
func (nopSink) Close(context.Context) error  { return nil }

func TestRegistry(t *testing.T) {
	RegisterSink("test-nop", func(_ context.Context, s Spec) (Sink, error) { return nopSink{name: s.Name}, nil })
	f, ok := LookupSink("test-nop")
	require.True(t, ok)
	sink, err := f(context.Background(), Spec{Name: "a"})
	require.NoError(t, err)
	assert.Equal(t, "a", sink.Name())
	assert.Contains(t, SinkKinds(), "test-nop")

	sink, err = NewSink(context.Background(), Spec{Name: "b", Kind: "test-nop"})
	require.NoError(t, err)
	assert.Equal(t, "b", sink.Name())

	_, err = NewSink(context.Background(), Spec{Name: "q", Kind: "kafka"})
	assert.ErrorContains(t, err, `protocol "kafka" is not available in this build`)
	_, err = NewSource(context.Background(), Spec{Name: "q", Kind: "kafka"})
	assert.ErrorContains(t, err, "source q")
	_, ok = LookupSource("kafka")
	assert.False(t, ok)
	assert.NotContains(t, SourceKinds(), "kafka")

	var r Router = RouterFunc(func(context.Context, *envelope.Envelope) (Decision, error) {
		return Decision{Destinations: []string{"x"}}, nil
	})
	d, err := r.Route(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, d.Destinations)

	assert.Nil(t, (*Result)(nil).Reply())
	assert.Equal(t, 201, (&Result{Status: 201}).Reply().Status)
	assert.NotNil(t, Env{}.Log())
}

type chatOptions struct {
	WebhookURL string `yaml:"webhook_url"`
	Retries    int    `yaml:"retries,omitempty"`
}

func TestDecodeOptionsIsStrict(t *testing.T) {
	var o chatOptions
	spec := Spec{Name: "chat", Kind: "x-chat", Options: map[string]any{"webhook_url": "https://x", "retries": 2}}
	require.NoError(t, spec.Decode(&o))
	assert.Equal(t, chatOptions{WebhookURL: "https://x", Retries: 2}, o)

	err := Spec{Name: "chat", Kind: "x-chat", Options: map[string]any{"webhook_ur": "typo"}}.Decode(&o)
	assert.ErrorContains(t, err, "x-chat chat: options:")
	assert.ErrorContains(t, err, "webhook_ur")

	err = DecodeOptions(map[string]any{"retries": "many"}, &o)
	assert.Error(t, err)
	var empty chatOptions
	assert.NoError(t, DecodeOptions(nil, &empty), "no options block decodes to the zero value")
}

func TestRegistrations(t *testing.T) {
	RegisterSink("x-test-reg", func(context.Context, Spec) (Sink, error) { return nopSink{}, nil },
		WithOptions(chatOptions{}), WithDescription("a test plugin"))
	var found *Registration
	for _, r := range SinkRegistrations() {
		if r.Kind == "x-test-reg" {
			r := r
			found = &r
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, "a test plugin", found.Description)
	assert.IsType(t, chatOptions{}, found.Options)
	assert.NotNil(t, SourceRegistrations())
}

func TestOnSettle(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		status envelope.AckStatus
		final  bool
	}{
		{envelope.Delivered, true}, {envelope.DeadLettered, true}, {envelope.Unrouted, true},
		{envelope.Filtered, true}, {envelope.Failed, false},
	} {
		env := envelope.New("s", "test", nil)
		var got []bool
		OnSettle(env, "", func(_ context.Context, final bool) { got = append(got, final) })
		env.Queued()
		env.Ack(ctx, envelope.Outcome{Status: c.status})
		assert.Equal(t, []bool{c.final}, got, c.status.String())
	}
	env := envelope.New("s", "test", nil)
	var got []bool
	OnSettle(env, "accepted", func(_ context.Context, final bool) { got = append(got, final) })
	env.Queued()
	env.Ack(ctx, envelope.Outcome{Status: envelope.Failed})
	assert.Equal(t, []bool{true}, got, "accepted settles once the relay holds the message")
}

func TestRetryable(t *testing.T) {
	retryOn := []string{"connect", "timeout", "5xx", "429"}
	neverOn := []string{"4xx"}
	for name, c := range map[string]struct {
		class Class
		tags  []string
		want  bool
	}{
		"a 503 retries":                          {Transient, []string{"5xx", "503"}, true},
		"a 404 does not":                         {Permanent, []string{"4xx", "404"}, false},
		"429 beats 4xx":                          {RateLimited, []string{"4xx", "429"}, true},
		"an unlisted transient tag retries":      {Transient, []string{"ws:close_1011"}, true},
		"an unlisted permanent failure does not": {Permanent, []string{"kafka:INVALID_RECORD"}, false},
		"tags compare without case":              {Transient, []string{"5XX"}, true},
	} {
		assert.Equal(t, c.want, Retryable(c.class, c.tags, retryOn, neverOn), name)
	}
	assert.False(t, Retryable(Transient, []string{"5xx", "503"}, retryOn, []string{"503"}), "a code in never_on beats the 5xx range in retry_on")
	assert.False(t, Retryable(Transient, []string{"ws:close_1011"}, nil, []string{"WS:CLOSE_1011"}), "a listed protocol tag stops retries")
}
