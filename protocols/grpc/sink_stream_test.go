package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/protocols/grpc/internal/echopb"
)

// texts are the texts of requests.
func texts(reqs []*echopb.SayRequest) []string {
	out := make([]string, len(reqs))
	for i, r := range reqs {
		out[i] = r.GetText()
	}
	return out
}

func TestSinkStreamingMethods(t *testing.T) {
	for _, via := range []string{"descriptor_set", "reflection"} {
		t.Run(via, func(t *testing.T) {
			es := &echoServer{header: metadata.Pairs("x-served-by", "echo-1", rerouteKey, "billing")}
			srv := startEcho(t, es, serverOpts{})
			sink := func(method string) *Sink {
				ep := config.Endpoint{URL: "grpc://" + srv.addr, Method: method}
				if via == "descriptor_set" {
					ep.DescriptorSet = echoSet
				}
				return newTestSink(t, ep)
			}

			env := msg(`{"text":"hi"}`)
			res, err := sink(collectMethod).Deliver(context.Background(), env)
			require.NoError(t, err, "client-streaming")
			assert.JSONEq(t, `{"received":1,"last":"hi"}`, string(res.Body), "the one response")
			assert.Equal(t, contentJSON, res.ContentType)
			assert.Equal(t, "grpc://"+srv.addr, res.Endpoint)
			assert.Equal(t, []string{"echo-1"}, res.Headers["X-Served-By"])
			assert.Equal(t, []string{"billing"}, res.Reroute)
			require.Len(t, es.collected(), 1)
			assert.Equal(t, []string{"hi"}, texts(es.collected()[0]), "the message is the stream's one request")
			calls := es.calls()
			assert.Equal(t, []string{env.ID}, calls[len(calls)-1].Get("idempotency-key"))

			res, err = sink(watchMethod).Deliver(context.Background(), msg(`{"topic":"t","limit":3}`))
			require.NoError(t, err, "server-streaming")
			assert.JSONEq(t, `[{"topic":"t","seq":"1","data":""},{"topic":"t","seq":"2","data":""},{"topic":"t","seq":"3","data":""}]`,
				string(res.Body), "every response, as a JSON array")
			assert.Equal(t, contentJSON, res.ContentType)
			assert.Equal(t, []string{"test"}, res.Headers["X-Feed"])

			res, err = sink(watchMethod).Deliver(context.Background(), msg(`{"topic":"t"}`))
			require.NoError(t, err)
			assert.Equal(t, "[]", string(res.Body), "a stream without responses")

			res, err = sink(talkMethod).Deliver(context.Background(), msg(`{"text":"hi","count":1}`))
			require.NoError(t, err, "bidi")
			assert.JSONEq(t, `[{"text":"HI","count":2}]`, string(res.Body))
			assert.Equal(t, []string{"billing"}, res.Reroute)
			talks := es.talked()
			assert.Equal(t, []string{"hi"}, texts(talks[len(talks)-1]))
		})
	}
}

func TestSinkStreamingErrors(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	collect := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: collectMethod, DescriptorSet: echoSet})
	talk := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: talkMethod, DescriptorSet: echoSet})
	for _, c := range []struct {
		code  codes.Code
		class plugin.Class
		tags  []string
	}{
		{codes.Unavailable, plugin.Transient, []string{"grpc:UNAVAILABLE", "5xx"}},
		{codes.Internal, plugin.Transient, []string{"grpc:INTERNAL", "5xx"}},
		{codes.ResourceExhausted, plugin.RateLimited, []string{"grpc:RESOURCE_EXHAUSTED", "429"}},
		{codes.InvalidArgument, plugin.Permanent, []string{"grpc:INVALID_ARGUMENT", "4xx"}},
	} {
		t.Run(c.code.String(), func(t *testing.T) {
			es.set(func(es *echoServer) { es.fail = status.Error(c.code, "nope") })
			_, err := collect.Deliver(context.Background(), msg(`{"text":"x"}`))
			require.Error(t, err)
			class, _ := plugin.Classify(err)
			assert.Equal(t, c.class, class)
			assert.Equal(t, c.tags, plugin.Tags(err), "classified as a unary call is")
			assert.ErrorContains(t, err, collectMethod+": "+codeName(c.code)+": nope")

			_, err = talk.Deliver(context.Background(), msg(`{"text":"x"}`))
			require.Error(t, err, "a status after responses fails the delivery")
			class, _ = plugin.Classify(err)
			assert.Equal(t, c.class, class)
			assert.Equal(t, c.tags, plugin.Tags(err))
		})
	}

	es.set(func(es *echoServer) {
		es.fail, es.trailer = status.Error(codes.ResourceExhausted, "slow down"), metadata.Pairs("retry-after", "2")
	})
	_, err := collect.Deliver(context.Background(), msg(`{}`))
	_, retryAfter := plugin.Classify(err)
	assert.Equal(t, 2*time.Second, retryAfter, "the trailers' retry hint")

	es.set(func(es *echoServer) { es.fail, es.trailer, es.delay = nil, nil, time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = collect.Deliver(ctx, msg(`{}`))
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)
	assert.Equal(t, []string{"grpc:DEADLINE_EXCEEDED", "timeout"}, plugin.Tags(err))

	es.set(func(es *echoServer) { es.delay, es.hold = 0, true })
	watch := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: watchMethod, DescriptorSet: echoSet})
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = watch.Deliver(ctx, msg(`{"topic":"t","limit":2}`))
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class, "a stream that does not end within the attempt's deadline")

	for _, method := range []string{collectMethod, watchMethod, talkMethod} {
		down := newTestSink(t, config.Endpoint{URL: "grpc://" + closedAddr(t), Method: method, DescriptorSet: echoSet})
		_, err := down.Deliver(context.Background(), msg(`{}`))
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Transient, class, method)
		assert.Contains(t, plugin.Tags(err), plugin.TagConnect, method)
	}

	_, err = talk.Deliver(context.Background(), msg(`{"text":1}`))
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "a message that does not fit")
	assert.ErrorContains(t, err, "the message does not fit hikyaku.test.echo.v1.SayRequest")
}

// The responses of a stream fill a reply of at most maxReply; the ones
// past it are read and dropped.
func TestSinkStreamReplyIsCut(t *testing.T) {
	es := &echoServer{data: strings.Repeat("x", 100)}
	srv := startEcho(t, es, serverOpts{})
	logs := &records{}
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: watchMethod, DescriptorSet: echoSet})
	s.log = slog.New(logs)
	s.replyLimit = 400
	res, err := s.Deliver(context.Background(), msg(`{"topic":"t","limit":10}`))
	require.NoError(t, err)
	var events []map[string]any
	require.NoError(t, json.Unmarshal(res.Body, &events), "still a JSON array")
	assert.Len(t, events, 2, "as many responses as fit")
	assert.LessOrEqual(t, len(res.Body), 400)
	assert.Equal(t, 1, logs.count(slog.LevelWarn, "grpc reply cut: the responses are larger than the 10MiB a reply holds"))
}

func TestSinkBatch(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: collectMethod, DescriptorSet: echoSet,
		Headers: map[string]string{"x-tenant": "{{ .json.tenant }}", "x-static": "yes"}})
	member := func(text, tenant, request string) *envelope.Envelope {
		env := msg(`{"text":"` + text + `","tenant":"` + tenant + `"}`)
		env.Headers = map[string][]string{"X-Shared": {"same"}, "X-Request-Id": {request}}
		return env
	}
	envs := []*envelope.Envelope{member("a", "acme", "r1"), member("b", "acme", "r2"), member("c", "acme", "r3")}
	res, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	assert.JSONEq(t, `{"received":3,"last":"c"}`, string(res.Body), "one call, one request per message")
	require.Len(t, es.collected(), 1)
	assert.Equal(t, []string{"a", "b", "c"}, texts(es.collected()[0]), "in order")
	md := es.calls()[0]
	assert.Equal(t, []string{"acme"}, md.Get("x-tenant"), "what every message agrees on")
	assert.Equal(t, []string{"yes"}, md.Get("x-static"))
	assert.Equal(t, []string{"same"}, md.Get("x-shared"))
	assert.Empty(t, md.Get("x-request-id"), "what they do not agree on is left out")
	key := md.Get("idempotency-key")
	require.Len(t, key, 1)
	assert.True(t, strings.HasPrefix(key[0], "batch-"), key[0])
	assert.Equal(t, batchKey(envs), key[0])

	_, err = s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	assert.Equal(t, key, es.calls()[1].Get("idempotency-key"), "a batch tried again has the same key")
	assert.NotEqual(t, batchKey(envs), batchKey(envs[:2]))
	assert.NotEqual(t, batchKey(envs), batchKey([]*envelope.Envelope{envs[1], envs[0], envs[2]}))

	mixed := []*envelope.Envelope{member("a", "acme", "r1"), member("b", "globex", "r1")}
	_, err = s.DeliverBatch(context.Background(), mixed)
	require.NoError(t, err)
	assert.Empty(t, es.calls()[2].Get("x-tenant"))
	assert.Equal(t, []string{"r1"}, es.calls()[2].Get("x-request-id"))

	one := member("solo", "acme", "r9")
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{one})
	require.NoError(t, err)
	assert.Equal(t, []string{one.ID}, es.calls()[3].Get("idempotency-key"), "a batch of one keeps its message's key")

	res, err = s.DeliverBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, res.Body, "an empty batch calls nothing")
	assert.Len(t, es.calls(), 4)
}

func TestSinkBatchBidi(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: talkMethod})
	res, err := s.DeliverBatch(context.Background(), []*envelope.Envelope{msg(`{"text":"a"}`), msg(`{"text":"b","count":1}`), msg(`{"text":"c"}`)})
	require.NoError(t, err, "resolved through reflection")
	assert.JSONEq(t, `[{"text":"A","count":1},{"text":"B","count":2},{"text":"C","count":1}]`, string(res.Body))
	require.Len(t, es.talked(), 1, "one call")
	assert.Equal(t, []string{"a", "b", "c"}, texts(es.talked()[0]))
}

// A method that takes one request per call hands a batch back, and the
// relay delivers the combined message instead.
func TestSinkBatchHandsBackOneRequestMethods(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	for _, method := range []string{sayMethod, watchMethod} {
		for _, set := range []string{echoSet, ""} {
			s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: method, DescriptorSet: set})
			_, err := s.DeliverBatch(context.Background(), []*envelope.Envelope{msg(`{}`), msg(`{}`)})
			assert.ErrorIs(t, err, plugin.ErrNoBatch, "%s %q", method, set)
		}
	}
	assert.Empty(t, es.says)
	assert.Empty(t, es.watches)

	down := newTestSink(t, config.Endpoint{URL: "grpc://" + closedAddr(t), Method: sayMethod})
	_, err := down.DeliverBatch(context.Background(), []*envelope.Envelope{msg(`{}`)})
	assert.False(t, errors.Is(err, plugin.ErrNoBatch), "a method that cannot be resolved fails the attempt")
	assert.Contains(t, plugin.Tags(err), plugin.TagConnect)
}

func TestSinkBatchMemberErrors(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: collectMethod, DescriptorSet: echoSet})
	bad := msg(`{"count":"many"}`)
	_, err := s.DeliverBatch(context.Background(), []*envelope.Envelope{msg(`{}`), bad, msg(`{}`)})
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.ErrorContains(t, err, "message 2 of 3 ("+bad.ID+"): the message does not fit hikyaku.test.echo.v1.SayRequest")
	var pe *plugin.Error
	assert.ErrorAs(t, err, &pe, "a *plugin.Error")

	tmpl := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: collectMethod, DescriptorSet: echoSet,
		Headers: map[string]string{"x-a": `{{ b64dec .json.v }}`}})
	_, err = tmpl.DeliverBatch(context.Background(), []*envelope.Envelope{msg(`{"v":"YQ=="}`), msg(`{"v":"%%%"}`)})
	assert.Equal(t, []string{"template"}, plugin.Tags(err), "the tags of a member's failure")
	assert.Empty(t, es.collected(), "nothing was sent")
}
