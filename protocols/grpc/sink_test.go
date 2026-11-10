package grpc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/protocols/grpc/internal/echopb"
	"github.com/xavidop/hikyaku/protocols/grpc/relaypb"
)

func newTestSink(t *testing.T, ep config.Endpoint) *Sink {
	t.Helper()
	ep.Type = kind
	s, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: kind, Config: &ep})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func TestSinkDelivers(t *testing.T) {
	for _, via := range []string{"descriptor_set", "reflection"} {
		t.Run(via, func(t *testing.T) {
			es := &echoServer{header: metadata.Pairs("x-served-by", "echo-1")}
			srv := startEcho(t, es, serverOpts{})
			ep := config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod}
			if via == "descriptor_set" {
				ep.DescriptorSet = echoSet
			}
			s := newTestSink(t, ep)
			assert.Equal(t, "out", s.Name())
			res, err := s.Deliver(context.Background(), msg(`{"text":"hi","count":1,"at":"2026-11-09T12:00:00Z","extra":true}`))
			require.NoError(t, err)
			assert.JSONEq(t, `{"text":"HI","count":2}`, string(res.Body), "the response, as JSON")
			assert.Equal(t, contentJSON, res.ContentType)
			assert.Equal(t, 0, res.Status)
			assert.Equal(t, "grpc://"+srv.addr, res.Endpoint)
			assert.Equal(t, []string{contentJSON}, res.Headers["Content-Type"])
			assert.Equal(t, []string{"echo-1"}, res.Headers["X-Served-By"], "the response's header metadata")
			got := es.lastSay()
			require.NotNil(t, got)
			assert.Equal(t, "hi", got.GetText())
			assert.Equal(t, int32(1), got.GetCount())
			assert.Equal(t, time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC).Unix(), got.GetAt().GetSeconds(), "well-known types")
		})
	}
}

func TestSinkMetadata(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet,
		Headers: map[string]string{"Authorization": "Bearer {{ .json.token }}", "x-subject": "{{ .subject }}", "x-static": "yes"}})
	env := msg(`{"text":"hi","token":"t0k"}`)
	env.Subject = "orders"
	env.Headers = map[string][]string{
		"X-Tenant": {"acme"}, "Authorization": {"Bearer caller"}, "Cookie": {"c=1"}, "Te": {"trailers"},
		"Content-Length": {"9"}, "X-Weird Key": {"dropped"}, "Trace-Bin": {"AQI="}, "X-Line": {"a\nb"},
	}
	_, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	md := es.calls()[0]
	assert.Equal(t, []string{env.ID}, md.Get("idempotency-key"), "the message ID")
	assert.Equal(t, []string{"Bearer t0k"}, md.Get("authorization"), "the configured header, not the caller's")
	assert.Equal(t, []string{"orders"}, md.Get("x-subject"))
	assert.Equal(t, []string{"yes"}, md.Get("x-static"))
	assert.Equal(t, []string{"acme"}, md.Get("x-tenant"), "the message's headers are passed on")
	assert.Equal(t, []string{"\x01\x02"}, md.Get("trace-bin"), "binary headers, decoded")
	assert.Empty(t, md.Get("cookie"))
	assert.Empty(t, md.Get("content-length"))
	assert.Empty(t, md.Get("x-line"), "a value gRPC would refuse")
	assert.Contains(t, md.Get("user-agent")[0], userAgent)

	keyed := msg(`{"text":"again"}`)
	keyed.Attributes["idempotency_key"] = "caller-key"
	_, err = s.Deliver(context.Background(), keyed)
	require.NoError(t, err)
	assert.Equal(t, []string{"caller-key"}, es.calls()[1].Get("idempotency-key"), "the caller's idempotency key wins")
}

func TestSinkBodies(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	tmpl := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet,
		Body: `{"text": "{{ .json.message }}", "count": 7}`})
	_, err := tmpl.Deliver(context.Background(), msg(`{"message":"from a template"}`))
	require.NoError(t, err)
	assert.Equal(t, "from a template", es.lastSay().GetText())
	assert.Equal(t, int32(7), es.lastSay().GetCount())

	plain := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet})
	bin := msg("")
	bin.Body = mustMarshal(t, &echopb.SayRequest{Text: "binary"})
	bin.ContentType = "application/x-protobuf"
	_, err = plain.Deliver(context.Background(), bin)
	require.NoError(t, err)
	assert.Equal(t, "binary", es.lastSay().GetText(), "a protobuf body is sent as it is")

	_, err = plain.Deliver(context.Background(), msg(""))
	require.NoError(t, err, "an empty body is an empty request")

	for _, body := range []string{"not json", `{"count":"many"}`} {
		_, err := plain.Deliver(context.Background(), msg(body))
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, body)
		assert.ErrorContains(t, err, "the message does not fit hikyaku.test.echo.v1.SayRequest")
	}
	for _, ep := range []config.Endpoint{
		{Headers: map[string]string{"x-a": `{{ b64dec "%%%" }}`}},
		{Body: `{{ b64dec "%%%" }}`},
	} {
		ep.URL, ep.Method, ep.DescriptorSet = "grpc://"+srv.addr, sayMethod, echoSet
		_, err = newTestSink(t, ep).Deliver(context.Background(), msg(`{"text":"x"}`))
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, "a template that fails to render")
		assert.Equal(t, []string{"template"}, plugin.Tags(err))
	}
}

func TestSinkErrorClasses(t *testing.T) {
	cases := []struct {
		code  codes.Code
		class plugin.Class
		tags  []string
	}{
		{codes.Unavailable, plugin.Transient, []string{"grpc:UNAVAILABLE", "5xx"}},
		{codes.Aborted, plugin.Transient, []string{"grpc:ABORTED", "5xx"}},
		{codes.Internal, plugin.Transient, []string{"grpc:INTERNAL", "5xx"}},
		{codes.Unknown, plugin.Transient, []string{"grpc:UNKNOWN", "5xx"}},
		{codes.ResourceExhausted, plugin.RateLimited, []string{"grpc:RESOURCE_EXHAUSTED", "429"}},
		{codes.DeadlineExceeded, plugin.Timeout, []string{"grpc:DEADLINE_EXCEEDED", "timeout"}},
		{codes.InvalidArgument, plugin.Permanent, []string{"grpc:INVALID_ARGUMENT", "4xx"}},
		{codes.NotFound, plugin.Permanent, []string{"grpc:NOT_FOUND", "4xx"}},
		{codes.AlreadyExists, plugin.Permanent, []string{"grpc:ALREADY_EXISTS", "4xx"}},
		{codes.PermissionDenied, plugin.Permanent, []string{"grpc:PERMISSION_DENIED", "4xx"}},
		{codes.Unauthenticated, plugin.Permanent, []string{"grpc:UNAUTHENTICATED", "4xx"}},
		{codes.FailedPrecondition, plugin.Permanent, []string{"grpc:FAILED_PRECONDITION", "4xx"}},
		{codes.OutOfRange, plugin.Permanent, []string{"grpc:OUT_OF_RANGE", "4xx"}},
		{codes.Unimplemented, plugin.Permanent, []string{"grpc:UNIMPLEMENTED", "4xx"}},
		{codes.DataLoss, plugin.Permanent, []string{"grpc:DATA_LOSS", "4xx"}},
		{codes.Canceled, plugin.Permanent, []string{"grpc:CANCELLED", "4xx"}},
	}
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet})
	for _, c := range cases {
		t.Run(c.code.String(), func(t *testing.T) {
			es.set(func(es *echoServer) { es.fail = status.Error(c.code, "nope") })
			_, err := s.Deliver(context.Background(), msg(`{"text":"x"}`))
			require.Error(t, err)
			class, _ := plugin.Classify(err)
			assert.Equal(t, c.class, class)
			assert.Equal(t, c.tags, plugin.Tags(err), "a server's answer is not a connect failure")
			assert.ErrorContains(t, err, sayMethod+": "+codeName(c.code)+": nope")
		})
	}
}

func TestSinkRetryHints(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet})
	for _, c := range []struct {
		md   metadata.MD
		want time.Duration
	}{
		{metadata.Pairs("retry-after", "2"), 2 * time.Second},
		{metadata.Pairs("grpc-retry-pushback-ms", "1500"), 1500 * time.Millisecond},
		{nil, 0},
	} {
		es.set(func(es *echoServer) { es.fail, es.trailer = status.Error(codes.ResourceExhausted, "slow down"), c.md })
		_, err := s.Deliver(context.Background(), msg(`{}`))
		class, ra := plugin.Classify(err)
		assert.Equal(t, plugin.RateLimited, class)
		assert.Equal(t, c.want, ra, "%v", c.md)
	}
}

func TestSinkConnectFailure(t *testing.T) {
	for _, via := range []string{"descriptor_set", "reflection"} {
		s := newTestSink(t, config.Endpoint{URL: "grpc://" + closedAddr(t), Method: sayMethod,
			DescriptorSet: map[string]string{"descriptor_set": echoSet}[via]})
		_, err := s.Deliver(context.Background(), msg(`{"text":"x"}`))
		require.Error(t, err, via)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Transient, class, via)
		assert.Contains(t, plugin.Tags(err), plugin.TagConnect, via)
		assert.Contains(t, plugin.Tags(err), "grpc:UNAVAILABLE", via)
	}
}

func TestSinkTimeout(t *testing.T) {
	es := &echoServer{delay: time.Second}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := s.Deliver(ctx, msg(`{"text":"x"}`))
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)
	assert.Equal(t, []string{"grpc:DEADLINE_EXCEEDED", "timeout"}, plugin.Tags(err))
}

func TestSinkResolvesThroughReflection(t *testing.T) {
	es := &echoServer{}
	bare := startEcho(t, es, serverOpts{noReflection: true})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + bare.addr, Method: sayMethod})
	_, err := s.Deliver(context.Background(), msg(`{"text":"x"}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "a server without reflection")
	assert.Contains(t, plugin.Tags(err), "grpc:UNIMPLEMENTED")
	assert.ErrorContains(t, err, "set descriptor_set")

	srv := startEcho(t, es, serverOpts{})
	streaming := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: watchMethod})
	res, err := streaming.Deliver(context.Background(), msg(`{"topic":"t","limit":2}`))
	require.NoError(t, err, "a streaming method, found through reflection")
	assert.JSONEq(t, `[{"topic":"t","seq":"1","data":""},{"topic":"t","seq":"2","data":""}]`, string(res.Body))

	missing := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: "/hikyaku.test.echo.v1.Echo/Nope"})
	_, err = missing.Deliver(context.Background(), msg(`{}`))
	assert.ErrorContains(t, err, "has no method Nope")

	ok := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod})
	assert.Nil(t, ok.md, "resolved on first use: the server may be down when the relay starts")
	_, err = ok.Deliver(context.Background(), msg(`{"text":"x"}`))
	require.NoError(t, err)
	resolved := ok.md
	require.NotNil(t, resolved)
	_, err = ok.Deliver(context.Background(), msg(`{"text":"y"}`))
	require.NoError(t, err)
	assert.Same(t, resolved, ok.md, "and kept")
}

func TestSinkReroute(t *testing.T) {
	es := &echoServer{header: metadata.Pairs(rerouteKey, "billing, support")}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet})
	res, err := s.Deliver(context.Background(), msg(`{}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing", "support"}, res.Reroute)
}

func TestSinkHealth(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet})
	require.NoError(t, s.Health(context.Background()), "the server is SERVING")

	srv.hs.SetServingStatus(echoService, healthgrpc.HealthCheckResponse_NOT_SERVING)
	err := s.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NOT_SERVING", "the method's service first")
	srv.hs.SetServingStatus(echoService, healthgrpc.HealthCheckResponse_SERVING)
	require.NoError(t, s.Health(context.Background()))

	bare := startEcho(t, es, serverOpts{noHealth: true})
	b := newTestSink(t, config.Endpoint{URL: "grpc://" + bare.addr, Method: sayMethod, DescriptorSet: echoSet})
	assert.NoError(t, b.Health(context.Background()), "no health service: connected is healthy")

	down := newTestSink(t, config.Endpoint{URL: "grpc://" + closedAddr(t), Method: sayMethod, DescriptorSet: echoSet})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.Error(t, down.Health(ctx))
}

func TestSinkClose(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet})
	_, err := s.Deliver(context.Background(), msg(`{}`))
	require.NoError(t, err)
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, s.Close(context.Background()), "closing twice is fine")
	_, err = s.Deliver(context.Background(), msg(`{}`))
	assert.Error(t, err)
	assert.Error(t, s.Health(context.Background()))
}

func TestSinkTLS(t *testing.T) {
	cert, key := selfSigned(t, "server")
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{creds: mustServerTLS(t, cert, key)})
	s := newTestSink(t, config.Endpoint{URL: "grpcs://" + srv.addr, Method: sayMethod, TLS: &config.TLS{CAFile: cert}})
	res, err := s.Deliver(context.Background(), msg(`{"text":"tls"}`))
	require.NoError(t, err)
	assert.JSONEq(t, `{"text":"TLS","count":1}`, string(res.Body))

	plain := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod, DescriptorSet: echoSet})
	_, err = plain.Deliver(context.Background(), msg(`{}`))
	assert.Error(t, err, "plaintext to a TLS server")
}

func TestSinkConfigErrors(t *testing.T) {
	ok := config.Endpoint{URL: "grpc://localhost:50051", Method: sayMethod}
	with := func(f func(*config.Endpoint)) config.Endpoint { c := ok; f(&c); return c }
	cases := []struct {
		name string
		ep   config.Endpoint
		want string
	}{
		{"no url", with(func(e *config.Endpoint) { e.URL = "" }), "url is required"},
		{"scheme", with(func(e *config.Endpoint) { e.URL = "https://x" }), "must start with grpc:// or grpcs://"},
		{"template", with(func(e *config.Endpoint) { e.URL = "grpc://{{ .json.h }}" }), "cannot be a per-message template"},
		{"no method", with(func(e *config.Endpoint) { e.Method = "" }), "method is required"},
		{"bad method", with(func(e *config.Endpoint) { e.Method = "/a/b/c" }), `method "/a/b/c" is not /package.Service/Method`},
		{"header key", with(func(e *config.Endpoint) { e.Headers = map[string]string{"x:y": "1"} }), "a gRPC metadata key holds only"},
		{"reserved", with(func(e *config.Endpoint) { e.Headers = map[string]string{"Content-Type": "x"} }), "content-type is set by gRPC itself"},
		{"user agent", with(func(e *config.Endpoint) { e.Headers = map[string]string{"User-Agent": "x"} }), "user-agent is set by gRPC itself"},
		{"header template", with(func(e *config.Endpoint) { e.Headers = map[string]string{"x-a": "{{ .json"} }), "template headers.x-a"},
		{"header value", with(func(e *config.Endpoint) { e.Headers = map[string]string{"x-a": "café"} }), "not printable ASCII"},
		{"bin value", with(func(e *config.Endpoint) { e.Headers = map[string]string{"x-bin": "%%%"} }), "is not base64"},
		{"body template", with(func(e *config.Endpoint) { e.Body = "{{ nope }}" }), "template body"},
		{"no reflection", with(func(e *config.Endpoint) { e.Reflection = config.Ptr(false) }), "reflection: false needs descriptor_set"},
		{"missing set", with(func(e *config.Endpoint) { e.DescriptorSet = "testdata/missing.pb" }), "descriptor_set: open testdata/missing.pb"},
		{"unknown service", with(func(e *config.Endpoint) { e.DescriptorSet = echoSet; e.Method = "/x.Y/Z" }), "service x.Y is not described"},
		{"unknown method", with(func(e *config.Endpoint) { e.DescriptorSet = echoSet; e.Method = "/hikyaku.test.echo.v1.Chat/Shout" }), "service hikyaku.test.echo.v1.Chat has no method Shout (it has Talk)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.ep.Type = kind
			_, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: kind, Config: &c.ep})
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.True(t, strings.HasPrefix(err.Error(), "destination out: "), err.Error())
		})
	}
	_, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: kind})
	assert.ErrorContains(t, err, "missing config")
}

func TestSinkSendsStaticHeadersWithReflectionAndHealth(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{token: "t0k"})
	s := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod,
		Headers: map[string]string{"Authorization": "Bearer t0k", "x-id": "{{ .id }}"}})
	_, err := s.Deliver(context.Background(), msg(`{"text":"x"}`))
	require.NoError(t, err, "reflection carries the token")
	assert.NoError(t, s.Health(context.Background()), "so does the health check")

	anonymous := newTestSink(t, config.Endpoint{URL: "grpc://" + srv.addr, Method: sayMethod})
	_, err = anonymous.Deliver(context.Background(), msg(`{"text":"x"}`))
	assert.Contains(t, plugin.Tags(err), "grpc:UNAUTHENTICATED")
	assert.Error(t, anonymous.Health(context.Background()))
}

// One relay's grpc destination feeds another relay's grpc listen source:
// Relay/Send, found through its reflection, behind a bearer token.
func TestSinkToARelay(t *testing.T) {
	l := newTestListener(t, config.Source{Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"t0k"}}}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	seen := fakeEngine(t, out, nil)

	s := newTestSink(t, config.Endpoint{URL: "grpc://" + addr, Method: relaypb.Relay_Send_FullMethodName,
		Headers: map[string]string{"authorization": "Bearer t0k"},
		Body:    `{"subject": "{{ .subject }}", "body": "{{ b64enc .bodyText }}", "contentType": "{{ .contentType }}"}`})
	env := msg(`{"order":7}`)
	env.Subject = "orders"
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.JSONEq(t, `{"id":"`+env.ID+`","status":"delivered"}`, string(res.Body), "the idempotency key named the message")
	got := recv(t, seen)
	assert.Equal(t, env.ID, got.ID)
	assert.Equal(t, "orders", got.Subject)
	assert.JSONEq(t, `{"order":7}`, got.BodyText())
	assert.Equal(t, contentJSON, got.ContentType)
}
