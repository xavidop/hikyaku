package grpc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/protocols/grpc/internal/echopb"
	"github.com/xavidop/hikyaku/protocols/grpc/relaypb"
)

func newTestListener(t *testing.T, c config.Source) *Listener {
	t.Helper()
	c.Mode = modeListen
	if c.Listen == "" {
		c.Listen = "127.0.0.1:0"
	}
	cfg := sourceConfig(c)
	l, err := newListener(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: cfg}, cfg)
	require.NoError(t, err)
	l.grace = 300 * time.Millisecond
	return l
}

// listening starts l on out and returns its address and a stop.
func listening(t *testing.T, l *Listener, out chan *envelope.Envelope) (string, func()) {
	t.Helper()
	stop := start(t, l, out)
	require.Eventually(t, func() bool { return l.Addr() != "" }, 3*time.Second, 5*time.Millisecond)
	return l.Addr(), stop
}

// echoListener is a listen source serving the Echo service of the test
// descriptor set.
func echoListener(t *testing.T, c config.Source) *Listener {
	t.Helper()
	c.DescriptorSet = echoSet
	c.Services = []string{echoService}
	return newTestListener(t, c)
}

func TestListenStripsCredentials(t *testing.T) {
	l := newTestListener(t, config.Source{Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"t1"}}}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	seen := fakeEngine(t, out, nil)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer t1", "cookie", "a=b", "x-request-id", "r1")
	_, err := relaypb.NewRelayClient(dial(t, addr)).Send(ctx, &relaypb.Message{Body: []byte(`{}`)})
	require.NoError(t, err)
	env := recv(t, seen)
	assert.Empty(t, env.Header("Authorization"), "the caller's credentials never reach a destination")
	assert.Empty(t, env.Header("Cookie"))
	assert.Equal(t, "r1", env.Header("X-Request-Id"))
}

func TestRelaySendDelivered(t *testing.T) {
	l := newTestListener(t, config.Source{})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	seen := fakeEngine(t, out, nil)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"x-request-id", "r1", "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	reply, err := relaypb.NewRelayClient(dial(t, addr)).Send(ctx, &relaypb.Message{
		Id: "evt_1", Subject: "orders.created", Headers: map[string]string{"x-tenant": "acme"}, Body: []byte(`{"id":1}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "evt_1", reply.GetId())
	assert.Equal(t, "delivered", reply.GetStatus())

	env := recv(t, seen)
	assert.Equal(t, "evt_1", env.ID)
	assert.Equal(t, "evt_1", env.Attributes["idempotency_key"])
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, "grpc", env.Protocol)
	assert.Equal(t, "orders.created", env.Subject)
	assert.Equal(t, contentJSON, env.ContentType, "a JSON body without content_type")
	assert.JSONEq(t, `{"id":1}`, env.BodyText())
	assert.Equal(t, "acme", env.Header("X-Tenant"), "the message's headers")
	assert.Equal(t, "r1", env.Header("X-Request-Id"), "the call's metadata")
	assert.Empty(t, env.Header("Content-Type"), "application/grpc is the call's, not the message's")
	assert.Equal(t, relaypb.Relay_Send_FullMethodName, env.Attributes["grpc.method"])
	assert.Contains(t, env.Attributes["grpc.peer"], "127.0.0.1:")
	assert.Equal(t, env.Attributes["grpc.peer"], env.Attributes["remote_addr"])
	assert.True(t, env.Trace.IsValid(), "the caller's trace context")
	assert.Equal(t, "0af7651916cd43dd8448eb211c80319c", env.Trace.TraceID().String())
}

func TestRelaySendDefaults(t *testing.T) {
	l := newTestListener(t, config.Source{})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	seen := fakeEngine(t, out, nil)
	client := relaypb.NewRelayClient(dial(t, addr))

	ctx := metadata.AppendToOutgoingContext(context.Background(), "idempotency-key", "key-7")
	reply, err := client.Send(ctx, &relaypb.Message{Body: []byte("hello")})
	require.NoError(t, err)
	env := recv(t, seen)
	assert.Equal(t, "key-7", reply.GetId(), "the idempotency-key metadata names the message")
	assert.Equal(t, "key-7", env.ID)
	assert.Equal(t, relaypb.Relay_Send_FullMethodName, env.Subject, "no subject: the method")
	assert.Equal(t, contentText, env.ContentType)

	reply, err = client.Send(context.Background(), &relaypb.Message{Body: []byte{0xff, 0x00, 0xfe}, ContentType: ""})
	require.NoError(t, err)
	env = recv(t, seen)
	assert.Equal(t, env.ID, reply.GetId())
	assert.Len(t, env.ID, 26, "a ULID")
	assert.Equal(t, contentBinary, env.ContentType)

	_, err = client.Send(context.Background(), &relaypb.Message{Body: []byte("x"), ContentType: "application/xml"})
	require.NoError(t, err)
	assert.Equal(t, "application/xml", recv(t, seen).ContentType)

	_, err = client.Send(context.Background(), &relaypb.Message{Id: strings.Repeat("x", 257)})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRelaySendOutcomes(t *testing.T) {
	cases := []struct {
		outcome envelope.AckStatus
		code    codes.Code
		status  string
	}{
		{envelope.Delivered, codes.OK, "delivered"},
		{envelope.DeadLettered, codes.OK, "dead_lettered"},
		{envelope.Unrouted, codes.OK, "unrouted"},
		{envelope.Filtered, codes.OK, "filtered"},
		{envelope.Failed, codes.Unavailable, ""},
	}
	l := newTestListener(t, config.Source{})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	var mu sync.Mutex
	next := envelope.Delivered
	fakeEngine(t, out, func(*envelope.Envelope) envelope.Outcome {
		mu.Lock()
		defer mu.Unlock()
		return envelope.Outcome{Status: next, Err: errors.New("boom")}
	})
	client := relaypb.NewRelayClient(dial(t, addr))
	for _, c := range cases {
		t.Run(c.outcome.String(), func(t *testing.T) {
			mu.Lock()
			next = c.outcome
			mu.Unlock()
			reply, err := client.Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
			require.Equal(t, c.code, status.Code(err), "%v", err)
			if c.code == codes.OK {
				assert.Equal(t, c.status, reply.GetStatus())
			}
		})
	}
}

func TestRelaySendAccepted(t *testing.T) {
	l := newTestListener(t, config.Source{Ack: "accepted"})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	held := make(chan *envelope.Envelope, 1)
	go func() {
		env := <-out
		env.Queued() // never acked: accepted answers without the outcome
		held <- env
	}()
	reply, err := relaypb.NewRelayClient(dial(t, addr)).Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
	require.NoError(t, err)
	assert.Equal(t, "accepted", reply.GetStatus())
	env := <-held
	assert.Equal(t, env.ID, reply.GetId())
}

func TestRelaySendWaitRunsOut(t *testing.T) {
	for _, respond := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "respond"}[respond], func(t *testing.T) {
			c := config.Source{}
			if respond {
				c.Respond = &config.Respond{From: "api", Timeout: config.Duration(150 * time.Millisecond)}
			}
			l := newTestListener(t, c)
			l.opts.wait = 150 * time.Millisecond
			out := make(chan *envelope.Envelope, 8)
			addr, _ := listening(t, l, out)
			reply, err := relaypb.NewRelayClient(dial(t, addr)).Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
			if respond {
				assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "pending", reply.GetStatus(), "the relay still has the message")
		})
	}
}

func TestBufferFullIsResourceExhausted(t *testing.T) {
	l := echoListener(t, config.Source{})
	out := make(chan *envelope.Envelope) // nobody reads: the buffer is full
	addr, _ := listening(t, l, out)
	cc := dial(t, addr)

	var trailer metadata.MD
	_, err := relaypb.NewRelayClient(cc).Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)}, grpc.Trailer(&trailer))
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Equal(t, []string{"1"}, trailer.Get("retry-after"))

	_, err = echopb.NewEchoClient(cc).Say(context.Background(), &echopb.SayRequest{Text: "hi"})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "described methods push back too")
}

func TestUnaryMethodMapsTheReply(t *testing.T) {
	l := echoListener(t, config.Source{Respond: &config.Respond{From: "api"}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	seen := fakeEngine(t, out, func(env *envelope.Envelope) envelope.Outcome {
		return envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
			"api":   {Status: 200, Body: []byte(`{"text":"HELLO","count":3,"unknown":"ignored"}`), ContentType: contentJSON, Headers: map[string][]string{"X-Served-By": {"api-1"}, "Set-Cookie": {"s=1"}}},
			"other": {Body: []byte(`{"text":"not this one"}`)},
		}}
	})
	at := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "idempotency-key", "say-1", "x-tenant", "acme", "trace-bin", "\x01\x02")
	var header metadata.MD
	reply, err := echopb.NewEchoClient(dial(t, addr)).Say(ctx, &echopb.SayRequest{Text: "hello", Count: 2, At: timestamppb.New(at)}, grpc.Header(&header))
	require.NoError(t, err)
	assert.Equal(t, "HELLO", reply.GetText(), "respond.from's reply, in the output type")
	assert.Equal(t, int32(3), reply.GetCount())
	assert.Equal(t, []string{"say-1"}, header.Get(headerID))
	assert.Equal(t, []string{"delivered"}, header.Get(headerStatus))
	assert.Equal(t, []string{"api-1"}, header.Get("x-served-by"), "the reply's headers")
	assert.Empty(t, header.Get("set-cookie"))

	env := recv(t, seen)
	assert.Equal(t, "say-1", env.ID)
	assert.Equal(t, sayMethod, env.Subject)
	assert.Equal(t, sayMethod, env.Attributes["grpc.method"])
	assert.Equal(t, contentJSON, env.ContentType)
	assert.JSONEq(t, `{"text":"hello","count":2,"at":"2026-11-09T12:00:00Z"}`, env.BodyText())
	assert.Equal(t, "acme", env.Header("X-Tenant"))
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{1, 2}), env.Header("Trace-Bin"), "binary metadata, base64")
}

func TestUnaryMethodWithoutRespond(t *testing.T) {
	l := echoListener(t, config.Source{})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	var mu sync.Mutex
	next := envelope.Delivered
	fakeEngine(t, out, func(*envelope.Envelope) envelope.Outcome {
		mu.Lock()
		defer mu.Unlock()
		return envelope.Outcome{Status: next}
	})
	client := echopb.NewEchoClient(dial(t, addr))
	for _, st := range []envelope.AckStatus{envelope.Delivered, envelope.DeadLettered, envelope.Unrouted} {
		mu.Lock()
		next = st
		mu.Unlock()
		var header metadata.MD
		reply, err := client.Say(context.Background(), &echopb.SayRequest{Text: "x"}, grpc.Header(&header))
		require.NoError(t, err, st.String())
		assert.True(t, proto.Equal(&echopb.SayReply{}, reply), "an empty output")
		assert.Equal(t, []string{st.String()}, header.Get(headerStatus))
	}
	mu.Lock()
	next = envelope.Failed
	mu.Unlock()
	_, err := client.Say(context.Background(), &echopb.SayRequest{Text: "x"})
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestUnaryMethodRespondFailures(t *testing.T) {
	cases := []struct {
		name    string
		outcome envelope.Outcome
		code    codes.Code
	}{
		{"dead-lettered without a reply", envelope.Outcome{Status: envelope.DeadLettered}, codes.Unavailable},
		{"routed elsewhere", envelope.Outcome{Status: envelope.Delivered}, codes.OK},
		{"a reply that is not JSON", envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{"api": {Body: []byte("<html>")}}}, codes.Internal},
		{"a reply of another shape", envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{"api": {Body: []byte(`{"count":"many"}`)}}}, codes.Internal},
		{"a binary protobuf reply", envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{"api": {Body: mustMarshal(t, &echopb.SayReply{Text: "BIN"}), ContentType: "application/x-protobuf"}}}, codes.OK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := echoListener(t, config.Source{Respond: &config.Respond{From: "api"}})
			out := make(chan *envelope.Envelope, 8)
			addr, _ := listening(t, l, out)
			fakeEngine(t, out, func(*envelope.Envelope) envelope.Outcome { return c.outcome })
			reply, err := echopb.NewEchoClient(dial(t, addr)).Say(context.Background(), &echopb.SayRequest{Text: "x"})
			assert.Equal(t, c.code, status.Code(err), "%v", err)
			if c.name == "a binary protobuf reply" {
				assert.Equal(t, "BIN", reply.GetText())
			}
		})
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	require.NoError(t, err)
	return b
}

func TestClientStreamingMethod(t *testing.T) {
	l := echoListener(t, config.Source{Respond: &config.Respond{From: "api"}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	var mu sync.Mutex
	acked := 0
	seen := fakeEngine(t, out, func(env *envelope.Envelope) envelope.Outcome {
		mu.Lock()
		defer mu.Unlock()
		acked++
		var in struct{ Text string }
		_ = json.Unmarshal(env.Body, &in)
		return envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
			"api": {Body: []byte(`{"received":` + itoa(acked) + `,"last":"` + in.Text + `"}`)},
		}}
	})
	ctx := metadata.AppendToOutgoingContext(context.Background(), "idempotency-key", "batch-9")
	stream, err := echopb.NewEchoClient(dial(t, addr)).Collect(ctx)
	require.NoError(t, err)
	for _, text := range []string{"a", "b", "c"} {
		require.NoError(t, stream.Send(&echopb.SayRequest{Text: text}))
	}
	reply, err := stream.CloseAndRecv()
	require.NoError(t, err)
	assert.Equal(t, int32(3), reply.GetReceived(), "answered once all three settled, with the last one's reply")
	assert.Equal(t, "c", reply.GetLast())

	for i, text := range []string{"a", "b", "c"} {
		env := recv(t, seen)
		assert.Equal(t, collectMethod, env.Subject)
		assert.Equal(t, "batch-9-"+itoa(i), env.ID, "the stream's key, numbered")
		assert.Equal(t, i, env.Attributes["grpc.message_index"])
		assert.JSONEq(t, `{"text":"`+text+`","count":0,"at":null}`, env.BodyText())
	}
}

func TestClientStreamingFailures(t *testing.T) {
	l := echoListener(t, config.Source{})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	fakeEngine(t, out, func(env *envelope.Envelope) envelope.Outcome {
		if strings.Contains(env.BodyText(), "bad") {
			return envelope.Outcome{Status: envelope.Failed}
		}
		return envelope.Outcome{Status: envelope.Delivered}
	})
	client := echopb.NewEchoClient(dial(t, addr))

	stream, err := client.Collect(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echopb.SayRequest{Text: "good"}))
	require.NoError(t, stream.Send(&echopb.SayRequest{Text: "bad"}))
	_, err = stream.CloseAndRecv()
	assert.Equal(t, codes.Unavailable, status.Code(err), "one failed message fails the call")

	stream, err = client.Collect(context.Background())
	require.NoError(t, err)
	reply, err := stream.CloseAndRecv()
	require.NoError(t, err, "an empty stream relays nothing")
	assert.True(t, proto.Equal(&echopb.CollectReply{}, reply))
}

func itoa(i int) string { return big.NewInt(int64(i)).String() }

func TestServedServices(t *testing.T) {
	served := func(services []string) ([]string, error) {
		cfg := sourceConfig(config.Source{Mode: modeListen, Listen: "127.0.0.1:0", DescriptorSet: echoSet, Services: services})
		l, err := newListener(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: cfg}, cfg)
		if err != nil {
			return nil, err
		}
		var names []string
		for _, sd := range l.services {
			names = append(names, string(sd.FullName()))
		}
		return names, nil
	}
	names, err := served(nil)
	require.NoError(t, err, "methods of every kind are served")
	assert.Equal(t, []string{chatService, echoService, feedService}, names)
	names, err = served([]string{feedService, "." + chatService, feedService})
	require.NoError(t, err)
	assert.Equal(t, []string{feedService, chatService}, names, "the services named, once each")

	for _, c := range []struct {
		services []string
		want     string
	}{
		{[]string{"hikyaku.test.echo.v1.Nope"}, "services: service hikyaku.test.echo.v1.Nope is not described"},
		{[]string{"hikyaku.relay.v1.Relay"}, "services: hikyaku.relay.v1.Relay is served by every grpc listen source"},
	} {
		_, err := served(c.services)
		require.Error(t, err)
		assert.Contains(t, err.Error(), c.want)
	}
}

func TestListenServesHealthAndReflection(t *testing.T) {
	l := echoListener(t, config.Source{})
	out := make(chan *envelope.Envelope, 8)
	addr, stop := listening(t, l, out)
	cc := dial(t, addr)

	hc := healthgrpc.NewHealthClient(cc)
	for _, svc := range []string{"", "hikyaku.relay.v1.Relay", echoService} {
		resp, err := hc.Check(context.Background(), &healthgrpc.HealthCheckRequest{Service: svc})
		require.NoError(t, err, svc)
		assert.Equal(t, healthgrpc.HealthCheckResponse_SERVING, resp.GetStatus(), svc)
	}

	reg, err := describe(context.Background(), cc, echoService)
	require.NoError(t, err, "the descriptor set, through reflection")
	md, err := reg.method(sayMethod)
	require.NoError(t, err)
	assert.Equal(t, "hikyaku.test.echo.v1.SayReply", string(md.Output().FullName()))
	_, err = describe(context.Background(), cc, "hikyaku.relay.v1.Relay")
	require.NoError(t, err, "the relay service, from the linked files")

	require.NoError(t, l.Health(context.Background()))
	stop()
	assert.ErrorIs(t, l.Health(context.Background()), errStopped)
}

func TestReflectionOff(t *testing.T) {
	l := newTestListener(t, config.Source{Reflection: config.Ptr(false)})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	_, err := describe(context.Background(), dial(t, addr), "hikyaku.relay.v1.Relay")
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestListenBearerAuth(t *testing.T) {
	l := newTestListener(t, config.Source{Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"s3cret"}}}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	fakeEngine(t, out, nil)
	cc := dial(t, addr)
	client := relaypb.NewRelayClient(cc)

	_, err := client.Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
	assert.Equal(t, codes.Unauthenticated, status.Code(err), "no token")
	wrong := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer nope")
	_, err = client.Send(wrong, &relaypb.Message{Body: []byte(`{}`)})
	assert.Equal(t, codes.Unauthenticated, status.Code(err), "a wrong token")
	right := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer s3cret")
	_, err = client.Send(right, &relaypb.Message{Body: []byte(`{}`)})
	assert.NoError(t, err)

	_, err = describe(context.Background(), cc, "hikyaku.relay.v1.Relay")
	assert.Equal(t, codes.Unauthenticated, status.Code(err), "reflection needs the token too")
	_, err = describe(right, cc, "hikyaku.relay.v1.Relay")
	assert.NoError(t, err)
	resp, err := healthgrpc.NewHealthClient(cc).Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	require.NoError(t, err, "health checks are open")
	assert.Equal(t, healthgrpc.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestListenIPAllow(t *testing.T) {
	for _, c := range []struct {
		allow string
		code  codes.Code
	}{{"10.0.0.0/8", codes.Unauthenticated}, {"127.0.0.1", codes.OK}} {
		l := newTestListener(t, config.Source{Auth: &config.Auth{IPAllow: []string{c.allow}}})
		out := make(chan *envelope.Envelope, 8)
		addr, _ := listening(t, l, out)
		fakeEngine(t, out, nil)
		_, err := relaypb.NewRelayClient(dial(t, addr)).Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
		assert.Equal(t, c.code, status.Code(err), c.allow)
	}
}

func TestListenJWTAuth(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwks := map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer srv.Close()
	sign := func(iss string) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": iss, "aud": "hikyaku", "exp": time.Now().Add(time.Hour).Unix()})
		tok.Header["kid"] = "k1"
		s, err := tok.SignedString(key)
		require.NoError(t, err)
		return s
	}
	l := newTestListener(t, config.Source{Auth: &config.Auth{JWT: &config.JWTAuth{JWKSURL: srv.URL, Issuer: "https://issuer", Audience: "hikyaku"}}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	fakeEngine(t, out, nil)
	client := relaypb.NewRelayClient(dial(t, addr))
	call := func(token string) error {
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
		_, err := client.Send(ctx, &relaypb.Message{Body: []byte(`{}`)})
		return err
	}
	good := sign("https://issuer")
	require.Eventually(t, func() bool { return call(good) == nil }, 3*time.Second, 20*time.Millisecond, "a JWT signed by the JWKS")
	assert.Equal(t, codes.Unauthenticated, status.Code(call(sign("https://evil"))))
}

func TestListenTLSAndMTLS(t *testing.T) {
	serverCert, serverKey := selfSigned(t, "server")
	clientCert, clientKey := selfSigned(t, "client")

	l := newTestListener(t, config.Source{TLS: &config.TLS{CertFile: serverCert, KeyFile: serverKey}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	fakeEngine(t, out, nil)
	secure := dial(t, addr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS(t, serverCert, "", ""))))
	_, err := relaypb.NewRelayClient(secure).Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
	require.NoError(t, err)
	_, err = relaypb.NewRelayClient(dial(t, addr)).Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
	assert.Equal(t, codes.Unavailable, status.Code(err), "plaintext to a TLS listener")

	ml := newTestListener(t, config.Source{TLS: &config.TLS{CertFile: serverCert, KeyFile: serverKey, ClientCAFile: clientCert}})
	mout := make(chan *envelope.Envelope, 8)
	maddr, _ := listening(t, ml, mout)
	fakeEngine(t, mout, nil)
	anonymous := dial(t, maddr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS(t, serverCert, "", ""))))
	_, err = relaypb.NewRelayClient(anonymous).Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
	assert.Error(t, err, "mTLS needs a client certificate")
	known := dial(t, maddr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS(t, serverCert, clientCert, clientKey))))
	_, err = relaypb.NewRelayClient(known).Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
	assert.NoError(t, err)
}

func TestListenMaxBodyBytes(t *testing.T) {
	l := newTestListener(t, config.Source{MaxBodyBytes: 1024})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	fakeEngine(t, out, nil)
	client := relaypb.NewRelayClient(dial(t, addr))
	_, err := client.Send(context.Background(), &relaypb.Message{Body: make([]byte, 2048)})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "larger than max_body_bytes")
	_, err = client.Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
	assert.NoError(t, err, "the source carries on")
}

func TestStopAnswersWaitingCalls(t *testing.T) {
	l := newTestListener(t, config.Source{})
	out := make(chan *envelope.Envelope, 8)
	addr, stop := listening(t, l, out)
	client := relaypb.NewRelayClient(dial(t, addr))
	replies := make(chan *relaypb.SendReply, 1)
	go func() {
		r, err := client.Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
		assert.NoError(t, err)
		replies <- r
	}()
	env := recv(t, out) // taken, never acked: the call waits
	began := time.Now()
	stop()
	select {
	case r := <-replies:
		assert.Equal(t, "pending", r.GetStatus(), "the relay still has it")
		assert.Equal(t, env.ID, r.GetId())
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting call was not answered")
	}
	assert.Less(t, time.Since(began), 3*time.Second)

	_, err := client.Send(context.Background(), &relaypb.Message{Body: []byte(`{}`)})
	assert.Error(t, err, "a stopped source takes nothing")
}

func TestListenBindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	l := newTestListener(t, config.Source{Listen: ln.Addr().String()})
	assert.ErrorIs(t, l.Health(context.Background()), errNotStarted)
	err = l.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listen "+ln.Addr().String())
	assert.Error(t, l.Health(context.Background()))
}

// A hot reload starts the new source on the port the old one holds: the
// new one takes the new calls, and the port stays open when the old one
// stops.
func TestReloadHandsThePortOver(t *testing.T) {
	old := newTestListener(t, config.Source{})
	oldOut := make(chan *envelope.Envelope, 8)
	addr, stopOld := listening(t, old, oldOut)
	oldSeen := fakeEngine(t, oldOut, nil)

	_, err := relaypb.NewRelayClient(dial(t, addr)).Send(context.Background(), &relaypb.Message{Body: []byte(`"old"`)})
	require.NoError(t, err)
	assert.Equal(t, `"old"`, recv(t, oldSeen).BodyText())

	fresh := newTestListener(t, config.Source{Listen: addr})
	freshOut := make(chan *envelope.Envelope, 8)
	freshAddr, stopFresh := listening(t, fresh, freshOut)
	assert.Equal(t, addr, freshAddr)
	freshSeen := fakeEngine(t, freshOut, nil)
	stopOld()

	_, err = relaypb.NewRelayClient(dial(t, addr)).Send(context.Background(), &relaypb.Message{Body: []byte(`"new"`)})
	require.NoError(t, err)
	assert.Equal(t, `"new"`, recv(t, freshSeen).BodyText())

	stopFresh()
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err, "the port closes with its last source")
	_ = ln.Close()
}

func TestListenConfigErrors(t *testing.T) {
	cases := []struct {
		name string
		cfg  config.Source
		want string
	}{
		{"hmac", config.Source{Auth: &config.Auth{HMAC: &config.HMACAuth{Header: "X-Sig", Secret: "s"}}}, "auth.hmac applies to http sources"},
		{"bad listen", config.Source{Listen: "nope"}, `listen "nope" is not host:port`},
		{"path", config.Source{Path: "/x"}, "path does not apply"},
		{"negative max", config.Source{MaxBodyBytes: -1}, "max_body_bytes must not be negative"},
		{"ack", config.Source{Ack: "later"}, "ack must be delivered or accepted"},
		{"respond accepted", config.Source{Ack: "accepted", Respond: &config.Respond{From: "x"}}, "respond needs ack: delivered"},
		{"respond from", config.Source{Respond: &config.Respond{}}, "respond.from is required"},
		{"services alone", config.Source{Services: []string{"a.B"}}, "services needs descriptor_set"},
		{"missing set", config.Source{DescriptorSet: "testdata/nope.pb"}, "descriptor_set: open testdata/nope.pb"},
		{"not a set", config.Source{DescriptorSet: "testdata/echo.proto"}, "is not a FileDescriptorSet"},
		{"tls without key", config.Source{TLS: &config.TLS{CertFile: "x.pem"}}, "a listener needs cert_file and key_file"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.cfg.Mode = modeListen
			if c.cfg.Listen == "" {
				c.cfg.Listen = "127.0.0.1:0"
			}
			cfg := sourceConfig(c.cfg)
			_, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: cfg})
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.True(t, strings.HasPrefix(err.Error(), "source in: "), err.Error())
		})
	}
}
