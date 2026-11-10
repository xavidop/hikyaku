//go:build integration

package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// grpcbinImage is the third-party server of the suite: a grpc-go server
// with unary, streaming (client, server and bidi) and error methods, and
// server reflection in its v1alpha version only (so the suite covers that
// fallback). It is published for linux/amd64 only and runs emulated
// elsewhere.
const grpcbinImage = "moul/grpcbin"

// startGRPCBin runs grpcbin and returns its plaintext URL.
func startGRPCBin(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.Run(ctx, grpcbinImage,
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("listening on :9000").WithStartupTimeout(2*time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	url, err := ctr.PortEndpoint(ctx, "9000/tcp", "grpc")
	require.NoError(t, err)
	return url
}

func TestIntegrationGRPCBin(t *testing.T) {
	url := startGRPCBin(t)

	t.Run("subscribe through v1alpha reflection", func(t *testing.T) {
		s := newTestSubscriber(t, config.Source{URL: url, Method: "/grpcbin.GRPCBin/DummyServerStream",
			Body: `{"fString": "hikyaku", "fInt32": 7, "fInt64": "9"}`}, nil)
		out := make(chan *envelope.Envelope, 32)
		start(t, s, out)
		for range 10 { // grpcbin streams ten copies of the request
			env := recv(t, out)
			var got map[string]any
			require.NoError(t, json.Unmarshal(env.Body, &got), env.BodyText())
			assert.Equal(t, "hikyaku", got["fString"])
			assert.Equal(t, float64(7), got["fInt32"])
			assert.Equal(t, "9", got["fInt64"], "int64 is a JSON string")
			assert.Equal(t, "/grpcbin.GRPCBin/DummyServerStream", env.Subject)
			assert.Equal(t, url, env.Attributes["grpc.url"])
		}
	})

	t.Run("subscribe to a greeting stream", func(t *testing.T) {
		s := newTestSubscriber(t, config.Source{URL: url, Method: "/hello.HelloService/LotsOfReplies", Body: `{"greeting": "hikyaku"}`}, nil)
		out := make(chan *envelope.Envelope, 32)
		start(t, s, out)
		assert.Contains(t, recv(t, out).BodyText(), "hikyaku")
	})

	t.Run("sink", func(t *testing.T) {
		say := newTestSink(t, config.Endpoint{URL: url, Method: "/hello.HelloService/SayHello"})
		res, err := say.Deliver(context.Background(), msg(`{"greeting": "hikyaku", "ignored": true}`))
		require.NoError(t, err)
		assert.JSONEq(t, `{"reply": "hello hikyaku"}`, string(res.Body))
		assert.Equal(t, contentJSON, res.ContentType)

		sum := newTestSink(t, config.Endpoint{URL: url, Method: "/addsvc.Add/Sum", Body: `{"a": {{ .json.x }}, "b": 40}`})
		res, err = sum.Deliver(context.Background(), msg(`{"x": 2}`))
		require.NoError(t, err)
		assert.JSONEq(t, `{"v": "42", "err": ""}`, string(res.Body))

		headers := newTestSink(t, config.Endpoint{URL: url, Method: "/grpcbin.GRPCBin/HeadersUnary", Headers: map[string]string{"x-relay": "{{ .source }}"}})
		env := msg(`{}`)
		res, err = headers.Deliver(context.Background(), env)
		require.NoError(t, err)
		assert.Contains(t, string(res.Body), env.ID, "the idempotency-key reached the server")
		assert.Contains(t, string(res.Body), `"x-relay"`)

		assert.NoError(t, say.Health(context.Background()), "no health service: connected is healthy")
	})

	t.Run("error classes", func(t *testing.T) {
		s := newTestSink(t, config.Endpoint{URL: url, Method: "/grpcbin.GRPCBin/SpecificError"})
		for _, c := range []struct {
			code  int
			class plugin.Class
			tag   string
		}{
			{14, plugin.Transient, "grpc:UNAVAILABLE"},
			{8, plugin.RateLimited, "grpc:RESOURCE_EXHAUSTED"},
			{3, plugin.Permanent, "grpc:INVALID_ARGUMENT"},
			{5, plugin.Permanent, "grpc:NOT_FOUND"},
		} {
			_, err := s.Deliver(context.Background(), msg(`{"code": `+itoa(c.code)+`, "reason": "integration"}`))
			require.Error(t, err)
			class, _ := plugin.Classify(err)
			assert.Equal(t, c.class, class, c.tag)
			assert.Contains(t, plugin.Tags(err), c.tag)
			assert.NotContains(t, plugin.Tags(err), plugin.TagConnect, "the server answered")
		}
	})

	greetings := func(names ...string) []*envelope.Envelope {
		envs := make([]*envelope.Envelope, len(names))
		for i, n := range names {
			envs[i] = msg(`{"greeting": "` + n + `"}`)
		}
		return envs
	}

	t.Run("streaming sink", func(t *testing.T) {
		ctx := context.Background()
		collect := newTestSink(t, config.Endpoint{URL: url, Method: "/hello.HelloService/LotsOfGreetings"})
		res, err := collect.Deliver(ctx, msg(`{"greeting": "hikyaku"}`))
		require.NoError(t, err, "client-streaming")
		assert.JSONEq(t, `{"reply": "hello hikyaku"}`, string(res.Body))
		res, err = collect.DeliverBatch(ctx, greetings("a", "b", "c"))
		require.NoError(t, err)
		assert.JSONEq(t, `{"reply": "hello a, b, c"}`, string(res.Body), "a batch is one call")

		replies := newTestSink(t, config.Endpoint{URL: url, Method: "/hello.HelloService/LotsOfReplies"})
		res, err = replies.Deliver(ctx, msg(`{"greeting": "hikyaku"}`))
		require.NoError(t, err, "server-streaming")
		var got []map[string]string
		require.NoError(t, json.Unmarshal(res.Body, &got))
		require.Len(t, got, 10, "grpcbin streams ten replies")
		assert.Equal(t, "hello hikyaku", got[9]["reply"])

		bidi := newTestSink(t, config.Endpoint{URL: url, Method: "/hello.HelloService/BidiHello"})
		res, err = bidi.Deliver(ctx, msg(`{"greeting": "hikyaku"}`))
		require.NoError(t, err, "bidi")
		assert.JSONEq(t, `[{"reply": "hello hikyaku"}]`, string(res.Body))
		res, err = bidi.DeliverBatch(ctx, greetings("a", "b", "c"))
		require.NoError(t, err)
		assert.JSONEq(t, `[{"reply": "hello a"}, {"reply": "hello b"}, {"reply": "hello c"}]`, string(res.Body))

		_, err = replies.DeliverBatch(ctx, greetings("a", "b"))
		assert.ErrorIs(t, err, plugin.ErrNoBatch, "one request per call: the relay combines the batch")

		// DummyClientStream answers the tenth of ten requests, and fails
		// with INTERNAL on fewer.
		dummy := newTestSink(t, config.Endpoint{URL: url, Method: "/grpcbin.GRPCBin/DummyClientStream"})
		_, err = dummy.Deliver(ctx, msg(`{"fString": "one"}`))
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Transient, class)
		assert.Contains(t, plugin.Tags(err), "grpc:INTERNAL")
		ten := make([]*envelope.Envelope, 10)
		for i := range ten {
			ten[i] = msg(fmt.Sprintf(`{"fString": "m%d"}`, i))
		}
		res, err = dummy.DeliverBatch(ctx, ten)
		require.NoError(t, err)
		var last map[string]any
		require.NoError(t, json.Unmarshal(res.Body, &last))
		assert.Equal(t, "m9", last["fString"])
	})

	t.Run("a relay in front of grpcbin", func(t *testing.T) {
		set := reflectedSet(t, url, "hello.HelloService")
		repliesAddr, chatAddr := closedAddr(t), closedAddr(t)
		spec := fmt.Sprintf(`
version: 1
name: grpcbin-proxy
sources:
  - name: replies
    type: grpc
    listen: "%[1]s"
    descriptor_set: %[3]s
    respond: { from: lots }
  - name: chat
    type: grpc
    listen: "%[2]s"
    descriptor_set: %[3]s
    respond: { from: hello }
destinations:
  - name: lots
    type: grpc
    url: %[4]s
    method: /hello.HelloService/LotsOfReplies
  - name: hello
    type: grpc
    url: %[4]s
    method: /hello.HelloService/SayHello
routes:
  - { name: replies, from: [replies], to: [lots] }
  - { name: chat, from: [chat], to: [hello] }
server: { admin: { disabled: true } }
`, repliesAddr, chatAddr, set, url)
		Register()
		l, err := config.LoadBytes(context.Background(), "proxy.yaml", []byte(spec), config.LoadOptions{})
		require.NoError(t, err)
		e, err := engine.New(context.Background(), l.Spec, engine.Options{})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.Run(ctx) }()
		defer func() {
			cancel()
			require.NoError(t, <-done)
		}()
		for _, addr := range []string{repliesAddr, chatAddr} {
			cc := dial(t, addr)
			require.Eventually(t, func() bool {
				_, err := healthgrpc.NewHealthClient(cc).Check(context.Background(), &healthgrpc.HealthCheckRequest{})
				return err == nil
			}, 10*time.Second, 50*time.Millisecond)
		}

		// The relay's own grpc destination is the client: it resolves the
		// methods through the relay's reflection.
		replies := newTestSink(t, config.Endpoint{URL: "grpc://" + repliesAddr, Method: "/hello.HelloService/LotsOfReplies"})
		res, err := replies.Deliver(context.Background(), msg(`{"greeting": "relay"}`))
		require.NoError(t, err)
		var got []map[string]string
		require.NoError(t, json.Unmarshal(res.Body, &got))
		require.Len(t, got, 10, "grpcbin's stream, one response per response")
		assert.Equal(t, "hello relay", got[0]["reply"])

		chat := newTestSink(t, config.Endpoint{URL: "grpc://" + chatAddr, Method: "/hello.HelloService/BidiHello"})
		res, err = chat.DeliverBatch(context.Background(), greetings("a", "b", "c"))
		require.NoError(t, err)
		assert.JSONEq(t, `[{"reply": "hello a"}, {"reply": "hello b"}, {"reply": "hello c"}]`, string(res.Body),
			"each request of the bidi call answered by SayHello, in order")
	})
}

// reflectedSet writes the files a server describes for service, through
// reflection, as a descriptor set, and returns its path.
func reflectedSet(t *testing.T, url, service string) string {
	t.Helper()
	reg, err := describe(context.Background(), dial(t, strings.TrimPrefix(url, "grpc://")), service)
	require.NoError(t, err)
	set := &descriptorpb.FileDescriptorSet{}
	reg.files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
		return true
	})
	b, err := proto.Marshal(set)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "grpcbin.pb")
	require.NoError(t, os.WriteFile(path, b, 0o600))
	return path
}
