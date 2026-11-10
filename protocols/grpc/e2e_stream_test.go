package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/protocols/grpc/internal/echopb"
)

// A relay that proxies gRPC streams: each request of a bidi call is
// answered with a unary destination's reply; a server-streaming call is
// answered with an upstream server stream, one response per upstream
// response; a client-streaming destination takes a batch in one call, and
// a unary one takes the batch combined into one request.
func TestEndToEndStreams(t *testing.T) {
	Register()
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	chatAddr, feedAddr := closedAddr(t), closedAddr(t)
	spec := fmt.Sprintf(`
version: 1
name: grpc-streams
sources:
  - name: chat
    type: grpc
    listen: "%[1]s"
    descriptor_set: testdata/echo.pb
    services: [hikyaku.test.echo.v1.Chat]
    respond: { from: say, timeout: 5s }
  - name: feed
    type: grpc
    listen: "%[2]s"
    descriptor_set: testdata/echo.pb
    services: [hikyaku.test.echo.v1.Feed]
    respond: { from: watch, timeout: 5s }
destinations:
  - name: say
    type: grpc
    url: grpc://%[3]s
    method: /hikyaku.test.echo.v1.Echo/Say
  - name: collect
    type: grpc
    url: grpc://%[3]s
    method: /hikyaku.test.echo.v1.Echo/Collect
    batch: { max_messages: 3, max_wait: 5s }
  - name: bulk
    type: grpc
    url: grpc://%[3]s
    method: /hikyaku.test.echo.v1.Echo/Say
    descriptor_set: testdata/echo.pb
    batch: { max_messages: 3, max_wait: 5s, format: json_array }
    body: '{"text": {{ json .bodyText }}, "count": {{ index .attributes "batch.size" }}}'
  - name: watch
    type: grpc
    url: grpc://%[3]s
    method: /hikyaku.test.echo.v1.Feed/Watch
routes:
  - { name: chat, from: [chat], to: [say, collect, bulk] }
  - { name: feed, from: [feed], to: [watch] }
server: { admin: { disabled: true } }
`, chatAddr, feedAddr, srv.addr)
	l, err := config.LoadBytes(context.Background(), "e2e.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), l.Spec, engine.Options{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	chatConn, feedConn := dial(t, chatAddr), dial(t, feedAddr)
	for _, cc := range []*grpc.ClientConn{chatConn, feedConn} {
		require.Eventually(t, func() bool {
			_, err := healthgrpc.NewHealthClient(cc).Check(context.Background(), &healthgrpc.HealthCheckRequest{})
			return err == nil
		}, 5*time.Second, 20*time.Millisecond, "the sources listen")
	}

	// Every message of the bidi call waits for its batch, so the three
	// requests go out before any response comes back.
	call := metadata.AppendToOutgoingContext(context.Background(), "idempotency-key", "talk-1")
	stream, err := echopb.NewChatClient(chatConn).Talk(call)
	require.NoError(t, err)
	for i, text := range []string{"a", "b", "c"} {
		require.NoError(t, stream.Send(&echopb.SayRequest{Text: text, Count: int32(i + 1)}))
	}
	require.NoError(t, stream.CloseSend())
	for _, want := range []*echopb.SayReply{{Text: "A", Count: 2}, {Text: "B", Count: 3}, {Text: "C", Count: 4}} {
		reply, err := stream.Recv()
		require.NoError(t, err)
		assert.Equal(t, want.GetText(), reply.GetText(), "say's reply answers each request, in order")
		assert.Equal(t, want.GetCount(), reply.GetCount())
	}
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{"talk-1-2"}, stream.Trailer().Get(headerID))
	assert.Equal(t, []string{"delivered"}, stream.Trailer().Get(headerStatus))

	require.Len(t, es.collected(), 1, "the client-streaming destination took the batch in one call")
	assert.ElementsMatch(t, []string{"a", "b", "c"}, texts(es.collected()[0]))
	es.mu.Lock()
	var single []string
	var combined []*echopb.SayRequest
	for _, s := range es.says {
		if s.GetCount() == 3 && len(s.GetText()) > 1 {
			combined = append(combined, s)
		} else {
			single = append(single, s.GetText())
		}
	}
	es.mu.Unlock()
	assert.ElementsMatch(t, []string{"a", "b", "c"}, single)
	require.Len(t, combined, 1, "the unary destination took the batch combined into one request")
	var bodies []map[string]any
	require.NoError(t, json.Unmarshal([]byte(combined[0].GetText()), &bodies))
	assert.Len(t, bodies, 3)
	keys := map[string]bool{}
	for _, md := range es.calls() {
		for _, k := range md.Get("idempotency-key") {
			keys[k] = true
		}
	}
	assert.True(t, keys["talk-1-0"] && keys["talk-1-1"] && keys["talk-1-2"], "each message's key: %v", keys)

	watch, err := echopb.NewFeedClient(feedConn).Watch(context.Background(), &echopb.WatchRequest{Topic: "orders", Limit: 3})
	require.NoError(t, err)
	var seqs []int64
	for {
		ev, err := watch.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		assert.Equal(t, "orders", ev.GetTopic())
		seqs = append(seqs, ev.GetSeq())
	}
	assert.Equal(t, []int64{1, 2, 3}, seqs, "the upstream stream, one response per response")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the relay did not stop")
	}
}
