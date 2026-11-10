package grpc

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/protocols/grpc/internal/echopb"
)

// A relay that is a gRPC proxy: a call to the listen source is delivered to
// a gRPC destination, whose reply answers the call; and a subscribe source
// feeds another destination through a body template.
func TestEndToEnd(t *testing.T) {
	Register()
	es := &echoServer{hold: true}
	srv := startEcho(t, es, serverOpts{})
	addr := closedAddr(t)
	spec := fmt.Sprintf(`
version: 1
name: grpc-e2e
sources:
  - name: rpc
    type: grpc
    listen: "%s"
    descriptor_set: testdata/echo.pb
    services: [hikyaku.test.echo.v1.Echo]
    respond: { from: echo, timeout: 5s }
  - name: feed
    type: grpc
    url: grpc://%s
    method: /hikyaku.test.echo.v1.Feed/Watch
    body: '{"topic": "orders", "limit": 2}'
destinations:
  - name: echo
    type: grpc
    url: grpc://%s
    method: /hikyaku.test.echo.v1.Echo/Say
    retry: { attempts: 3, backoff: 1ms }
  - name: from-feed
    type: grpc
    url: grpc://%s
    method: /hikyaku.test.echo.v1.Echo/Say
    descriptor_set: testdata/echo.pb
    body: '{"text": "{{ .json.topic }}-{{ .json.seq }}"}'
routes:
  - { name: calls, from: [rpc], to: [echo] }
  - { name: events, from: [feed], to: [from-feed] }
server: { admin: { disabled: true } }
`, addr, srv.addr, srv.addr, srv.addr)
	l, err := config.LoadBytes(context.Background(), "e2e.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), l.Spec, engine.Options{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	client := echopb.NewEchoClient(dial(t, addr))
	var reply *echopb.SayReply
	require.Eventually(t, func() bool {
		call := metadata.AppendToOutgoingContext(context.Background(), "idempotency-key", "call-1")
		reply, err = client.Say(call, &echopb.SayRequest{Text: "hello", Count: 41})
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)
	assert.Equal(t, "HELLO", reply.GetText(), "the destination's reply answers the call")
	assert.Equal(t, int32(42), reply.GetCount())

	texts := func() []string {
		es.mu.Lock()
		defer es.mu.Unlock()
		var out []string
		for _, s := range es.says {
			out = append(out, s.GetText())
		}
		return out
	}
	require.Eventually(t, func() bool {
		got := texts()
		return slices.Contains(got, "orders-1") && slices.Contains(got, "orders-2")
	}, 5*time.Second, 20*time.Millisecond, "the streamed events reached the destination")
	keys := map[string]bool{}
	for _, md := range es.calls() {
		for _, k := range md.Get("idempotency-key") {
			keys[k] = true
		}
	}
	assert.True(t, keys["call-1"], "the caller's idempotency key went through")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the relay did not stop")
	}
}
