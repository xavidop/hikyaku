package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/protocols/grpc/internal/echopb"
)

// streamListener is a listen source serving the Feed (server-streaming
// Watch) and Chat (bidi Talk) services of the test descriptor set.
func streamListener(t *testing.T, c config.Source) *Listener {
	t.Helper()
	c.DescriptorSet = echoSet
	c.Services = []string{feedService, chatService}
	return newTestListener(t, c)
}

// textOf is the text of a SayRequest envelope.
func textOf(env *envelope.Envelope) string {
	var in struct{ Text string }
	_ = json.Unmarshal(env.Body, &in)
	return in.Text
}

// events reads a Watch stream to its end.
func events(t *testing.T, stream grpc.ServerStreamingClient[echopb.Event]) ([]*echopb.Event, error) {
	t.Helper()
	var out []*echopb.Event
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, ev)
	}
}

func TestServerStreamingMethod(t *testing.T) {
	cases := []struct {
		name  string
		reply *envelope.Reply
		want  []string // the topics of the events streamed
	}{
		{"a JSON array", &envelope.Reply{Body: []byte(`[{"topic":"a","seq":"1"},{"topic":"b","seq":2}]`), ContentType: contentJSON}, []string{"a", "b"}},
		{"NDJSON", &envelope.Reply{Body: []byte("{\"topic\":\"a\"}\n\n{\"topic\":\"b\"}\n{\"topic\":\"c\"}\n"), ContentType: "application/x-ndjson"}, []string{"a", "b", "c"}},
		{"one object", &envelope.Reply{Body: []byte(`{"topic":"one"}`), ContentType: contentJSON}, []string{"one"}},
		{"an empty array", &envelope.Reply{Body: []byte(` [] `), ContentType: contentJSON}, nil},
		{"binary protobuf", &envelope.Reply{Body: mustMarshal(t, &echopb.Event{Topic: "bin"}), ContentType: "application/x-protobuf"}, []string{"bin"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := streamListener(t, config.Source{Respond: &config.Respond{From: "api"}})
			out := make(chan *envelope.Envelope, 8)
			addr, _ := listening(t, l, out)
			seen := fakeEngine(t, out, func(*envelope.Envelope) envelope.Outcome {
				rp := *c.reply
				rp.Headers = map[string][]string{"X-Served-By": {"api-1"}}
				return envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{"api": &rp}}
			})
			ctx := metadata.AppendToOutgoingContext(context.Background(), "idempotency-key", "watch-1")
			stream, err := echopb.NewFeedClient(dial(t, addr)).Watch(ctx, &echopb.WatchRequest{Topic: "orders", Limit: 3})
			require.NoError(t, err)
			evs, err := events(t, stream)
			require.NoError(t, err, "the stream ends OK")
			var topics []string
			for _, ev := range evs {
				topics = append(topics, ev.GetTopic())
			}
			assert.Equal(t, c.want, topics, "the reply, one response per value")
			header, err := stream.Header()
			require.NoError(t, err)
			assert.Equal(t, []string{"watch-1"}, header.Get(headerID))
			assert.Equal(t, []string{"delivered"}, header.Get(headerStatus))
			assert.Equal(t, []string{"api-1"}, header.Get("x-served-by"), "the reply's headers")

			env := recv(t, seen)
			assert.Equal(t, "watch-1", env.ID, "the request is one message: the key as it is")
			assert.Equal(t, watchMethod, env.Subject)
			assert.NotContains(t, env.Attributes, "grpc.message_index")
			assert.JSONEq(t, `{"topic":"orders","limit":3}`, env.BodyText())
		})
	}
}

func TestServerStreamingAnswers(t *testing.T) {
	cases := []struct {
		name    string
		src     config.Source
		outcome envelope.Outcome
		code    codes.Code
		status  string // hikyaku-status of the one empty response, with OK
	}{
		{"without respond", config.Source{}, envelope.Outcome{Status: envelope.DeadLettered}, codes.OK, "dead_lettered"},
		{"routed elsewhere", config.Source{Respond: &config.Respond{From: "api"}}, envelope.Outcome{Status: envelope.Unrouted}, codes.OK, "unrouted"},
		{"failed", config.Source{}, envelope.Outcome{Status: envelope.Failed}, codes.Unavailable, ""},
		{"dead-lettered without a reply", config.Source{Respond: &config.Respond{From: "api"}}, envelope.Outcome{Status: envelope.DeadLettered}, codes.Unavailable, ""},
		{"a value that does not fit", config.Source{Respond: &config.Respond{From: "api"}}, envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
			"api": {Body: []byte(`[{"topic":"a"},{"seq":"many"}]`)},
		}}, codes.Internal, ""},
		{"a reply that is not JSON", config.Source{Respond: &config.Respond{From: "api"}}, envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
			"api": {Body: []byte("{\"topic\":\"a\"}\nnot json")},
		}}, codes.Internal, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := streamListener(t, c.src)
			out := make(chan *envelope.Envelope, 8)
			addr, _ := listening(t, l, out)
			fakeEngine(t, out, func(*envelope.Envelope) envelope.Outcome { return c.outcome })
			stream, err := echopb.NewFeedClient(dial(t, addr)).Watch(context.Background(), &echopb.WatchRequest{Topic: "x"})
			require.NoError(t, err)
			evs, err := events(t, stream)
			require.Equal(t, c.code, status.Code(err), "%v", err)
			if c.code != codes.OK {
				assert.Empty(t, evs)
				return
			}
			require.Len(t, evs, 1, "one response, as a unary call gets")
			assert.True(t, proto.Equal(&echopb.Event{}, evs[0]), "an empty output")
			header, err := stream.Header()
			require.NoError(t, err)
			assert.Equal(t, []string{c.status}, header.Get(headerStatus))
		})
	}

	t.Run("accepted", func(t *testing.T) {
		l := streamListener(t, config.Source{Ack: "accepted"})
		out := make(chan *envelope.Envelope, 8)
		addr, _ := listening(t, l, out)
		go func() {
			select {
			case env := <-out:
				env.Queued() // never acked
			case <-time.After(5 * time.Second):
			}
		}()
		stream, err := echopb.NewFeedClient(dial(t, addr)).Watch(context.Background(), &echopb.WatchRequest{Topic: "x"})
		require.NoError(t, err)
		evs, err := events(t, stream)
		require.NoError(t, err)
		assert.Len(t, evs, 1)
		header, err := stream.Header()
		require.NoError(t, err)
		assert.Equal(t, []string{"accepted"}, header.Get(headerStatus))
	})
}

func TestBidiStreamingMethod(t *testing.T) {
	l := streamListener(t, config.Source{Respond: &config.Respond{From: "api"}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	seen := fakeEngine(t, out, func(env *envelope.Envelope) envelope.Outcome {
		n, _ := env.Attributes["grpc.message_index"].(int)
		return envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
			"api": {Body: []byte(`{"text":"` + strings.ToUpper(textOf(env)) + `","count":` + itoa(n) + `}`), Headers: map[string][]string{"X-Served-By": {"api-1"}}},
		}}
	})
	ctx := metadata.AppendToOutgoingContext(context.Background(), "idempotency-key", "chat-7")
	stream, err := echopb.NewChatClient(dial(t, addr)).Talk(ctx)
	require.NoError(t, err)
	for i, text := range []string{"a", "b", "c"} {
		require.NoError(t, stream.Send(&echopb.SayRequest{Text: text}))
		reply, err := stream.Recv()
		require.NoError(t, err)
		assert.Equal(t, strings.ToUpper(text), reply.GetText(), "each request is answered in turn, with respond.from's reply")
		assert.Equal(t, int32(i), reply.GetCount())
	}
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF, "the call ends OK once the client closed its side and every response is sent")
	trailer := stream.Trailer()
	assert.Equal(t, []string{"chat-7-2"}, trailer.Get(headerID), "the trailers name the last message")
	assert.Equal(t, []string{"delivered"}, trailer.Get(headerStatus))
	header, err := stream.Header()
	require.NoError(t, err)
	assert.Empty(t, header.Get("x-served-by"), "no reply headers: they would be one message's")

	for i, text := range []string{"a", "b", "c"} {
		env := recv(t, seen)
		assert.Equal(t, talkMethod, env.Subject)
		assert.Equal(t, "chat-7-"+itoa(i), env.ID, "the call's key, numbered")
		assert.Equal(t, env.ID, env.Attributes["idempotency_key"])
		assert.Equal(t, i, env.Attributes["grpc.message_index"])
		assert.JSONEq(t, `{"text":"`+text+`","count":0,"at":null}`, env.BodyText())
	}
}

// Outcomes may come in any order; responses go out in request order.
func TestBidiStreamingAnswersInOrder(t *testing.T) {
	l := streamListener(t, config.Source{Respond: &config.Respond{From: "api"}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	go func() {
		var envs []*envelope.Envelope
		for range 3 {
			select {
			case env := <-out:
				envs = append(envs, env)
			case <-time.After(5 * time.Second):
				return
			}
		}
		for i := len(envs) - 1; i >= 0; i-- { // the last one first
			envs[i].Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
				"api": {Body: []byte(`{"text":"` + strings.ToUpper(textOf(envs[i])) + `"}`)},
			}})
		}
	}()
	stream, err := echopb.NewChatClient(dial(t, addr)).Talk(context.Background())
	require.NoError(t, err)
	for _, text := range []string{"a", "b", "c"} {
		require.NoError(t, stream.Send(&echopb.SayRequest{Text: text}))
	}
	require.NoError(t, stream.CloseSend())
	for _, want := range []string{"A", "B", "C"} {
		reply, err := stream.Recv()
		require.NoError(t, err)
		assert.Equal(t, want, reply.GetText())
	}
	_, err = stream.Recv()
	assert.ErrorIs(t, err, io.EOF)
}

// A call takes no more than window messages waiting for their outcome: the
// next request is read once the oldest is answered.
func TestBidiStreamingWindow(t *testing.T) {
	l := streamListener(t, config.Source{})
	l.window = 2
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	stream, err := echopb.NewChatClient(dial(t, addr)).Talk(context.Background())
	require.NoError(t, err)
	for range 5 {
		require.NoError(t, stream.Send(&echopb.SayRequest{Text: "x"}))
	}
	ack := func(env *envelope.Envelope) {
		env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	}
	none := func() {
		t.Helper()
		select {
		case env := <-out:
			t.Fatalf("message %v was taken past the window", env.Attributes["grpc.message_index"])
		case <-time.After(200 * time.Millisecond):
		}
	}
	first, second := recv(t, out), recv(t, out)
	none()
	ack(first)
	_, err = stream.Recv()
	require.NoError(t, err, "the first is answered")
	third := recv(t, out)
	assert.Equal(t, 2, third.Attributes["grpc.message_index"], "the window takes the next request")
	none()
	ack(second)
	ack(third)
	fourth, fifth := recv(t, out), recv(t, out)
	ack(fourth)
	ack(fifth)
	require.NoError(t, stream.CloseSend())
	for range 4 {
		_, err := stream.Recv()
		require.NoError(t, err)
	}
	_, err = stream.Recv()
	assert.ErrorIs(t, err, io.EOF)
}

// A message whose unary answer would be an error status ends the call with
// it, once the responses before it are sent.
func TestBidiStreamingFailureEndsTheCall(t *testing.T) {
	l := streamListener(t, config.Source{})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	seen := fakeEngine(t, out, func(env *envelope.Envelope) envelope.Outcome {
		if textOf(env) == "bad" {
			return envelope.Outcome{Status: envelope.Failed}
		}
		return envelope.Outcome{Status: envelope.Delivered}
	})
	stream, err := echopb.NewChatClient(dial(t, addr)).Talk(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echopb.SayRequest{Text: "good"}))
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echopb.SayRequest{Text: "bad"}))
	_, err = stream.Recv()
	assert.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	first := recv(t, seen)
	assert.Equal(t, []string{first.ID}, stream.Trailer().Get(headerID), "the last message answered")
	assert.Equal(t, []string{"delivered"}, stream.Trailer().Get(headerStatus))
}

func TestBidiStreamingWaitRunsOut(t *testing.T) {
	for _, respond := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "respond"}[respond], func(t *testing.T) {
			c := config.Source{}
			if respond {
				c.Respond = &config.Respond{From: "api"}
			}
			l := streamListener(t, c)
			l.opts.wait = 100 * time.Millisecond
			out := make(chan *envelope.Envelope, 8)
			addr, _ := listening(t, l, out)
			go func() {
				for range 2 {
					select {
					case env := <-out:
						if textOf(env) == "fast" { // slow never settles
							env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
						}
					case <-time.After(5 * time.Second):
						return
					}
				}
			}()
			stream, err := echopb.NewChatClient(dial(t, addr)).Talk(context.Background())
			require.NoError(t, err)
			require.NoError(t, stream.Send(&echopb.SayRequest{Text: "slow"}))
			require.NoError(t, stream.Send(&echopb.SayRequest{Text: "fast"}))
			if respond {
				_, err = stream.Recv()
				assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "%v", err)
				return
			}
			for range 2 {
				reply, err := stream.Recv()
				require.NoError(t, err)
				assert.True(t, proto.Equal(&echopb.SayReply{}, reply))
			}
			require.NoError(t, stream.CloseSend())
			_, err = stream.Recv()
			require.ErrorIs(t, err, io.EOF)
			assert.Equal(t, []string{"pending"}, stream.Trailer().Get(headerStatus), "the worst status: the relay still has the first")
		})
	}
}

// A full buffer refuses the request, and the call ends with
// RESOURCE_EXHAUSTED once the messages before it are answered.
func TestBidiStreamingBufferFull(t *testing.T) {
	l := streamListener(t, config.Source{})
	out := make(chan *envelope.Envelope) // one message is taken, then the buffer is full
	addr, _ := listening(t, l, out)
	taken := make(chan *envelope.Envelope, 1)
	go func() {
		select {
		case env := <-out:
			taken <- env
		case <-time.After(5 * time.Second):
		}
	}()
	stream, err := echopb.NewChatClient(dial(t, addr)).Talk(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echopb.SayRequest{Text: "a"}))
	env := <-taken
	require.NoError(t, stream.Send(&echopb.SayRequest{Text: "b"}))
	time.Sleep(50 * time.Millisecond) // b is refused while a waits
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	_, err = stream.Recv()
	require.NoError(t, err, "a is answered first")
	_, err = stream.Recv()
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
	assert.Equal(t, []string{"1"}, stream.Trailer().Get("retry-after"))
	assert.Equal(t, []string{env.ID}, stream.Trailer().Get(headerID))
}

// When the source stops, the messages waiting are answered at once, and
// the call ends: UNAVAILABLE while the client may send more, OK once it
// closed its side.
func TestBidiStreamingStop(t *testing.T) {
	for _, c := range []struct {
		name    string
		respond bool
		closed  bool
	}{{"pending", false, false}, {"closed", false, true}, {"respond", true, false}} {
		t.Run(c.name, func(t *testing.T) {
			src := config.Source{}
			if c.respond {
				src.Respond = &config.Respond{From: "api"}
			}
			l := streamListener(t, src)
			out := make(chan *envelope.Envelope, 8)
			addr, stop := listening(t, l, out)
			stream, err := echopb.NewChatClient(dial(t, addr)).Talk(context.Background())
			require.NoError(t, err)
			require.NoError(t, stream.Send(&echopb.SayRequest{Text: "a"}))
			require.NoError(t, stream.Send(&echopb.SayRequest{Text: "b"}))
			recv(t, out) // taken, never acked
			recv(t, out)
			if c.closed {
				require.NoError(t, stream.CloseSend())
				time.Sleep(50 * time.Millisecond) // the source sees the call closed before it stops
			}
			began := time.Now()
			stop()
			assert.Less(t, time.Since(began), 250*time.Millisecond, "the call ends at once, not after the grace period")
			if c.respond {
				_, err = stream.Recv()
				assert.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
				return
			}
			for range 2 {
				reply, err := stream.Recv()
				require.NoError(t, err)
				assert.True(t, proto.Equal(&echopb.SayReply{}, reply))
			}
			_, err = stream.Recv()
			if c.closed {
				assert.ErrorIs(t, err, io.EOF)
			} else {
				assert.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
			}
			assert.Equal(t, []string{"pending"}, stream.Trailer().Get(headerStatus))
		})
	}
}

func TestBidiStreamingAccepted(t *testing.T) {
	l := streamListener(t, config.Source{Ack: "accepted"})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	go func() {
		for range 2 {
			select {
			case env := <-out:
				env.Queued() // never acked
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()
	stream, err := echopb.NewChatClient(dial(t, addr)).Talk(context.Background())
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, stream.Send(&echopb.SayRequest{Text: "x"}))
		reply, err := stream.Recv()
		require.NoError(t, err, "answered once the relay holds the message")
		assert.True(t, proto.Equal(&echopb.SayReply{}, reply))
	}
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{"accepted"}, stream.Trailer().Get(headerStatus))
}

func TestBidiStreamingEdges(t *testing.T) {
	l := streamListener(t, config.Source{MaxBodyBytes: 64, Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"t0k"}}}})
	out := make(chan *envelope.Envelope, 8)
	addr, _ := listening(t, l, out)
	fakeEngine(t, out, nil)
	cc := dial(t, addr)
	authed := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer t0k")

	stream, err := echopb.NewChatClient(cc).Talk(context.Background())
	require.NoError(t, err)
	_, err = stream.Recv()
	assert.Equal(t, codes.Unauthenticated, status.Code(err), "streams are checked against auth too")
	watch, err := echopb.NewFeedClient(cc).Watch(context.Background(), &echopb.WatchRequest{})
	require.NoError(t, err)
	_, err = watch.Recv()
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	stream, err = echopb.NewChatClient(cc).Talk(authed)
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF, "a call without requests relays nothing")
	assert.Empty(t, stream.Trailer().Get(headerStatus))

	stream, err = echopb.NewChatClient(cc).Talk(authed)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echopb.SayRequest{Text: "fits"}))
	_, err = stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echopb.SayRequest{Text: strings.Repeat("x", 100)}))
	_, err = stream.Recv()
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "a request larger than max_body_bytes")
}
