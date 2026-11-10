package grpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// DeliverBatch delivers a batch over one call of a client- or
// bidi-streaming method: one request per message, in order. A call has one
// set of metadata: the metadata every message agrees on, and an
// idempotency-key that names the batch. A unary or server-streaming method
// takes one request per call, so DeliverBatch hands the batch back
// (plugin.ErrNoBatch) and the relay delivers the messages combined into one.
func (s *Sink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	md, types, err := s.method(ctx)
	if err != nil {
		return nil, err
	}
	if !md.IsStreamingClient() {
		return nil, plugin.ErrNoBatch
	}
	if len(envs) == 0 {
		return &plugin.Result{Endpoint: s.opts.target.shown}, nil
	}
	reqs := make([]proto.Message, len(envs))
	mds := make([]metadata.MD, len(envs))
	for i, env := range envs {
		if reqs[i], err = s.request(md, types, env); err == nil {
			mds[i], err = s.callMetadata(env)
		}
		if err != nil {
			return nil, inBatch(i, len(envs), env, err)
		}
	}
	out := agreed(mds)
	if len(out.Get("idempotency-key")) == 0 {
		out.Set("idempotency-key", batchKey(envs))
	}
	return s.stream(ctx, md, types, reqs, out)
}

// inBatch names the message of a batch an error is about, keeping its
// class and tags.
func inBatch(i, n int, env *envelope.Envelope, err error) error {
	class, retryAfter := plugin.Classify(err)
	return &plugin.Error{Class: class, RetryAfter: retryAfter, Tags: plugin.Tags(err),
		Err: fmt.Errorf("message %d of %d (%s): %w", i+1, n, env.ID, err)}
}

// agreed is the metadata the messages of a batch agree on: each key whose
// values are the same in all of them.
func agreed(mds []metadata.MD) metadata.MD {
	out := metadata.MD{}
	for k, vs := range mds[0] {
		same := true
		for _, md := range mds[1:] {
			if !slices.Equal(md[k], vs) {
				same = false
				break
			}
		}
		if same {
			out[k] = slices.Clone(vs)
		}
	}
	return out
}

// batchKey names a batch of messages by their idempotency keys, in order:
// a batch that is tried again, here or on a fallback, has the same key.
func batchKey(envs []*envelope.Envelope) string {
	h := sha256.New()
	for _, env := range envs {
		_, _ = io.WriteString(h, idempotencyKey(env))
		_, _ = h.Write([]byte{0})
	}
	return "batch-" + hex.EncodeToString(h.Sum(nil)[:16])
}

// responses is what the server streamed back.
type responses struct {
	bodies    [][]byte // as JSON
	truncated bool     // some were dropped: past maxReply
	err       error
}

// stream makes one streaming call: it sends reqs, closes its side, and
// reads the responses until the server ends the stream. A client-streaming
// method's response is the body; a server-streaming or bidi method's
// responses are a JSON array, of at most maxReply bytes (the responses
// past it are read and dropped, as an http destination cuts a response).
func (s *Sink) stream(ctx context.Context, md protoreflect.MethodDescriptor, types resolver, reqs []proto.Message, out metadata.MD) (*plugin.Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	desc := &grpc.StreamDesc{StreamName: string(md.Name()), ClientStreams: md.IsStreamingClient(), ServerStreams: md.IsStreamingServer()}
	var header, trailer metadata.MD
	var p peer.Peer
	cs, err := s.conn.NewStream(metadata.NewOutgoingContext(ctx, out), desc, s.opts.method,
		grpc.Header(&header), grpc.Trailer(&trailer), grpc.Peer(&p))
	if err != nil {
		return nil, classify(s.opts.method, err, p.Addr != nil, trailer, header)
	}
	// The requests go out while the responses are read: a bidi server may
	// answer each request before it reads the next one.
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for _, req := range reqs {
			if cs.SendMsg(req) != nil {
				return // the stream is over, and receive says why
			}
		}
		_ = cs.CloseSend()
	}()
	r := receive(cs, md, types, s.replyLimit)
	cancel() // a send that flow control holds back gives up
	<-sent
	if r.err != nil {
		var pe *plugin.Error
		if errors.As(r.err, &pe) {
			return nil, r.err
		}
		return nil, classify(s.opts.method, r.err, p.Addr != nil, trailer, header)
	}
	if !md.IsStreamingServer() {
		return s.result(r.bodies[0], header), nil
	}
	if r.truncated {
		s.log.Warn("grpc reply cut: the responses are larger than the 10MiB a reply holds", "destination", s.name,
			"method", s.opts.method, "kept", len(r.bodies))
	}
	return s.result(jsonArray(r.bodies), header), nil
}

// receive reads a call's responses, as JSON, until the server ends the
// stream: the one response of a client-streaming method, every response of
// a server-streaming or bidi one, as many as fit in a JSON array of limit
// bytes.
func receive(cs grpc.ClientStream, md protoreflect.MethodDescriptor, types resolver, limit int) responses {
	var r responses
	size := len("[]")
	for {
		resp := dynamicpb.NewMessage(md.Output())
		if err := cs.RecvMsg(resp); err != nil {
			if !errors.Is(err, io.EOF) {
				r.err = err
			}
			return r
		}
		data, err := toJSON(resp, types)
		if err != nil {
			r.err = plugin.Permanentf("rendering the response as JSON: %v", err)
			return r
		}
		if !md.IsStreamingServer() {
			r.bodies = [][]byte{data}
			return r
		}
		if size += len(data) + 1; size > limit {
			r.truncated = true
			continue
		}
		r.bodies = append(r.bodies, data)
	}
}

// jsonArray joins JSON values into an array.
func jsonArray(values [][]byte) []byte {
	n := len("[]") + len(values)
	for _, v := range values {
		n += len(v)
	}
	out := make([]byte, 0, n)
	out = append(out, '[')
	for i, v := range values {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, v...)
	}
	return append(out, ']')
}
