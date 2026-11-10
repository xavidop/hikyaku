package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/xavidop/hikyaku/envelope"
)

// defaultWindow bounds the messages of one bidi call that wait for their
// outcome. A call with that many reads no further request until the oldest
// is answered, so HTTP/2 flow control holds a fast client back.
const defaultWindow = 64

// serverStream handles a server-streaming method: its request is one
// message, and the response stream carries the answer. A respond.from
// reply that holds several JSON values (a JSON array, or NDJSON) is one
// response per value; any other answer is the one response a unary call
// gets. The stream then ends OK.
func (l *Listener) serverStream(md protoreflect.MethodDescriptor) grpc.StreamHandler {
	full := fullMethod(md)
	return func(_ any, ss grpc.ServerStream) error {
		ctx := ss.Context()
		in := dynamicpb.NewMessage(md.Input())
		if err := ss.RecvMsg(in); err != nil {
			return err
		}
		env, err := l.messageEnvelope(ctx, full, in, -1)
		if err != nil {
			return err
		}
		c, err := l.handOver(env, ss.SetTrailer)
		if err != nil {
			return err
		}
		v := l.judge([]*envelope.Envelope{env}, l.wait(ctx, []*call{c}))
		if v.err != nil {
			return v.err
		}
		outs, err := l.outputs(md, v.reply)
		if err != nil {
			return err
		}
		_ = ss.SetHeader(v.md)
		for _, out := range outs {
			if err := ss.SendMsg(out); err != nil {
				return err
			}
		}
		return nil
	}
}

// outputs maps a reply into the responses of a server-streaming method:
// one per element of a JSON array, or per line of NDJSON; any other reply
// (and no reply) is one response, as for a unary method.
func (l *Listener) outputs(md protoreflect.MethodDescriptor, rp *envelope.Reply) ([]proto.Message, error) {
	var values [][]byte
	if rp != nil && !isProtobuf(rp.ContentType) {
		values = jsonValues(rp.Body)
	}
	if values == nil {
		out, err := l.output(md, rp)
		if err != nil {
			return nil, err
		}
		return []proto.Message{out}, nil
	}
	outs := make([]proto.Message, len(values))
	for i, v := range values {
		out := dynamicpb.NewMessage(md.Output())
		if err := fromJSON(v, out, l.types()); err != nil {
			return nil, status.Errorf(codes.Internal, "value %d of the reply of %s does not fit %s: %v", i, l.opts.respond, md.Output().FullName(), err)
		}
		outs[i] = out
	}
	return outs, nil
}

// jsonValues splits a body that holds a sequence of JSON values: the
// elements of a JSON array (none for []), or the lines of NDJSON. It is nil
// for any other body, such as a single JSON object.
func jsonValues(b []byte) [][]byte {
	b = bytes.TrimSpace(b)
	if json.Valid(b) {
		var elems []json.RawMessage
		if len(b) == 0 || b[0] != '[' || json.Unmarshal(b, &elems) != nil {
			return nil
		}
		out := make([][]byte, len(elems))
		for i, e := range elems {
			out[i] = e
		}
		return out
	}
	var out [][]byte
	for line := range bytes.SplitSeq(b, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if !json.Valid(line) {
			return nil
		}
		out = append(out, line)
	}
	return out
}

// bidiStream handles a bidi-streaming method: each request is one message,
// and each gets the response a unary call of the method would get, in
// request order, once its outcome is final. At most window messages wait
// at a time. The first message whose answer is an error status ends the
// call with that status, once the responses before it are sent. The call
// ends OK once the client closed its side and every response is sent.
func (l *Listener) bidiStream(md protoreflect.MethodDescriptor) grpc.StreamHandler {
	full := fullMethod(md)
	return func(_ any, ss grpc.ServerStream) error {
		b := &bidi{l: l, md: md, full: full, ss: ss}
		err := b.run()
		if b.answered > 0 {
			// The headers went out with the first response: the trailers
			// say what became of the messages.
			ss.SetTrailer(statusMD(b.lastID, b.worst))
		}
		return err
	}
}

// bidi is one call to a bidi-streaming method.
type bidi struct {
	l    *Listener
	md   protoreflect.MethodDescriptor
	full string
	ss   grpc.ServerStream

	waiting  []*inflight // handed over and not answered, in request order
	index    int         // the next request's
	answered int
	lastID   string // the last message answered
	worst    string // the worst status of the messages answered
}

// inflight is a message of a bidi call waiting for its outcome.
type inflight struct {
	env      *envelope.Envelope
	c        *call
	deadline time.Time // when its response stops waiting for the outcome
}

// request is what the reader received: a request, or why there is none.
type request struct {
	msg *dynamicpb.Message
	err error
}

func (b *bidi) run() error {
	ctx := b.ss.Context()
	reqs := make(chan request)
	quit := make(chan struct{})
	defer close(quit)
	go b.read(reqs, quit)

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	var armed *inflight // the message the timer runs for
	var end error       // ends the call once the messages before it are answered
	closed := false     // the client closed its side
	for {
		if len(b.waiting) == 0 && (closed || end != nil) {
			return end
		}
		var in <-chan request
		if !closed && end == nil && len(b.waiting) < b.l.window {
			in = reqs
		}
		var settledC <-chan settled
		var expired <-chan time.Time
		if len(b.waiting) > 0 {
			head := b.waiting[0]
			if armed != head {
				armed = head
				timer.Reset(time.Until(head.deadline))
			}
			settledC, expired = head.c.done, timer.C
		}
		select {
		case r := <-in:
			switch {
			case errors.Is(r.err, io.EOF):
				closed = true
			case r.err != nil:
				return r.err // the stream is over: nothing more can be sent
			default:
				end = b.take(ctx, r.msg)
			}
		case s := <-settledC:
			if err := b.answer(b.one(s)); err != nil {
				return err
			}
		case <-expired:
			if err := b.answer(b.known(result{timedOut: true})); err != nil {
				return err
			}
		case <-b.l.stop:
			if !closed && end == nil {
				select {
				case r := <-reqs: // a request that came with the stop is not taken
					closed = errors.Is(r.err, io.EOF)
				default:
				}
			}
			return b.stopping(closed, end)
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

// read receives the requests and hands them to run one at a time, until
// the client closes its side or the call ends. While run takes no more,
// read receives no more, and the client is held back.
func (b *bidi) read(reqs chan<- request, quit <-chan struct{}) {
	for {
		msg := dynamicpb.NewMessage(b.md.Input())
		err := b.ss.RecvMsg(msg)
		select {
		case reqs <- request{msg: msg, err: err}:
		case <-quit:
			return
		}
		if err != nil {
			return
		}
	}
}

// take hands a request to the relay as the call's next message. A refusal
// (a full buffer, a stopping source) ends the call once the messages
// before it are answered.
func (b *bidi) take(ctx context.Context, msg *dynamicpb.Message) error {
	env, err := b.l.messageEnvelope(ctx, b.full, msg, b.index)
	if err != nil {
		return err
	}
	b.index++
	c, err := b.l.handOver(env, b.ss.SetTrailer)
	if err != nil {
		return err
	}
	b.waiting = append(b.waiting, &inflight{env: env, c: c, deadline: time.Now().Add(b.l.opts.wait)})
	return nil
}

// one is the result of the oldest waiting message once it settled.
func (b *bidi) one(s settled) result {
	res := b.l.newResult(1)
	res.settle(0, s)
	return res
}

// known is the result of the oldest waiting message: its outcome when it
// is already known, else fallback.
func (b *bidi) known(fallback result) result {
	select {
	case s := <-b.waiting[0].c.done:
		return b.one(s)
	default:
		return fallback
	}
}

// answer sends the oldest waiting message its response, or returns the
// status that ends the call instead.
func (b *bidi) answer(res result) error {
	head := b.waiting[0]
	b.waiting[0] = nil
	b.waiting = b.waiting[1:]
	v := b.l.judge([]*envelope.Envelope{head.env}, res)
	if v.err != nil {
		return v.err
	}
	out, err := b.l.output(b.md, v.reply)
	if err != nil {
		return err
	}
	if err := b.ss.SendMsg(out); err != nil {
		return err
	}
	b.answered++
	b.lastID = head.env.ID
	if b.worst == "" || rank[v.status] > rank[b.worst] {
		b.worst = v.status
	}
	return nil
}

// stopping answers the waiting messages once the source stops, as calls
// waiting for an outcome are answered (pending, or UNAVAILABLE with
// respond), unless their outcome is known. The call then ends OK if the
// client had closed its side, else UNAVAILABLE: the client sends the rest
// again.
func (b *bidi) stopping(closed bool, end error) error {
	for len(b.waiting) > 0 {
		if err := b.answer(b.known(result{stopping: true})); err != nil {
			return err
		}
	}
	switch {
	case end != nil:
		return end
	case closed:
		return nil
	}
	return status.Error(codes.Unavailable, "the relay is shutting down; retry")
}

// rank orders the statuses of a bidi call's messages from the best to the
// worst, for the one its trailers carry.
var rank = map[string]int{
	statusAccepted: 0, "delivered": 0, "filtered": 1, "unrouted": 2, statusPending: 3, "dead_lettered": 4,
}
