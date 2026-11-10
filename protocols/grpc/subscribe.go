package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a subscribe source; tests shorten them.
const (
	// defaultStableAfter: a stream up this long resets the failure count
	// even if it delivered nothing.
	defaultStableAfter = 30 * time.Second
	// defaultDialTimeout bounds resolving the method through reflection.
	defaultDialTimeout = 30 * time.Second
)

// userAgent is what hikyaku calls itself on its outgoing calls.
const userAgent = "hikyaku"

// Subscriber is a grpc subscribe source: it calls a server-streaming
// method and relays every response.
type Subscriber struct {
	name string
	opts subscribeOptions
	log  *slog.Logger
	// From descriptor_set; without one every session resolves the method
	// through server reflection.
	reg *registry
	md  protoreflect.MethodDescriptor
	req *dynamicpb.Message

	stableAfter time.Duration
	dialTimeout time.Duration
	rnd         func() float64

	started atomic.Bool
	mu      sync.Mutex
	status  error
}

func newSubscriber(spec plugin.Spec, src *config.Source) (*Subscriber, error) {
	o, err := parseSubscribe(spec.Name, src)
	if err != nil {
		return nil, err
	}
	s := &Subscriber{name: spec.Name, opts: o, log: spec.Env.Log(), stableAfter: defaultStableAfter,
		dialTimeout: defaultDialTimeout, rnd: rand.Float64, status: errNotStarted}
	if o.descriptor != "" {
		if s.reg, err = loadDescriptorSet(o.descriptor); err != nil {
			return nil, fmt.Errorf("source %s: %w", spec.Name, err)
		}
		if s.md, s.req, err = streamMethod(s.reg, o.method, o.body); err != nil {
			return nil, fmt.Errorf("source %s: %w", spec.Name, err)
		}
	}
	return s, nil
}

// streamMethod finds a server-streaming method and builds its request.
func streamMethod(reg *registry, method string, body []byte) (protoreflect.MethodDescriptor, *dynamicpb.Message, error) {
	md, err := reg.method(method)
	if err != nil {
		return nil, nil, err
	}
	if !md.IsStreamingServer() || md.IsStreamingClient() {
		return nil, nil, fmt.Errorf("method %s is %s: a subscribe source calls a server-streaming method", method, streaming(md))
	}
	req := dynamicpb.NewMessage(md.Input())
	if err := fromJSON(body, req, reg.types); err != nil {
		return nil, nil, fmt.Errorf("body does not fit %s: %v", md.Input().FullName(), err)
	}
	return md, req, nil
}

// Name is the source name.
func (s *Subscriber) Name() string { return s.name }

// Health is nil while the stream is up, else why it is not.
func (s *Subscriber) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *Subscriber) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

// Start calls the method and relays its stream until ctx ends, then
// returns nil. A stream that ends or breaks is called again with backoff;
// a failed attempt rotates to the next of url and failover_urls, and a
// stream that delivered a message or stayed up for a while resets the
// failure count. Start fails only after reconnect.max_attempts failed
// attempts in a row.
func (s *Subscriber) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	rc := s.opts.reconnect
	failures, i := 0, 0
	for {
		st, err := s.session(ctx, s.opts.targets[i], out)
		if ctx.Err() != nil {
			s.setStatus(errStopped)
			return nil
		}
		s.setStatus(err)
		if st.connected && (st.delivered > 0 || st.uptime >= s.stableAfter) {
			failures = 0
		} else {
			failures++
			if rc.maxAttempts > 0 && failures >= rc.maxAttempts {
				return fmt.Errorf("giving up after %d failed attempts in a row: %w", failures, err)
			}
			i = (i + 1) % len(s.opts.targets)
		}
		wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, max(failures, 1), s.rnd)
		s.log.Warn("grpc source reconnecting", "source", s.name, "url", s.opts.targets[i].shown,
			"failures", failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			s.setStatus(errStopped)
			return nil
		}
	}
}

// session is what one call of the method did.
type session struct {
	connected bool
	delivered int
	uptime    time.Duration
}

// session dials t, calls the method and relays the stream until it ends.
func (s *Subscriber) session(ctx context.Context, t target, out chan<- *envelope.Envelope) (session, error) {
	conn, err := grpc.NewClient("dns:///"+t.addr, grpc.WithTransportCredentials(t.creds), grpc.WithUserAgent(userAgent))
	if err != nil {
		return session{}, fmt.Errorf("%s: %w", t.shown, err)
	}
	defer func() { _ = conn.Close() }()
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	md, req, types, err := s.resolve(sctx, conn)
	if err != nil {
		return session{}, fmt.Errorf("%s: %w", t.shown, err)
	}
	stream, err := conn.NewStream(metadata.NewOutgoingContext(sctx, s.opts.md), &grpc.StreamDesc{ServerStreams: true},
		s.opts.method, grpc.MaxCallRecvMsgSize(s.wireLimit()))
	if err != nil {
		return session{}, fmt.Errorf("%s: %w", t.shown, err)
	}
	if err := stream.SendMsg(req); err != nil {
		return session{}, fmt.Errorf("%s: %w", t.shown, streamErr(err, func() error {
			return stream.RecvMsg(dynamicpb.NewMessage(md.Output()))
		}))
	}
	if err := stream.CloseSend(); err != nil {
		return session{}, fmt.Errorf("%s: %w", t.shown, err)
	}
	s.setStatus(nil)
	s.log.Info("grpc source subscribed", "source", s.name, "url", t.shown, "method", s.opts.method)

	start := time.Now()
	n := 0
	var headers map[string][]string
	for {
		msg := dynamicpb.NewMessage(md.Output())
		if err := stream.RecvMsg(msg); err != nil {
			st := session{connected: true, delivered: n, uptime: time.Since(start)}
			if errors.Is(err, io.EOF) {
				return st, fmt.Errorf("%s: the server ended the stream", t.shown)
			}
			return st, fmt.Errorf("%s: %w", t.shown, err)
		}
		if headers == nil {
			h, _ := stream.Header()
			headers = headersOf(h)
		}
		body, err := toJSON(msg, types)
		if err != nil {
			s.log.Error("grpc message dropped: it does not render as JSON", "source", s.name, "method", s.opts.method, "err", err)
			continue
		}
		if len(body) > s.opts.maxBytes {
			s.log.Error("grpc message dropped: larger than max_body_bytes", "source", s.name, "method", s.opts.method,
				"bytes", len(body), "max_body_bytes", s.opts.maxBytes)
			continue
		}
		env := s.newEnvelope(t, body, headers)
		select {
		case out <- env:
			n++
		case <-ctx.Done():
			return session{connected: true, delivered: n, uptime: time.Since(start)}, nil
		}
	}
}

// resolve returns the method, its request and the types to render its
// responses with: from descriptor_set, or what the server describes
// through reflection.
func (s *Subscriber) resolve(ctx context.Context, conn grpc.ClientConnInterface) (protoreflect.MethodDescriptor, *dynamicpb.Message, resolver, error) {
	if s.reg != nil {
		return s.md, s.req, s.reg.types, nil
	}
	rctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, s.opts.md), s.dialTimeout)
	defer cancel()
	svc, _, _ := splitMethod(s.opts.method)
	reg, err := describe(rctx, conn, svc)
	if err != nil {
		return nil, nil, resolver{}, fmt.Errorf("resolving %s through server reflection: %w", s.opts.method, err)
	}
	md, req, err := streamMethod(reg, s.opts.method, s.opts.body)
	if err != nil {
		return nil, nil, resolver{}, err
	}
	return md, req, reg.types, nil
}

// wireLimit is the largest response the stream takes: room above
// max_body_bytes, so a message that is too large once rendered as JSON is
// dropped instead of breaking the stream.
func (s *Subscriber) wireLimit() int {
	if s.opts.maxBytes >= math.MaxInt32/4 {
		return math.MaxInt32
	}
	return max(4*s.opts.maxBytes, 16<<20)
}

// newEnvelope is the envelope of one streamed response.
func (s *Subscriber) newEnvelope(t target, body []byte, headers map[string][]string) *envelope.Envelope {
	env := envelope.New(s.name, kind, body)
	env.Subject = s.opts.method
	env.ContentType = contentJSON
	for k, v := range headers {
		env.Headers[k] = slices.Clone(v)
	}
	env.Attributes["grpc.method"] = s.opts.method
	env.Attributes["grpc.url"] = t.shown
	env.Attributes["ordering_key"] = s.opts.method
	id := env.ID
	plugin.OnSettle(env, s.opts.ack, func(_ context.Context, final bool) {
		if !final {
			s.log.Warn("grpc message failed; a gRPC stream cannot redeliver it", "source", s.name, "id", id)
		}
	})
	return env
}
