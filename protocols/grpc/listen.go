package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/protocols/grpc/relaypb"
)

// defaultGrace is how long a stopping listen source lets calls in flight
// finish before it cuts them.
const defaultGrace = 5 * time.Second

// maxKey is the longest message ID a caller may choose.
const maxKey = 256

// Statuses answered beside the relay's own (envelope.AckStatus).
const (
	statusAccepted = "accepted"
	statusPending  = "pending"
)

// The response metadata of a call to a descriptor_set method.
const (
	headerID     = "hikyaku-id"
	headerStatus = "hikyaku-status"
)

var (
	errNotStarted = errors.New("not started")
	errStopped    = errors.New("stopped")
)

// ownServices are served by every listen source; a descriptor set may not
// serve them again.
var ownServices = map[string]bool{
	"hikyaku.relay.v1.Relay":                   true,
	"grpc.health.v1.Health":                    true,
	"grpc.reflection.v1.ServerReflection":      true,
	"grpc.reflection.v1alpha.ServerReflection": true,
}

var propagator = propagation.TraceContext{}

// Listener is a grpc listen source: a gRPC server whose calls are messages.
type Listener struct {
	refused  func(source string) // hikyaku_backpressure_total
	name     string
	opts     listenOptions
	auth     *inbound.Authenticator // nil without auth
	log      *slog.Logger
	reg      *registry // descriptor_set, nil without one
	services []protoreflect.ServiceDescriptor
	grace    time.Duration
	window   int // messages of one bidi call that may wait for their outcome

	started atomic.Bool
	out     chan<- *envelope.Envelope
	stop    chan struct{} // closed once the source stops

	mu     sync.Mutex
	addr   string
	status error
}

func newListener(ctx context.Context, spec plugin.Spec, src *config.Source) (*Listener, error) {
	o, err := parseListen(spec.Name, src)
	if err != nil {
		return nil, err
	}
	l := &Listener{name: spec.Name, opts: o, log: spec.Env.Log(), grace: defaultGrace, window: defaultWindow,
		stop: make(chan struct{}), status: errNotStarted, refused: spec.Env.Refused}
	if o.descriptor != "" {
		if l.reg, err = loadDescriptorSet(o.descriptor); err != nil {
			return nil, fmt.Errorf("source %s: %w", spec.Name, err)
		}
		if l.services, err = servedServices(l.reg, o.services); err != nil {
			return nil, fmt.Errorf("source %s: %w", spec.Name, err)
		}
	}
	if src.Auth != nil {
		if l.auth, err = inbound.New(ctx, src.Auth); err != nil {
			return nil, fmt.Errorf("source %s: %w", spec.Name, err)
		}
	}
	return l, nil
}

// servedServices picks the services of a descriptor set to serve: all, or
// those named. Methods of every kind are served.
func servedServices(reg *registry, names []string) ([]protoreflect.ServiceDescriptor, error) {
	var out []protoreflect.ServiceDescriptor
	if len(names) == 0 {
		for _, sd := range reg.services() {
			if !ownServices[string(sd.FullName())] {
				out = append(out, sd)
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("descriptor_set describes no service to serve")
		}
	}
	for _, n := range names {
		n = strings.TrimPrefix(strings.TrimSpace(n), ".")
		if ownServices[n] {
			return nil, fmt.Errorf("services: %s is served by every grpc listen source", n)
		}
		sd, err := reg.service(n)
		if err != nil {
			return nil, fmt.Errorf("services: %w", err)
		}
		if !slices.Contains(out, sd) {
			out = append(out, sd)
		}
	}
	return out, nil
}

// Name is the source name.
func (l *Listener) Name() string { return l.name }

// Health is nil while the source serves, else why not.
func (l *Listener) Health(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.status
}

// Addr is the address the source is bound to, "" before it listens.
func (l *Listener) Addr() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.addr
}

func (l *Listener) setStatus(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.status = err
}

// types resolves message types for the JSON mapping.
func (l *Listener) types() resolver {
	if l.reg == nil {
		return resolver{}
	}
	return l.reg.types
}

// Start serves until ctx ends, then stops: calls waiting for an outcome are
// answered at once (the relay still has their messages), and calls still
// running after the grace period are cut. It fails only when the address
// cannot be bound.
func (l *Listener) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !l.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	l.out = out
	srv := grpc.NewServer(l.serverOptions()...)
	relaypb.RegisterRelayServer(srv, relayServer{l: l})
	for _, sd := range l.services {
		srv.RegisterService(l.serviceDesc(sd), l)
	}
	hs := health.NewServer()
	healthgrpc.RegisterHealthServer(srv, hs)
	if l.opts.reflection {
		ro := reflection.ServerOptions{Services: srv, DescriptorResolver: files{local: l.localFiles()}}
		reflectionv1.RegisterServerReflectionServer(srv, reflection.NewServerV1(ro))
		reflectionv1alpha.RegisterServerReflectionServer(srv, reflection.NewServer(ro))
	}
	names := make([]string, 0, len(srv.GetServiceInfo()))
	for name := range srv.GetServiceInfo() {
		hs.SetServingStatus(name, healthgrpc.HealthCheckResponse_SERVING)
		names = append(names, name)
	}
	slices.Sort(names)

	ln, err := listenPort(l.opts.listen)
	if err != nil {
		srv.Stop()
		err = fmt.Errorf("listen %s: %w", l.opts.listen, err)
		l.setStatus(err)
		return err
	}
	l.mu.Lock()
	l.addr, l.status = ln.Addr().String(), nil
	l.mu.Unlock()
	l.log.Info("grpc source listening", "source", l.name, "addr", ln.Addr().String(), "services", names)

	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
	case err := <-served:
		// Only a stop ends Serve; anything else broke the listener.
		close(l.stop)
		srv.Stop()
		l.setStatus(fmt.Errorf("serve %s: %w", l.opts.listen, err))
		return fmt.Errorf("serve %s: %w", l.opts.listen, err)
	}
	l.shutdown(srv, hs)
	if err := <-served; err != nil {
		l.log.Debug("grpc server stopped", "source", l.name, "err", err)
	}
	return nil
}

// localFiles is the descriptor set reflection describes beside the files
// linked into hikyaku.
func (l *Listener) localFiles() *protoregistry.Files {
	if l.reg == nil {
		return nil
	}
	return l.reg.files
}

func (l *Listener) serverOptions() []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(l.opts.maxBytes),
		// Stop returns once every handler has: no message is handed over
		// after Start returns.
		grpc.WaitForHandlers(true),
	}
	if l.opts.serverTLS != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(l.opts.serverTLS)))
	}
	if l.auth != nil {
		opts = append(opts, grpc.ChainUnaryInterceptor(l.authUnary), grpc.ChainStreamInterceptor(l.authStream))
	}
	return opts
}

// shutdown stops serving: health turns NOT_SERVING, waiting calls are
// answered, and the server stops gracefully within the grace period.
func (l *Listener) shutdown(srv *grpc.Server, hs *health.Server) {
	l.setStatus(errStopped)
	close(l.stop)
	hs.Shutdown()
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	t := time.NewTimer(l.grace)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		srv.Stop()
		<-done
	}
}

// serviceDesc serves a described service with generic handlers.
func (l *Listener) serviceDesc(sd protoreflect.ServiceDescriptor) *grpc.ServiceDesc {
	desc := &grpc.ServiceDesc{
		ServiceName: string(sd.FullName()),
		HandlerType: (*any)(nil),
		Metadata:    sd.ParentFile().Path(),
	}
	for i := 0; i < sd.Methods().Len(); i++ {
		md := sd.Methods().Get(i)
		stream := grpc.StreamDesc{StreamName: string(md.Name()), ClientStreams: md.IsStreamingClient(), ServerStreams: md.IsStreamingServer()}
		switch {
		case stream.ClientStreams && stream.ServerStreams:
			stream.Handler = l.bidiStream(md)
		case stream.ClientStreams:
			stream.Handler = l.clientStream(md)
		case stream.ServerStreams:
			stream.Handler = l.serverStream(md)
		default:
			desc.Methods = append(desc.Methods, grpc.MethodDesc{MethodName: string(md.Name()), Handler: l.unary(md)})
			continue
		}
		desc.Streams = append(desc.Streams, stream)
	}
	return desc
}

// unary handles a unary method: its request is one message.
func (l *Listener) unary(md protoreflect.MethodDescriptor) grpc.MethodHandler {
	full := fullMethod(md)
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		in := dynamicpb.NewMessage(md.Input())
		if err := dec(in); err != nil {
			return nil, err
		}
		handle := func(ctx context.Context, req any) (any, error) {
			msg, ok := req.(*dynamicpb.Message)
			if !ok {
				return nil, status.Errorf(codes.Internal, "unexpected request type %T", req)
			}
			env, err := l.messageEnvelope(ctx, full, msg, -1)
			if err != nil {
				return nil, err
			}
			c, err := l.handOver(env, func(t metadata.MD) { _ = grpc.SetTrailer(ctx, t) })
			if err != nil {
				return nil, err
			}
			out, h, err := l.answer(md, []*envelope.Envelope{env}, l.wait(ctx, []*call{c}))
			if err != nil {
				return nil, err
			}
			_ = grpc.SetHeader(ctx, h)
			return out, nil
		}
		if interceptor == nil {
			return handle(ctx, in)
		}
		return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: full}, handle)
	}
}

// clientStream handles a client-streaming method: every request is one
// message, and the call is answered once they are all settled.
func (l *Listener) clientStream(md protoreflect.MethodDescriptor) grpc.StreamHandler {
	full := fullMethod(md)
	return func(_ any, ss grpc.ServerStream) error {
		ctx := ss.Context()
		var envs []*envelope.Envelope
		var calls []*call
		for i := 0; ; i++ {
			in := dynamicpb.NewMessage(md.Input())
			if err := ss.RecvMsg(in); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			env, err := l.messageEnvelope(ctx, full, in, i)
			if err != nil {
				return err
			}
			c, err := l.handOver(env, ss.SetTrailer)
			if err != nil {
				return err
			}
			envs, calls = append(envs, env), append(calls, c)
		}
		if len(calls) == 0 {
			return ss.SendMsg(dynamicpb.NewMessage(md.Output()))
		}
		reply, h, err := l.answer(md, envs, l.wait(ctx, calls))
		if err != nil {
			return err
		}
		_ = ss.SetHeader(h)
		return ss.SendMsg(reply)
	}
}

// messageEnvelope is the envelope of one request of a described method:
// its JSON, the full method name as subject. index numbers the requests of
// a client or bidi stream (-1 for a call with one request); an idempotency
// key the caller set becomes the ID, suffixed with the index in a stream.
func (l *Listener) messageEnvelope(ctx context.Context, method string, msg proto.Message, index int) (*envelope.Envelope, error) {
	body, err := toJSON(msg, l.types())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rendering the request as JSON: %v", err)
	}
	env := l.newEnvelope(ctx, method, body)
	env.ContentType = contentJSON
	if index >= 0 {
		env.Attributes["grpc.message_index"] = index
		if key, ok := env.Attributes["idempotency_key"].(string); ok {
			key += "-" + strconv.Itoa(index)
			env.ID, env.Attributes["idempotency_key"] = key, key
		}
	}
	return env, nil
}

// newEnvelope starts the envelope of a call: the call's metadata as
// headers, the method and the peer as attributes, the caller's trace, and
// its idempotency-key as the ID.
func (l *Listener) newEnvelope(ctx context.Context, method string, body []byte) *envelope.Envelope {
	env := envelope.New(l.name, kind, body)
	env.Subject = method
	md, _ := metadata.FromIncomingContext(ctx)
	env.Headers = headersOf(md)
	l.auth.StripCredentials(env.Headers)
	env.Attributes["grpc.method"] = method
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		env.Attributes["grpc.peer"] = p.Addr.String()
		env.Attributes["remote_addr"] = p.Addr.String()
	}
	carrier := propagation.HeaderCarrier(http.Header(env.Headers))
	if sc := trace.SpanContextFromContext(propagator.Extract(context.Background(), carrier)); sc.IsValid() {
		env.Trace = sc
	}
	if key := first(md, "idempotency-key"); key != "" && len(key) <= maxKey {
		env.ID = key
		env.Attributes["idempotency_key"] = key
	}
	return env
}

// call is one message handed to the relay.
type call struct {
	done chan settled // the queued signal (ack: accepted) and the outcome
}

type settled struct {
	queued  bool
	outcome envelope.Outcome
}

// handOver gives env to the relay without blocking: a full buffer answers
// RESOURCE_EXHAUSTED (with a retry-after trailer) instead of holding the
// caller, the way an http source answers 503.
func (l *Listener) handOver(env *envelope.Envelope, trailer func(metadata.MD)) (*call, error) {
	c := &call{done: make(chan settled, 2)}
	env.OnAck(func(_ context.Context, o envelope.Outcome) { c.done <- settled{outcome: o} })
	if l.opts.ack == ackAccepted {
		env.OnQueued(func() { c.done <- settled{queued: true} })
	}
	select {
	case <-l.stop:
		return nil, status.Error(codes.Unavailable, "the relay is shutting down; retry")
	default:
	}
	select {
	case l.out <- env:
		return c, nil
	default:
		if l.refused != nil {
			l.refused(l.name)
		}
		trailer(metadata.Pairs("retry-after", "1"))
		return nil, status.Error(codes.ResourceExhausted, "the relay is busy; retry in a second")
	}
}

// result is how the relay finished with the messages of a call.
type result struct {
	outcomes []envelope.Outcome // in order, when every message settled
	accepted bool               // ack: accepted, and the relay holds them all
	timedOut bool               // the wait ran out; the relay still has them
	stopping bool               // the source stopped first; the relay still has them
	err      error              // the caller went away
}

// wait waits for every message of a call to settle: delivered,
// dead-lettered, unrouted, filtered or failed (with ack: accepted, queued
// is enough), up to the source's wait.
func (l *Listener) wait(ctx context.Context, calls []*call) result {
	timer := time.NewTimer(l.opts.wait)
	defer timer.Stop()
	res := l.newResult(len(calls))
	for i, c := range calls {
		var s settled
		select {
		case s = <-c.done: // an outcome already known wins over the rest
		default:
			select {
			case s = <-c.done:
			case <-ctx.Done():
				return result{err: ctx.Err()}
			case <-l.stop:
				return result{stopping: true}
			case <-timer.C:
				return result{timedOut: true}
			}
		}
		res.settle(i, s)
	}
	return res
}

// newResult is the result of n messages before any settled.
func (l *Listener) newResult(n int) result {
	return result{outcomes: make([]envelope.Outcome, n), accepted: l.opts.ack == ackAccepted}
}

// settle records how message i settled: queued, with ack: accepted, or
// its outcome.
func (r *result) settle(i int, s settled) {
	if !s.queued {
		r.outcomes[i] = s.outcome
		r.accepted = r.accepted && s.outcome.Status != envelope.Failed
	}
}

// severity orders outcomes from the best to the worst, for the status of a
// client stream.
var severity = map[envelope.AckStatus]int{
	envelope.Delivered: 0, envelope.Filtered: 1, envelope.Unrouted: 2, envelope.DeadLettered: 3, envelope.Failed: 4,
}

func worst(outcomes []envelope.Outcome) envelope.AckStatus {
	w := envelope.Delivered
	for _, o := range outcomes {
		if severity[o.Status] > severity[w] {
			w = o.Status
		}
	}
	return w
}

// verdict is how a call to a described method is answered: with an error
// status, or OK with its response metadata and the respond.from reply to
// map into the output type.
type verdict struct {
	err    error
	md     metadata.MD     // hikyaku-id, hikyaku-status and the reply's headers
	status string          // hikyaku-status
	reply  *envelope.Reply // nil: an empty output
}

// judge decides the answer to a call from how its messages settled: the
// respond.from destination's reply to the last message when there is one,
// an empty output, or the status that says why not.
func (l *Listener) judge(envs []*envelope.Envelope, res result) verdict {
	id := envs[len(envs)-1].ID
	ok := func(st string) verdict { return verdict{md: statusMD(id, st), status: st} }
	switch {
	case res.err != nil:
		return verdict{err: status.FromContextError(res.err).Err()}
	case res.stopping:
		if l.opts.respond != "" {
			return verdict{err: status.Error(codes.Unavailable, "the relay is shutting down; the message is still being delivered")}
		}
		return ok(statusPending)
	case res.timedOut:
		if l.opts.respond != "" {
			return verdict{err: status.Errorf(codes.DeadlineExceeded, "no reply from %s within %s; the message is still being delivered", l.opts.respond, l.opts.wait)}
		}
		return ok(statusPending)
	}
	w := worst(res.outcomes)
	if w == envelope.Failed {
		return verdict{err: status.Error(codes.Unavailable, "the relay could not keep the message; retry")}
	}
	if res.accepted {
		return ok(statusAccepted)
	}
	v := ok(w.String())
	if l.opts.respond == "" {
		return v
	}
	last := res.outcomes[len(res.outcomes)-1]
	rp := last.Replies[l.opts.respond]
	if rp == nil {
		if last.Status == envelope.DeadLettered {
			return verdict{err: status.Errorf(codes.Unavailable, "%s failed; the message was dead-lettered", l.opts.respond)}
		}
		return v
	}
	v.md, v.reply = metadata.Join(forwarded(rp.Headers), v.md), rp
	return v
}

// answer is the reply to a call to a described method and its response
// metadata, or the status that says why there is none.
func (l *Listener) answer(md protoreflect.MethodDescriptor, envs []*envelope.Envelope, res result) (proto.Message, metadata.MD, error) {
	v := l.judge(envs, res)
	if v.err != nil {
		return nil, nil, v.err
	}
	out, err := l.output(md, v.reply)
	if err != nil {
		return nil, nil, err
	}
	return out, v.md, nil
}

// output maps a reply into the method's output type; no reply is an empty
// output.
func (l *Listener) output(md protoreflect.MethodDescriptor, rp *envelope.Reply) (proto.Message, error) {
	out := dynamicpb.NewMessage(md.Output())
	if rp == nil {
		return out, nil
	}
	if err := decode(rp.Body, rp.ContentType, out, l.types()); err != nil {
		return nil, status.Errorf(codes.Internal, "the reply of %s does not fit %s: %v", l.opts.respond, md.Output().FullName(), err)
	}
	return out, nil
}

// statusMD is the response metadata naming a call's message and outcome.
func statusMD(id, st string) metadata.MD {
	return metadata.Pairs(headerID, id, headerStatus, st)
}

// decode fills m from a body: binary protobuf when the content type says
// so, else JSON.
func decode(body []byte, contentType string, m proto.Message, r resolver) error {
	if isProtobuf(contentType) {
		return proto.UnmarshalOptions{Resolver: r}.Unmarshal(body, m)
	}
	return fromJSON(body, m, r)
}

// relayServer serves hikyaku.relay.v1.Relay.
type relayServer struct {
	relaypb.UnimplementedRelayServer
	l *Listener
}

// Send relays one message and says what became of it.
func (r relayServer) Send(ctx context.Context, m *relaypb.Message) (*relaypb.SendReply, error) {
	l := r.l
	if len(m.GetId()) > maxKey {
		return nil, status.Errorf(codes.InvalidArgument, "id is longer than %d characters", maxKey)
	}
	env := l.newEnvelope(ctx, relaypb.Relay_Send_FullMethodName, m.GetBody())
	if s := m.GetSubject(); s != "" {
		env.Subject = s
	}
	for k, v := range m.GetHeaders() {
		env.SetHeader(k, v)
	}
	env.ContentType = m.GetContentType()
	if env.ContentType == "" {
		env.ContentType = sniff(env.Body)
	}
	if id := m.GetId(); id != "" {
		env.ID = id
		env.Attributes["idempotency_key"] = id
	}
	c, err := l.handOver(env, func(t metadata.MD) { _ = grpc.SetTrailer(ctx, t) })
	if err != nil {
		return nil, err
	}
	res := l.wait(ctx, []*call{c})
	switch {
	case res.err != nil:
		return nil, status.FromContextError(res.err).Err()
	case res.stopping && l.opts.respond != "":
		return nil, status.Error(codes.Unavailable, "the relay is shutting down; the message is still being delivered")
	case res.timedOut && l.opts.respond != "":
		return nil, status.Errorf(codes.DeadlineExceeded, "no outcome within %s; the message is still being delivered", l.opts.wait)
	case res.stopping, res.timedOut:
		return &relaypb.SendReply{Id: env.ID, Status: statusPending}, nil
	}
	o := res.outcomes[0]
	switch {
	case o.Status == envelope.Failed:
		return nil, status.Error(codes.Unavailable, "the relay could not keep the message; retry")
	case res.accepted:
		return &relaypb.SendReply{Id: env.ID, Status: statusAccepted}, nil
	}
	return &relaypb.SendReply{Id: env.ID, Status: o.Status.String()}, nil
}

// authUnary checks a unary call against auth.
func (l *Listener) authUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := l.authorize(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// authStream checks a streaming call against auth.
func (l *Listener) authStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := l.authorize(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}

// authorize runs the inbound checks on a call: they read an HTTP request,
// so the call's metadata become its headers and the peer its remote
// address. Health checks are open, as /healthz is.
func (l *Listener) authorize(ctx context.Context, method string) error {
	if strings.HasPrefix(method, "/grpc.health.v1.Health/") {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	r := (&http.Request{
		Method: http.MethodPost, URL: &url.URL{Path: method}, Header: http.Header{},
		Proto: "HTTP/2.0", ProtoMajor: 2,
	}).WithContext(ctx)
	for k, vs := range md {
		if strings.HasPrefix(k, ":") {
			continue
		}
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		r.RemoteAddr = p.Addr.String()
	}
	if err := l.auth.Check(r, nil); err != nil {
		l.log.Debug("grpc call refused", "source", l.name, "method", method, "remote_addr", r.RemoteAddr, "err", err)
		return status.Error(codes.Unauthenticated, "unauthenticated")
	}
	return nil
}
