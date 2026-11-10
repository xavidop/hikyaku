package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// maxReply bounds the response a destination takes, as the http
// destination does.
const maxReply = 10 << 20

// defaultHealthTimeout bounds a health check whose context has no deadline.
const defaultHealthTimeout = 5 * time.Second

// rerouteKey is how a server says a message belonged elsewhere: the
// metadata twin of the http X-Hikyaku-Reroute header.
const rerouteKey = "x-hikyaku-reroute"

// Sink is a grpc destination: one call per delivery over one long-lived
// connection, to a method of any kind. A client- or bidi-streaming method
// also takes a batch in one call.
type Sink struct {
	name    string
	opts    sinkOptions
	service string // the method's service, for health checks
	conn    *grpc.ClientConn
	log     *slog.Logger
	// static is the metadata of the headers that are not templates, sent
	// with reflection and health calls too (a server may want its token
	// there as well).
	static metadata.MD

	// replyLimit bounds the JSON array of a stream's responses: maxReply.
	replyLimit int

	// sem guards the method's resolution; unlike a mutex, waiting for it
	// honours the caller's context.
	sem chan struct{}
	reg *registry // descriptor_set, or what reflection described
	md  protoreflect.MethodDescriptor

	closeOnce sync.Once
	closeErr  error
}

var (
	_ plugin.Sink      = (*Sink)(nil)
	_ plugin.BatchSink = (*Sink)(nil)
)

func newSink(_ context.Context, spec plugin.Spec) (*Sink, error) {
	o, err := parseSink(spec)
	if err != nil {
		return nil, err
	}
	s := &Sink{name: spec.Name, opts: o, log: spec.Env.Log(), sem: make(chan struct{}, 1), static: metadata.MD{}, replyLimit: maxReply}
	s.service, _, _ = splitMethod(o.method)
	for k, t := range o.headers {
		if t.IsStatic() {
			v, _ := metadataValue(k, t.String()) // checked by parseSink
			s.static.Set(k, v)
		}
	}
	if o.descriptor != "" {
		if s.reg, err = loadDescriptorSet(o.descriptor); err != nil {
			return nil, fmt.Errorf("destination %s: %w", spec.Name, err)
		}
		if s.md, err = s.reg.method(o.method); err != nil {
			return nil, fmt.Errorf("destination %s: %w", spec.Name, err)
		}
	}
	s.conn, err = grpc.NewClient("dns:///"+o.target.addr,
		grpc.WithTransportCredentials(o.target.creds),
		grpc.WithUserAgent(userAgent),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxReply)))
	if err != nil {
		return nil, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	return s, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver makes one call: the message body (or the body template) as the
// request, the message's headers and the configured ones as metadata, and
// the idempotency key as idempotency-key. A streaming method gets the
// request as the one message of its stream. The response of a unary or
// client-streaming method is the result's body, as JSON; the responses of
// a server-streaming or bidi method are a JSON array.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	md, types, err := s.method(ctx)
	if err != nil {
		return nil, err
	}
	req, err := s.request(md, types, env)
	if err != nil {
		return nil, err
	}
	out, err := s.callMetadata(env)
	if err != nil {
		return nil, err
	}
	if !md.IsStreamingClient() && !md.IsStreamingServer() {
		return s.unary(ctx, md, types, req, out)
	}
	return s.stream(ctx, md, types, []proto.Message{req}, out)
}

// request is a message's request: its body (or the body template,
// rendered) mapped into the method's input type.
func (s *Sink) request(md protoreflect.MethodDescriptor, types resolver, env *envelope.Envelope) (proto.Message, error) {
	body, contentType := env.Body, env.ContentType
	if s.opts.body != nil {
		b, err := s.opts.body.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		body, contentType = []byte(b), contentJSON
	}
	req := dynamicpb.NewMessage(md.Input())
	if err := decode(body, contentType, req, types); err != nil {
		return nil, plugin.Permanentf("the message does not fit %s: %v", md.Input().FullName(), err)
	}
	return req, nil
}

// callMetadata is a message's metadata: its headers that gRPC accepts, its
// idempotency-key, then the configured headers, rendered.
func (s *Sink) callMetadata(env *envelope.Envelope) (metadata.MD, error) {
	out := forwarded(env.Headers)
	out.Set("idempotency-key", idempotencyKey(env))
	for k, t := range s.opts.headers {
		v, err := t.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		mv, err := metadataValue(k, v)
		if err != nil {
			return nil, plugin.Permanentf("headers: %v", err)
		}
		out.Set(k, mv)
	}
	return out, nil
}

// unary makes a unary call.
func (s *Sink) unary(ctx context.Context, md protoreflect.MethodDescriptor, types resolver, req proto.Message, out metadata.MD) (*plugin.Result, error) {
	resp := dynamicpb.NewMessage(md.Output())
	var header, trailer metadata.MD
	var p peer.Peer
	err := s.conn.Invoke(metadata.NewOutgoingContext(ctx, out), s.opts.method, req, resp,
		grpc.Header(&header), grpc.Trailer(&trailer), grpc.Peer(&p))
	if err != nil {
		return nil, classify(s.opts.method, err, p.Addr != nil, trailer, header)
	}
	data, err := toJSON(resp, types)
	if err != nil {
		return nil, plugin.Permanentf("rendering the response as JSON: %v", err)
	}
	return s.result(data, header), nil
}

// result is what a call that succeeded returns: the body, as JSON, the
// response's header metadata, and the destinations the server asked for
// with x-hikyaku-reroute.
func (s *Sink) result(body []byte, header metadata.MD) *plugin.Result {
	res := &plugin.Result{Body: body, ContentType: contentJSON, Headers: headersOf(header), Endpoint: s.opts.target.shown}
	res.Headers["Content-Type"] = []string{contentJSON}
	for _, v := range header.Get(rerouteKey) {
		for _, d := range strings.Split(v, ",") {
			if d = strings.TrimSpace(d); d != "" {
				res.Reroute = append(res.Reroute, d)
			}
		}
	}
	return res
}

// idempotencyKey is the key the engine sends with every attempt: the
// caller's own, else the message ID.
func idempotencyKey(env *envelope.Envelope) string {
	if v, ok := env.Attribute("idempotency_key"); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return env.ID
}

// method returns the method called and the types of its messages,
// resolving it through server reflection on first use.
func (s *Sink) method(ctx context.Context) (protoreflect.MethodDescriptor, resolver, error) {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, resolver{}, plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, ctx.Err())
	}
	defer func() { <-s.sem }()
	if s.md != nil {
		return s.md, s.reg.types, nil
	}
	reg, err := describe(metadata.NewOutgoingContext(ctx, s.static), s.conn, s.service)
	if err != nil {
		what := fmt.Sprintf("resolving %s through server reflection (set descriptor_set when the server has none)", s.opts.method)
		return nil, resolver{}, classify(what, err, false)
	}
	md, err := reg.method(s.opts.method)
	if err != nil {
		return nil, resolver{}, plugin.Permanentf("%v", err)
	}
	s.reg, s.md = reg, md
	return md, reg.types, nil
}

// Health asks the server's grpc.health.v1 service about the method's
// service (or the server as a whole when it does not know the service);
// a server without the health service is healthy while it is connected.
func (s *Sink) Health(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultHealthTimeout)
		defer cancel()
	}
	ctx = metadata.NewOutgoingContext(ctx, s.static)
	hc := healthgrpc.NewHealthClient(s.conn)
	resp, err := hc.Check(ctx, &healthgrpc.HealthCheckRequest{Service: s.service})
	if status.Code(err) == codes.NotFound {
		resp, err = hc.Check(ctx, &healthgrpc.HealthCheckRequest{})
	}
	switch status.Code(err) {
	case codes.OK:
		if st := resp.GetStatus(); st != healthgrpc.HealthCheckResponse_SERVING {
			return fmt.Errorf("%s: health: %s", s.opts.target.shown, st)
		}
		return nil
	case codes.Unimplemented, codes.NotFound:
		return s.connected(ctx)
	}
	return fmt.Errorf("%s: health check: %w", s.opts.target.shown, err)
}

// connected is nil once the connection is ready.
func (s *Sink) connected(ctx context.Context) error {
	s.conn.Connect()
	for {
		st := s.conn.GetState()
		switch st {
		case connectivity.Ready:
			return nil
		case connectivity.TransientFailure:
			return fmt.Errorf("%s: cannot connect", s.opts.target.shown)
		case connectivity.Shutdown:
			return fmt.Errorf("%s: closed", s.opts.target.shown)
		}
		if !s.conn.WaitForStateChange(ctx, st) {
			return fmt.Errorf("%s: not connected: %w", s.opts.target.shown, ctx.Err())
		}
	}
}

// Close closes the connection.
func (s *Sink) Close(context.Context) error {
	s.closeOnce.Do(func() { s.closeErr = s.conn.Close() })
	return s.closeErr
}
