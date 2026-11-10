package grpcplugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"time"

	goplugin "github.com/hashicorp/go-plugin"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// shutdownGrace bounds closing what the host left open when it stops the
// plugin: go-plugin kills the process two seconds after asking it to stop.
const shutdownGrace = 1500 * time.Millisecond

// Plugins are the kinds a plugin binary serves, by type name.
type Plugins struct {
	Sinks   map[string]SinkDef
	Sources map[string]SourceDef
}

// SinkDef is a destination type.
type SinkDef struct {
	// Factory builds a sink from a destination of the spec, as
	// plugin.RegisterSink's would. Its ctx carries hikyaku's deadline and
	// ends when the open call returns.
	Factory plugin.Factory[plugin.Sink]
	// Options is a zero value of the options struct (MyOptions{}): hikyaku
	// validates options: blocks against the JSON Schema generated from it
	// (yaml field names; jsonschema tags for required keys, enums,
	// descriptions). Nil accepts any options.
	Options any
	// Description is a one-line summary for docs and the schema.
	Description string
}

// SourceDef is a source type.
type SourceDef struct {
	// Factory builds a source from a source of the spec. Its ctx ends when
	// the open call returns; Start gets the long-lived one.
	Factory plugin.Factory[plugin.Source]
	// Options is a zero value of the options struct (see SinkDef.Options).
	Options any
	// Description is a one-line summary for docs and the schema.
	Description string
}

// Remote is the plugin.Spec.Config of the sinks and sources Serve opens:
// what hikyaku said about the spec entry besides its options: block.
type Remote struct {
	// Ack is a source's ack mode: "delivered" or "accepted".
	Ack string
}

// AckMode is a source's ack mode for plugin.OnSettle: "accepted" when the
// spec entry says so, else "delivered". It reads what hikyaku passed to a
// plugin served by Serve, and a spec entry in process alike.
func AckMode(spec plugin.Spec) string {
	var ack string
	switch c := spec.Config.(type) {
	case *Remote:
		ack = c.Ack
	case *config.Source:
		ack = c.Ack
	}
	if ack == "accepted" {
		return ack
	}
	return "delivered"
}

// Serve serves the plugins to hikyaku and returns once hikyaku stops the
// plugin, after closing what it left open. Call it from main. Run by hand
// (not by hikyaku), the binary says it is a plugin and exits 1, as it
// does when p is not valid.
//
// The plugin's logs belong on stderr, which hikyaku reads: the loggers in
// plugin.Spec.Env write hclog JSON lines there, so hikyaku logs every
// record with its level and attributes. Once serving, os.Stdout and
// os.Stderr are forwarded to hikyaku's log line by line.
func Serve(p Plugins) {
	stderr := os.Stderr // go-plugin replaces os.Stderr once it serves
	log := newPluginLogger(stderr, parseLevel(os.Getenv(logLevelEnv)))
	srv, err := newServer(p, log)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "hikyaku plugin: %v\n", err)
		os.Exit(1)
	}
	goplugin.Serve(&goplugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins:         pluginSet(srv),
		GRPCServer:      grpcServer(log),
		Logger:          newHCLogAdapter(log),
	})
	srv.shutdown()
}

// grpcServer builds the plugin's gRPC server: large messages, and a panic in
// plugin code fails its call (and is logged with its stack) instead of
// taking the process down.
func grpcServer(log *slog.Logger) func([]grpc.ServerOption) *grpc.Server {
	recovered := func(method string, r any) error {
		log.Error("plugin code panicked", "method", method, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		return status.Errorf(codes.Internal, "%s panicked: %v", method, r)
	}
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recovered(info.FullMethod, r)
			}
		}()
		return next(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recovered(info.FullMethod, r)
			}
		}()
		return next(srv, ss)
	}
	return func(opts []grpc.ServerOption) *grpc.Server {
		return grpc.NewServer(append(opts,
			grpc.MaxRecvMsgSize(maxMessageBytes),
			grpc.MaxSendMsgSize(maxMessageBytes),
			grpc.ChainUnaryInterceptor(unary),
			grpc.ChainStreamInterceptor(stream),
		)...)
	}
}

// server is the plugin side of the protocol.
type server struct {
	pluginpb.UnimplementedPluginServer
	defs  Plugins
	kinds []*pluginpb.Kind
	log   *slog.Logger

	mu      sync.Mutex
	next    uint64
	sinks   map[string]*openSink
	sources map[string]*openSource
}

type openSink struct {
	kind, name string
	sink       plugin.Sink
}

func newServer(p Plugins, log *slog.Logger) (*server, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	s := &server{defs: p, log: log, sinks: map[string]*openSink{}, sources: map[string]*openSource{}}
	for _, name := range slices.Sorted(maps.Keys(p.Sinks)) {
		d := p.Sinks[name]
		if err := s.addKind(name, pluginpb.Role_ROLE_SINK, d.Factory == nil, d.Options, d.Description); err != nil {
			return nil, err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(p.Sources)) {
		d := p.Sources[name]
		if err := s.addKind(name, pluginpb.Role_ROLE_SOURCE, d.Factory == nil, d.Options, d.Description); err != nil {
			return nil, err
		}
	}
	if len(s.kinds) == 0 {
		return nil, errors.New("no sinks or sources to serve")
	}
	return s, nil
}

func (s *server) addKind(name string, role pluginpb.Role, noFactory bool, options any, description string) error {
	if err := checkKindName(name); err != nil {
		return err
	}
	if noFactory {
		return fmt.Errorf("kind %q has no Factory", name)
	}
	schema, err := optionsSchema(options)
	if err != nil {
		return fmt.Errorf("kind %q: options schema: %w", name, err)
	}
	s.kinds = append(s.kinds, &pluginpb.Kind{Name: name, Role: role, Description: description, OptionsSchemaJson: schema})
	return nil
}

func (s *server) Describe(context.Context, *pluginpb.DescribeRequest) (*pluginpb.DescribeResponse, error) {
	return &pluginpb.DescribeResponse{Kinds: s.kinds}, nil
}

// spec is what a factory gets: the options as YAML would decode them (so
// numbers are ints where they are whole), and the host's logger.
func (s *server) spec(kind, name, relay, optionsJSON, ack string) (plugin.Spec, error) {
	opts := map[string]any{}
	if optionsJSON != "" {
		if err := yaml.Unmarshal([]byte(optionsJSON), &opts); err != nil {
			return plugin.Spec{}, fmt.Errorf("%s %s: options_json: %w", kind, name, err)
		}
		if opts == nil {
			opts = map[string]any{}
		}
	}
	return plugin.Spec{
		Name:    name,
		Kind:    kind,
		Options: opts,
		Config:  &Remote{Ack: ack},
		Env:     plugin.Env{Relay: relay, Logger: s.log.With("kind", kind, "name", name)},
	}, nil
}

func (s *server) handle(prefix string) string {
	s.next++
	return prefix + "-" + strconv.FormatUint(s.next, 10)
}

func (s *server) OpenSink(ctx context.Context, req *pluginpb.OpenSinkRequest) (*pluginpb.OpenSinkResponse, error) {
	def, ok := s.defs.Sinks[req.GetKind()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "this plugin serves no destination type %q", req.GetKind())
	}
	spec, err := s.spec(req.GetKind(), req.GetName(), req.GetRelay(), req.GetOptionsJson(), "")
	if err != nil {
		return &pluginpb.OpenSinkResponse{Error: errorToPB(err)}, nil
	}
	sink, err := def.Factory(ctx, spec)
	if err == nil && sink == nil {
		err = fmt.Errorf("%s %s: the factory returned no sink", req.GetKind(), req.GetName())
	}
	if err != nil {
		return &pluginpb.OpenSinkResponse{Error: errorToPB(err)}, nil
	}
	_, batch := sink.(plugin.BatchSink)
	s.mu.Lock()
	h := s.handle("sink")
	s.sinks[h] = &openSink{kind: req.GetKind(), name: req.GetName(), sink: sink}
	s.mu.Unlock()
	s.log.Debug("sink opened", "kind", req.GetKind(), "name", req.GetName(), "handle", h)
	return &pluginpb.OpenSinkResponse{Handle: h, Batch: batch}, nil
}

func (s *server) sink(h string) (*openSink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.sinks[h]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no open sink %q", h)
	}
	return o, nil
}

// deliverResponse puts a delivery's outcome in the response: the call
// itself succeeds, so the error keeps its class.
func deliverResponse(res *plugin.Result, err error) (*pluginpb.DeliverResponse, error) {
	if err != nil {
		return &pluginpb.DeliverResponse{Outcome: &pluginpb.DeliverResponse_Error{Error: errorToPB(err)}}, nil
	}
	if res == nil {
		return &pluginpb.DeliverResponse{}, nil
	}
	return &pluginpb.DeliverResponse{Outcome: &pluginpb.DeliverResponse_Result{Result: resultToPB(res)}}, nil
}

func (s *server) envelope(pe *pluginpb.Envelope) (*envelope.Envelope, error) {
	if pe == nil {
		return nil, status.Error(codes.InvalidArgument, "no envelope")
	}
	env, err := envelopeFromPB(pe)
	if err != nil {
		s.log.Warn("message attributes dropped", "id", pe.GetId(), "error", err)
	}
	return env, nil
}

func (s *server) Deliver(ctx context.Context, req *pluginpb.DeliverRequest) (*pluginpb.DeliverResponse, error) {
	o, err := s.sink(req.GetHandle())
	if err != nil {
		return nil, err
	}
	env, err := s.envelope(req.GetEnvelope())
	if err != nil {
		return nil, err
	}
	return deliverResponse(o.sink.Deliver(ctx, env))
}

func (s *server) DeliverBatch(ctx context.Context, req *pluginpb.DeliverBatchRequest) (*pluginpb.DeliverResponse, error) {
	o, err := s.sink(req.GetHandle())
	if err != nil {
		return nil, err
	}
	bs, ok := o.sink.(plugin.BatchSink)
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "destination %s does not batch", o.name)
	}
	envs := make([]*envelope.Envelope, len(req.GetEnvelopes()))
	for i, pe := range req.GetEnvelopes() {
		if envs[i], err = s.envelope(pe); err != nil {
			return nil, err
		}
	}
	return deliverResponse(bs.DeliverBatch(ctx, envs))
}

func (s *server) SinkHealth(ctx context.Context, req *pluginpb.HandleRequest) (*pluginpb.HealthResponse, error) {
	o, err := s.sink(req.GetHandle())
	if err != nil {
		return nil, err
	}
	return &pluginpb.HealthResponse{Error: errorToPB(o.sink.Health(ctx))}, nil
}

func (s *server) CloseSink(ctx context.Context, req *pluginpb.HandleRequest) (*pluginpb.CloseResponse, error) {
	s.mu.Lock()
	o, ok := s.sinks[req.GetHandle()]
	delete(s.sinks, req.GetHandle())
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no open sink %q", req.GetHandle())
	}
	s.log.Debug("sink closed", "kind", o.kind, "name", o.name)
	return &pluginpb.CloseResponse{Error: errorToPB(o.sink.Close(ctx))}, nil
}

// openSource is a source and the messages it streamed that hikyaku has not
// settled yet.
type openSource struct {
	kind, name string
	src        plugin.Source
	log        *slog.Logger

	run     sync.Mutex // held by the running stream: one at a time
	mu      sync.Mutex
	cancel  context.CancelFunc // stops the running stream
	ended   chan struct{}      // closed when it has stopped
	pending map[string][]*envelope.Envelope
}

func (s *server) OpenSource(ctx context.Context, req *pluginpb.OpenSourceRequest) (*pluginpb.OpenSourceResponse, error) {
	def, ok := s.defs.Sources[req.GetKind()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "this plugin serves no source type %q", req.GetKind())
	}
	spec, err := s.spec(req.GetKind(), req.GetName(), req.GetRelay(), req.GetOptionsJson(), req.GetAck())
	if err != nil {
		return &pluginpb.OpenSourceResponse{Error: errorToPB(err)}, nil
	}
	src, err := def.Factory(ctx, spec)
	if err == nil && src == nil {
		err = fmt.Errorf("%s %s: the factory returned no source", req.GetKind(), req.GetName())
	}
	if err != nil {
		return &pluginpb.OpenSourceResponse{Error: errorToPB(err)}, nil
	}
	s.mu.Lock()
	h := s.handle("source")
	s.sources[h] = &openSource{kind: req.GetKind(), name: req.GetName(), src: src, log: s.log.With("source", req.GetName()), pending: map[string][]*envelope.Envelope{}}
	s.mu.Unlock()
	s.log.Debug("source opened", "kind", req.GetKind(), "name", req.GetName(), "handle", h)
	return &pluginpb.OpenSourceResponse{Handle: h}, nil
}

func (s *server) source(h string) (*openSource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.sources[h]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no open source %q", h)
	}
	return o, nil
}

func (s *server) StartSource(req *pluginpb.HandleRequest, stream pluginpb.Plugin_StartSourceServer) error {
	o, err := s.source(req.GetHandle())
	if err != nil {
		return err
	}
	return o.stream(stream)
}

// stream runs the source for one StartSource call. A second call on the
// same handle stops the first one.
func (o *openSource) stream(stream pluginpb.Plugin_StartSourceServer) error {
	if err := o.stop(stream.Context()); err != nil {
		return status.FromContextError(err).Err()
	}
	o.run.Lock()
	defer o.run.Unlock()
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	ended := make(chan struct{})
	defer close(ended)
	o.mu.Lock()
	o.cancel, o.ended = cancel, ended
	o.mu.Unlock()

	out := make(chan *envelope.Envelope)
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				o.log.Error("source panicked", "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				done <- fmt.Errorf("source %s panicked: %v", o.name, r)
			}
		}()
		done <- o.src.Start(ctx, out)
	}()
	for {
		select {
		case env := <-out:
			if env == nil {
				continue
			}
			if err := o.send(ctx, stream, env); err != nil {
				cancel()
				o.drain(out, done)
				return err
			}
		case err := <-done:
			if err != nil && ctx.Err() == nil {
				return status.Error(codes.Aborted, err.Error())
			}
			return nil
		case <-ctx.Done():
			o.drain(out, done)
			return nil
		}
	}
}

// send streams one message and keeps it until hikyaku settles it.
func (o *openSource) send(ctx context.Context, stream pluginpb.Plugin_StartSourceServer, env *envelope.Envelope) error {
	if env.ID == "" {
		env.ID = envelope.NewID()
	}
	env.Source = o.name
	if env.Protocol == "" {
		env.Protocol = o.kind
	}
	pe := envelopeToPB(env)
	if proto.Size(pe) > maxMessageBytes-1024 {
		// It would come back forever: drop it.
		o.log.Error("message too large for hikyaku's plugin channel; dropped", "id", env.ID, "bytes", len(env.Body))
		env.Queued()
		env.Ack(ctx, envelope.Outcome{Status: envelope.Filtered, Err: errors.New("message too large")})
		return nil
	}
	o.mu.Lock()
	o.pending[env.ID] = append(o.pending[env.ID], env)
	o.mu.Unlock()
	if err := stream.Send(pe); err != nil {
		if e := o.take(env.ID); e != nil {
			e.Ack(context.WithoutCancel(ctx), envelope.Outcome{Status: envelope.Failed, Err: err})
		}
		return err
	}
	return nil
}

// drain waits for Start to return after its ctx ended, failing (so the
// source redelivers) whatever it still hands over.
func (o *openSource) drain(out <-chan *envelope.Envelope, done <-chan error) {
	for {
		select {
		case env := <-out:
			if env != nil {
				env.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed, Err: errors.New("hikyaku stopped the stream")})
			}
		case err := <-done:
			if err != nil {
				o.log.Debug("source stopped", "error", err)
			}
			return
		}
	}
}

// stop ends the running stream, if any, and waits for it: until ctx ends,
// should the source's Start not return.
func (o *openSource) stop(ctx context.Context) error {
	o.mu.Lock()
	cancel, ended := o.cancel, o.ended
	o.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-ended:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// take removes the oldest unsettled message with an ID.
func (o *openSource) take(id string) *envelope.Envelope {
	o.mu.Lock()
	defer o.mu.Unlock()
	envs := o.pending[id]
	if len(envs) == 0 {
		return nil
	}
	env := envs[0]
	if len(envs) == 1 {
		delete(o.pending, id)
	} else {
		o.pending[id] = envs[1:]
	}
	return env
}

func (s *server) Settle(ctx context.Context, req *pluginpb.SettleRequest) (*pluginpb.SettleResponse, error) {
	o, err := s.source(req.GetHandle())
	if err != nil {
		return nil, err
	}
	env := o.take(req.GetEnvelopeId())
	if env == nil {
		return nil, status.Errorf(codes.NotFound, "source %s: no unsettled message %q", o.name, req.GetEnvelopeId())
	}
	settle(ctx, env, req.GetFinal())
	return &pluginpb.SettleResponse{}, nil
}

// settle hands hikyaku's outcome to the source's hooks: a final settle runs
// both the OnQueued and the OnAck hook (with Delivered), whichever the
// source registered; a failed one runs OnAck with Failed.
func settle(ctx context.Context, env *envelope.Envelope, final bool) {
	if !final {
		env.Ack(ctx, envelope.Outcome{Status: envelope.Failed})
		return
	}
	env.Queued()
	env.Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
}

func (s *server) SourceHealth(ctx context.Context, req *pluginpb.HandleRequest) (*pluginpb.HealthResponse, error) {
	o, err := s.source(req.GetHandle())
	if err != nil {
		return nil, err
	}
	return &pluginpb.HealthResponse{Error: errorToPB(o.src.Health(ctx))}, nil
}

func (s *server) CloseSource(ctx context.Context, req *pluginpb.HandleRequest) (*pluginpb.CloseResponse, error) {
	s.mu.Lock()
	o, ok := s.sources[req.GetHandle()]
	delete(s.sources, req.GetHandle())
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no open source %q", req.GetHandle())
	}
	s.log.Debug("source closed", "kind", o.kind, "name", o.name)
	return &pluginpb.CloseResponse{Error: errorToPB(o.close(ctx))}, nil
}

// close stops the stream, fails what is still unsettled (the source
// redelivers it) and closes the source.
func (o *openSource) close(ctx context.Context) error {
	if err := o.stop(ctx); err != nil {
		o.log.Warn("the source did not stop; closing it anyway", "error", err)
	}
	o.mu.Lock()
	pending := o.pending
	o.pending = map[string][]*envelope.Envelope{}
	o.mu.Unlock()
	for _, envs := range pending {
		for _, env := range envs {
			env.Ack(ctx, envelope.Outcome{Status: envelope.Failed, Err: errors.New("the source closed before hikyaku settled the message")})
		}
	}
	if c, ok := o.src.(plugin.Closer); ok {
		return c.Close(ctx)
	}
	return nil
}

// shutdown closes what hikyaku left open when it stopped the plugin.
func (s *server) shutdown() {
	s.mu.Lock()
	sinks, sources := s.sinks, s.sources
	s.sinks, s.sources = map[string]*openSink{}, map[string]*openSource{}
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	for _, o := range sources {
		if err := o.close(ctx); err != nil {
			s.log.Warn("closing source", "name", o.name, "error", err)
		}
	}
	for _, o := range sinks {
		if err := o.sink.Close(ctx); err != nil {
			s.log.Warn("closing destination", "name", o.name, "error", err)
		}
	}
}
