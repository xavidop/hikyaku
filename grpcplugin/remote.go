package grpcplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// Defaults of a plugin source's restart policy (reconnect: in the spec)
// and of its largest message (max_body_bytes:).
const (
	defaultBackoff     = 500 * time.Millisecond
	defaultMaxBackoff  = 30 * time.Second
	defaultJitter      = "full"
	defaultMaxBodySize = 10 << 20
)

// openError is a sink or source the plugin could not build: bad options.
type openError struct{ err error }

func (e *openError) Error() string { return e.err.Error() }
func (e *openError) Unwrap() error { return e.err }

// remote is one handle in the plugin: a source or a destination of the
// spec. A relaunched process does not know the handles of the one before
// it, so the next call re-opens the handle there.
type remote struct {
	p    *Plugin
	role Role
	kind string
	name string
	// open opens the handle; close closes it. Both run with mu held.
	open  func(ctx context.Context, c pluginpb.PluginClient) (string, error)
	close func(ctx context.Context, c pluginpb.PluginClient, handle string) (*pluginpb.CloseResponse, error)

	mu     sync.Mutex
	handle string
	gen    uint64
	closed bool
	batch  bool // the sink takes DeliverBatch (set by open)
}

// get returns what to call: the client of the running process (relaunched
// if it died) and this remote's handle in it (re-opened if the process is
// new).
func (r *remote) get(ctx context.Context) (pluginpb.PluginClient, string, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, "", 0, plugin.Permanentf("%s %s is closed", r.role, r.name)
	}
	c, gen, err := r.p.conn(ctx)
	if err != nil {
		return nil, "", 0, err
	}
	if r.handle != "" && r.gen == gen {
		return c, r.handle, gen, nil
	}
	reopen := r.handle != ""
	h, err := r.open(ctx, c)
	if err != nil {
		var oe *openError
		if errors.As(err, &oe) {
			return nil, "", 0, &plugin.Error{Class: plugin.Transient, Tags: []string{plugin.TagConnect}, Err: oe}
		}
		return nil, "", 0, r.p.callError(ctx, gen, err)
	}
	r.p.healthy(gen)
	if reopen {
		r.p.log.Info("plugin handle re-opened", r.role.String(), r.name)
	}
	r.handle, r.gen = h, gen
	return c, h, gen, nil
}

// fail classifies a failed call on the handle. An unknown handle (the
// plugin lost it) is re-opened by the next call.
func (r *remote) fail(ctx context.Context, gen uint64, err error) error {
	if status.Code(err) == codes.NotFound {
		r.mu.Lock()
		if r.gen == gen {
			r.handle = ""
		}
		r.mu.Unlock()
		return &plugin.Error{Class: plugin.Transient, Tags: []string{plugin.TagConnect}, Err: fmt.Errorf("plugin %s: %s", r.p.name, status.Convert(err).Message())}
	}
	return r.p.callError(ctx, gen, err)
}

// shut closes the handle in the process that holds it, if it still runs.
func (r *remote) shut(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.p.forget(r)
	c, gen, ok := r.p.current()
	if !ok || gen != r.gen || r.handle == "" {
		return nil // the process that held the handle is gone
	}
	resp, err := r.close(ctx, c, r.handle)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return r.p.callError(ctx, gen, err)
	}
	return failure(resp.GetError())
}

// newRemote opens a handle for a spec entry; the factory's error when the
// plugin rejects it.
func (p *Plugin) newRemote(ctx context.Context, r *remote) error {
	if !p.track(r) {
		return fmt.Errorf("plugin %s is closed", p.name)
	}
	if _, _, _, err := r.get(ctx); err != nil {
		p.forget(r)
		var oe *openError
		if errors.As(err, &oe) {
			return fmt.Errorf("plugin %s: %w", p.name, oe.err)
		}
		return err
	}
	return nil
}

func optionsJSON(spec plugin.Spec) (string, error) {
	if len(spec.Options) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(spec.Options)
	if err != nil {
		return "", fmt.Errorf("%s %s: options: %w", spec.Kind, spec.Name, err)
	}
	return string(b), nil
}

// sinkFactory builds the destinations of a kind: a handle in the plugin
// behind a proxy.
func (p *Plugin) sinkFactory(kind string) plugin.Factory[plugin.Sink] {
	return func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		opts, err := optionsJSON(spec)
		if err != nil {
			return nil, err
		}
		r := &remote{p: p, role: RoleSink, kind: kind, name: spec.Name}
		r.open = func(ctx context.Context, c pluginpb.PluginClient) (string, error) {
			resp, err := c.OpenSink(ctx, &pluginpb.OpenSinkRequest{Kind: kind, Name: spec.Name, Relay: spec.Env.Relay, OptionsJson: opts})
			if err != nil {
				return "", err
			}
			if e := resp.GetError(); e != nil {
				return "", &openError{failure(e)}
			}
			if resp.GetHandle() == "" {
				return "", &openError{errors.New("the plugin opened no handle")}
			}
			r.batch = resp.GetBatch()
			return resp.GetHandle(), nil
		}
		r.close = func(ctx context.Context, c pluginpb.PluginClient, h string) (*pluginpb.CloseResponse, error) {
			return c.CloseSink(ctx, &pluginpb.HandleRequest{Handle: h})
		}
		if err := p.newRemote(ctx, r); err != nil {
			return nil, err
		}
		s := &remoteSink{r: r}
		if r.batch {
			return &remoteBatchSink{s}, nil
		}
		return s, nil
	}
}

// remoteSink is a destination served by a plugin.
type remoteSink struct {
	r *remote
	// unbatched: the plugin answered DeliverBatch with UNIMPLEMENTED after
	// all, so batches go one message at a time.
	unbatched atomic.Bool
}

func (s *remoteSink) Name() string { return s.r.name }

// Deliver makes one attempt in the plugin.
func (s *remoteSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	c, h, gen, err := s.r.get(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := c.Deliver(ctx, &pluginpb.DeliverRequest{Handle: h, Envelope: envelopeToPB(env)})
	if err != nil {
		return nil, s.r.fail(ctx, gen, err)
	}
	s.r.p.healthy(gen)
	return outcome(resp)
}

func outcome(resp *pluginpb.DeliverResponse) (*plugin.Result, error) {
	if e := resp.GetError(); e != nil {
		return nil, errorFromPB(e)
	}
	return resultFromPB(resp.GetResult()), nil
}

// Health asks the plugin's sink.
func (s *remoteSink) Health(ctx context.Context) error {
	c, h, gen, err := s.r.get(ctx)
	if err != nil {
		return err
	}
	resp, err := c.SinkHealth(ctx, &pluginpb.HandleRequest{Handle: h})
	if err != nil {
		return s.r.fail(ctx, gen, err)
	}
	return failure(resp.GetError())
}

// Close flushes and releases the plugin's sink.
func (s *remoteSink) Close(ctx context.Context) error { return s.r.shut(ctx) }

// oneByOne delivers a batch with single deliveries; the batch fails as a
// whole on the first failure, as a native batch call would.
func (s *remoteSink) oneByOne(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	var last *plugin.Result
	for _, e := range envs {
		res, err := s.Deliver(ctx, e)
		if err != nil {
			return nil, err
		}
		last = res
	}
	return last, nil
}

// remoteBatchSink is a remoteSink whose plugin sink batches natively.
type remoteBatchSink struct{ *remoteSink }

// DeliverBatch delivers the messages in one call to the plugin.
func (s *remoteBatchSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	if len(envs) == 0 {
		return nil, nil
	}
	if s.unbatched.Load() {
		return s.oneByOne(ctx, envs)
	}
	c, h, gen, err := s.r.get(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := c.DeliverBatch(ctx, &pluginpb.DeliverBatchRequest{Handle: h, Envelopes: envelopesToPB(envs)})
	if status.Code(err) == codes.Unimplemented {
		s.unbatched.Store(true)
		s.r.p.log.Warn("plugin sink does not batch after all; delivering batches one message at a time", "destination", s.r.name)
		return s.oneByOne(ctx, envs)
	}
	if err != nil {
		return nil, s.r.fail(ctx, gen, err)
	}
	s.r.p.healthy(gen)
	return outcome(resp)
}

// restart is a plugin source's restart policy: reconnect: in the spec.
type restart struct {
	base, max   time.Duration
	jitter      string
	maxAttempts int
}

func restartPolicy(rc *config.Reconnect) restart {
	r := restart{base: defaultBackoff, max: defaultMaxBackoff, jitter: defaultJitter}
	if rc == nil {
		return r
	}
	if rc.Backoff > 0 {
		r.base = rc.Backoff.Std()
	}
	if rc.MaxBackoff > 0 {
		r.max = rc.MaxBackoff.Std()
	}
	if rc.Jitter != "" {
		r.jitter = rc.Jitter
	}
	r.maxAttempts = rc.MaxAttempts
	return r
}

// sourceFactory builds the sources of a kind: a handle in the plugin
// behind a proxy.
func (p *Plugin) sourceFactory(kind string) plugin.Factory[plugin.Source] {
	return func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		opts, err := optionsJSON(spec)
		if err != nil {
			return nil, err
		}
		s := &remoteSource{
			ack:     AckMode(spec),
			maxBody: defaultMaxBodySize,
			restart: restartPolicy(nil),
			log:     p.log.With("source", spec.Name),
			rnd:     rand.Float64,
		}
		if src, ok := spec.Config.(*config.Source); ok {
			if src.MaxBodyBytes > 0 {
				s.maxBody = int64(src.MaxBodyBytes)
			}
			s.restart = restartPolicy(src.Reconnect)
		}
		r := &remote{p: p, role: RoleSource, kind: kind, name: spec.Name}
		r.open = func(ctx context.Context, c pluginpb.PluginClient) (string, error) {
			resp, err := c.OpenSource(ctx, &pluginpb.OpenSourceRequest{Kind: kind, Name: spec.Name, Relay: spec.Env.Relay, OptionsJson: opts, Ack: s.ack})
			if err != nil {
				return "", err
			}
			if e := resp.GetError(); e != nil {
				return "", &openError{failure(e)}
			}
			if resp.GetHandle() == "" {
				return "", &openError{errors.New("the plugin opened no handle")}
			}
			return resp.GetHandle(), nil
		}
		r.close = func(ctx context.Context, c pluginpb.PluginClient, h string) (*pluginpb.CloseResponse, error) {
			return c.CloseSource(ctx, &pluginpb.HandleRequest{Handle: h})
		}
		s.r = r
		if err := p.newRemote(ctx, r); err != nil {
			return nil, err
		}
		return s, nil
	}
}

// remoteSource is a source served by a plugin. It streams the plugin's
// messages, settles each one in the plugin with the relay's outcome, and
// restarts the stream (relaunching the plugin) when the connection drops.
type remoteSource struct {
	r       *remote
	ack     string
	maxBody int64
	restart restart
	log     *slog.Logger
	rnd     func() float64
}

// sourceAborted is a plugin source that said it cannot go on.
type sourceAborted struct{ err error }

func (e *sourceAborted) Error() string { return e.err.Error() }

func (s *remoteSource) Name() string { return s.r.name }

// Start streams until ctx ends (nil), the plugin's source runs out (nil),
// or it cannot go on (its error). A dropped connection restarts the stream
// with backoff, relaunching the plugin; reconnect.max_attempts failures in
// a row give up.
func (s *remoteSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	failures := 0
	for {
		got, err := s.stream(ctx, out)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return nil
		}
		var aborted *sourceAborted
		if errors.As(err, &aborted) {
			return fmt.Errorf("source %s: %w", s.r.name, aborted.err)
		}
		if got {
			failures = 0
		}
		failures++
		if s.restart.maxAttempts > 0 && failures > s.restart.maxAttempts {
			return fmt.Errorf("source %s: giving up after %d failed restarts: %w", s.r.name, s.restart.maxAttempts, err)
		}
		wait := backoff.Delay(s.restart.base, s.restart.max, s.restart.jitter, failures, s.rnd)
		s.log.Warn("plugin source stream ended; restarting", "error", err, "in", wait)
		if !backoff.Sleep(ctx, wait) {
			return nil
		}
	}
}

// stream runs one StartSource stream; got reports whether it carried a
// message.
func (s *remoteSource) stream(ctx context.Context, out chan<- *envelope.Envelope) (got bool, err error) {
	c, h, gen, err := s.r.get(ctx)
	if err != nil {
		return false, err
	}
	st, err := c.StartSource(ctx, &pluginpb.HandleRequest{Handle: h})
	if err != nil {
		return false, s.r.fail(ctx, gen, err)
	}
	for {
		pe, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return got, nil
		}
		if err != nil {
			if status.Code(err) == codes.Aborted {
				return got, &sourceAborted{errors.New(status.Convert(err).Message())}
			}
			return got, s.r.fail(ctx, gen, err)
		}
		if !got {
			got = true
			s.r.p.healthy(gen)
		}
		env, aerr := envelopeFromPB(pe)
		if aerr != nil {
			s.log.Warn("plugin message attributes dropped", "id", pe.GetId(), "error", aerr)
		}
		id := pe.GetId() // what the plugin knows the message by
		if env.ID == "" {
			env.ID = envelope.NewID()
		}
		env.Source = s.r.name // routes match the spec's source name
		if env.Protocol == "" {
			env.Protocol = s.r.kind
		}
		if env.ReceivedAt.IsZero() {
			env.ReceivedAt = time.Now().UTC()
		}
		if int64(len(env.Body)) > s.maxBody {
			// It would come back forever: drop it and let the plugin forget it.
			s.log.Error("message over max_body_bytes dropped", "id", env.ID, "bytes", len(env.Body), "max_body_bytes", s.maxBody)
			s.settle(ctx, h, gen, id, true)
			continue
		}
		plugin.OnSettle(env, s.ack, func(ctx context.Context, final bool) { s.settle(ctx, h, gen, id, final) })
		select {
		case out <- env:
		case <-ctx.Done():
			return got, nil // unsettled: the plugin redelivers it
		}
	}
}

// settle reports the relay's outcome to the process that sent the
// message. A message from a process that has died since is not settled:
// that process's upstream redelivers it.
func (s *remoteSource) settle(ctx context.Context, handle string, gen uint64, id string, final bool) {
	c, cur, ok := s.r.p.current()
	if !ok || cur != gen {
		s.log.Debug("settle skipped: the plugin process that sent the message is gone", "id", id, "final", final)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	if _, err := c.Settle(ctx, &pluginpb.SettleRequest{Handle: handle, EnvelopeId: id, Final: final}); err != nil {
		s.log.Warn("plugin settle failed", "id", id, "final", final, "error", s.r.p.callError(ctx, gen, err))
	}
}

// Health asks the plugin's source.
func (s *remoteSource) Health(ctx context.Context) error {
	c, h, gen, err := s.r.get(ctx)
	if err != nil {
		return err
	}
	resp, err := c.SourceHealth(ctx, &pluginpb.HandleRequest{Handle: h})
	if err != nil {
		return s.r.fail(ctx, gen, err)
	}
	return failure(resp.GetError())
}

// Close releases the plugin's source once the relay has settled its
// messages (plugin.Closer).
func (s *remoteSource) Close(ctx context.Context) error { return s.r.shut(ctx) }

// Interface checks.
var (
	_ plugin.Sink      = (*remoteSink)(nil)
	_ plugin.BatchSink = (*remoteBatchSink)(nil)
	_ plugin.Source    = (*remoteSource)(nil)
	_ plugin.Closer    = (*remoteSource)(nil)
)
