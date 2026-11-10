package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// Result headers.
const (
	// HeaderEntryID carries the ID Redis gave the stream entry (one value
	// per message of a batch, in order).
	HeaderEntryID = "redis-entry-id"
	// HeaderReceivers carries how many subscribers got the message (one
	// value per message of a batch, in order).
	HeaderReceivers = "redis-receivers"
)

// Sink is a redis destination: it appends each message to a stream (XADD)
// or publishes it to a channel (PUBLISH).
type Sink struct {
	name   string
	opts   sinkOptions
	client *goredis.Client
}

func newSink(_ context.Context, spec plugin.Spec) (*Sink, error) {
	opts, err := parseSink(spec)
	if err != nil {
		return nil, err
	}
	return &Sink{name: spec.Name, opts: opts, client: opts.server.client()}, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver makes one attempt: XADD to the stream, or PUBLISH to the
// channel. A publish no subscriber received still succeeds.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if s.opts.stream != "" {
		args, err := s.xadd(env)
		if err != nil {
			return nil, err
		}
		id, err := s.client.XAdd(ctx, args).Result()
		if err != nil {
			return nil, classify(ctx, fmt.Errorf("XADD %s: %w", s.opts.stream, err))
		}
		return &plugin.Result{Endpoint: s.endpoint("XADD", s.opts.stream), Headers: map[string][]string{HeaderEntryID: {id}}}, nil
	}
	channel, body, err := s.publication(env)
	if err != nil {
		return nil, err
	}
	n, err := s.client.Publish(ctx, channel, body).Result()
	if err != nil {
		return nil, classify(ctx, fmt.Errorf("PUBLISH %s: %w", channel, err))
	}
	return &plugin.Result{Endpoint: s.endpoint("PUBLISH", channel), Headers: map[string][]string{HeaderReceivers: {strconv.FormatInt(n, 10)}}}, nil
}

// DeliverBatch writes a batch in one round trip: the XADDs in one
// MULTI/EXEC, so a connection lost before EXEC writes none of them, or the
// PUBLISHes in one pipeline. Any failure fails the whole batch; a retry may
// then append entries already written, which their id field (the envelope
// ID) lets a reader dedupe.
func (s *Sink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	if s.opts.stream != "" {
		return s.xaddBatch(ctx, envs)
	}
	return s.publishBatch(ctx, envs)
}

func (s *Sink) xaddBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	args := make([]*goredis.XAddArgs, len(envs))
	for i, env := range envs {
		a, err := s.xadd(env)
		if err != nil {
			return nil, err
		}
		args[i] = a
	}
	cmds := make([]*goredis.StringCmd, len(args))
	_, err := s.client.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		for i, a := range args {
			cmds[i] = p.XAdd(ctx, a)
		}
		return nil
	})
	if err := firstError(err, cmds); err != nil {
		return nil, classify(ctx, fmt.Errorf("XADD %s (batch of %d): %w", s.opts.stream, len(envs), err))
	}
	ids := make([]string, len(cmds))
	for i, c := range cmds {
		ids[i] = c.Val()
	}
	return &plugin.Result{Endpoint: s.endpoint("XADD", s.opts.stream), Headers: map[string][]string{HeaderEntryID: ids}}, nil
}

func (s *Sink) publishBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	type publication struct {
		channel string
		body    []byte
	}
	pubs := make([]publication, len(envs))
	for i, env := range envs {
		channel, body, err := s.publication(env)
		if err != nil {
			return nil, err
		}
		pubs[i] = publication{channel, body}
	}
	cmds := make([]*goredis.IntCmd, len(pubs))
	_, err := s.client.Pipelined(ctx, func(p goredis.Pipeliner) error {
		for i, pub := range pubs {
			cmds[i] = p.Publish(ctx, pub.channel, pub.body)
		}
		return nil
	})
	if err := firstError(err, cmds); err != nil {
		return nil, classify(ctx, fmt.Errorf("PUBLISH (batch of %d): %w", len(envs), err))
	}
	counts := make([]string, len(cmds))
	for i, c := range cmds {
		counts[i] = strconv.FormatInt(c.Val(), 10)
	}
	endpoint := s.endpoint("PUBLISH", "")
	if len(pubs) > 0 && s.opts.channel.IsStatic() {
		endpoint = s.endpoint("PUBLISH", pubs[0].channel)
	}
	return &plugin.Result{Endpoint: endpoint, Headers: map[string][]string{HeaderReceivers: counts}}, nil
}

// firstError is the first command's own error, which names what went wrong
// better than the pipeline's (EXECABORT, or the first error again).
func firstError[C interface{ Err() error }](err error, cmds []C) error {
	for _, c := range cmds {
		if cerr := c.Err(); cerr != nil {
			return cerr
		}
	}
	return err
}

// xadd builds the XADD of one message.
func (s *Sink) xadd(env *envelope.Envelope) (*goredis.XAddArgs, error) {
	body, err := s.body(env)
	if err != nil {
		return nil, err
	}
	var extra map[string]string
	if len(s.opts.headers) > 0 {
		extra = make(map[string]string, len(s.opts.headers))
		for k, t := range s.opts.headers {
			v, err := t.Render(env)
			if err != nil {
				return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, fmt.Errorf("headers.%s: %w", k, err))
			}
			extra[k] = v
		}
	}
	return &goredis.XAddArgs{
		Stream: s.opts.stream,
		MaxLen: s.opts.maxLen,
		Approx: s.opts.maxLen > 0,
		ID:     "*",
		Values: encodeEntry(env, body, extra),
	}, nil
}

// publication is the channel and the body of one PUBLISH.
func (s *Sink) publication(env *envelope.Envelope) (string, []byte, error) {
	channel, err := s.opts.channel.Render(env)
	if err != nil {
		return "", nil, plugin.Wrap(plugin.Permanent, []string{"template"}, fmt.Errorf("channel: %w", err))
	}
	if channel == "" {
		return "", nil, plugin.Wrap(plugin.Permanent, []string{"template"}, errors.New("channel rendered empty"))
	}
	body, err := s.body(env)
	return channel, body, err
}

// body is the message body, or the body template rendered.
func (s *Sink) body(env *envelope.Envelope) ([]byte, error) {
	if s.opts.body == nil {
		return env.Body, nil
	}
	b, err := s.opts.body.Render(env)
	if err != nil {
		return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, fmt.Errorf("body: %w", err))
	}
	return []byte(b), nil
}

// endpoint names what served a delivery, without the URL's password.
func (s *Sink) endpoint(verb, key string) string {
	if key == "" {
		return s.opts.server.shown + " " + verb
	}
	return s.opts.server.shown + " " + verb + " " + key
}

// Health pings the server.
func (s *Sink) Health(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// Close closes the connections. Every delivery is synchronous, so nothing
// is left to flush.
func (s *Sink) Close(context.Context) error {
	if err := s.client.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
		return err
	}
	return nil
}
