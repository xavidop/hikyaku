package gcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	vkit "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// Pub/Sub's attribute limits; a forwarded header beyond them is skipped
// rather than failing the publish.
const (
	maxAttrKey   = 256
	maxAttrValue = 1024
)

// dropHeaders are not forwarded as attributes or metadata: hop-by-hop
// headers, framing, and the caller's own credentials (as for http
// destinations).
var dropHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Host": true,
	"Content-Length": true, "Authorization": true, "Cookie": true, "Accept-Encoding": true,
}

// resultHeader carries the server's message IDs in a delivery's result.
const resultHeader = "pubsub-message-id"

// PubSubSink is a pubsub destination. It keeps one publisher per topic.
type PubSubSink struct {
	name   string
	opts   pubsubSinkOptions
	client *pubsub.Client
	log    *slog.Logger

	mu     sync.Mutex
	pubs   map[string]*pubsub.Publisher
	paused map[pausedKey]bool
	closed bool
}

// pausedKey is an ordering key whose publishing paused after a failure.
type pausedKey struct{ topic, key string }

// noRetry makes a publish one attempt: the relay owns retries.
type noRetry struct{}

func (noRetry) Retry(error) (time.Duration, bool) { return 0, false }

func newPubSubSink(ctx context.Context, spec plugin.Spec) (*PubSubSink, error) {
	o, err := parsePubSubSink(spec)
	if err != nil {
		return nil, err
	}
	cfg := &pubsub.ClientConfig{TopicAdminCallOptions: &vkit.TopicAdminCallOptions{
		Publish: []gax.CallOption{gax.WithRetry(func() gax.Retryer { return noRetry{} })},
	}}
	client, err := pubsub.NewClientWithConfig(ctx, o.client.project, cfg, o.client.opts...)
	if err != nil {
		return nil, fmt.Errorf("destination %s: pubsub client: %w", spec.Name, err)
	}
	return &PubSubSink{
		name: spec.Name, opts: o, client: client, log: spec.Env.Log(),
		pubs: map[string]*pubsub.Publisher{}, paused: map[pausedKey]bool{},
	}, nil
}

// Name is the destination name.
func (s *PubSubSink) Name() string { return s.name }

// Deliver publishes one message and waits for the server's ID.
func (s *PubSubSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	topic, msg, err := s.message(env)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, pubsubError(topic, err) // too late: do not publish a copy that lands later
	}
	p, err := s.publisher(topic, msg.OrderingKey)
	if err != nil {
		return nil, err
	}
	id, err := p.Publish(ctx, msg).Get(ctx)
	if err != nil {
		s.pause(topic, msg.OrderingKey)
		return nil, pubsubError(topic, err)
	}
	return &plugin.Result{Headers: map[string][]string{resultHeader: {id}}, Endpoint: topic}, nil
}

// DeliverBatch publishes every message, then waits for them all. Every
// message is rendered first, so a template error publishes none of them.
// The batch fails when one message does, with that message's error;
// Idempotency-Key lets consumers drop the copies a retry publishes again.
func (s *PubSubSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	type pending struct {
		topic string
		msg   *pubsub.Message
		res   *pubsub.PublishResult
	}
	if len(envs) == 0 {
		return &plugin.Result{}, nil
	}
	all := make([]pending, len(envs))
	for i, env := range envs {
		topic, msg, err := s.message(env)
		if err != nil {
			return nil, err
		}
		all[i] = pending{topic: topic, msg: msg}
	}
	if err := ctx.Err(); err != nil {
		return nil, pubsubError(all[0].topic, err)
	}
	for i := range all {
		p, err := s.publisher(all[i].topic, all[i].msg.OrderingKey)
		if err != nil {
			return nil, err
		}
		all[i].res = p.Publish(ctx, all[i].msg)
	}
	ids := make([]string, 0, len(all))
	var first error
	failed := 0
	for _, p := range all {
		id, err := p.res.Get(ctx)
		if err != nil {
			s.pause(p.topic, p.msg.OrderingKey)
			failed++
			if first == nil {
				first = pubsubError(p.topic, err)
			}
			continue
		}
		ids = append(ids, id)
	}
	if first != nil {
		var pe *plugin.Error
		if errors.As(first, &pe) {
			return nil, &plugin.Error{Class: pe.Class, RetryAfter: pe.RetryAfter, Tags: pe.Tags,
				Err: fmt.Errorf("%d of %d messages failed: %w", failed, len(all), pe.Err)}
		}
		return nil, first
	}
	return &plugin.Result{Headers: map[string][]string{resultHeader: ids}, Endpoint: all[0].topic}, nil
}

// message renders the topic and builds the message of an envelope.
func (s *PubSubSink) message(env *envelope.Envelope) (string, *pubsub.Message, error) {
	topic := s.opts.static
	if topic == "" {
		t, err := s.opts.topic.Render(env)
		if err != nil {
			return "", nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		if topic, _, err = resourceName("topic", t, "topics", s.opts.client.project); err != nil {
			return "", nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
	}
	msg := &pubsub.Message{Data: env.Body}
	if s.opts.body != nil {
		b, err := s.opts.body.Render(env)
		if err != nil {
			return "", nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		msg.Data = []byte(b)
	}
	if s.opts.key != nil {
		k, err := s.opts.key.Render(env)
		if err != nil {
			return "", nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		msg.OrderingKey = k
	}
	attrs, err := s.attributes(env)
	if err != nil {
		return "", nil, err
	}
	msg.Attributes = attrs
	return topic, msg, nil
}

// attributes are the envelope's headers (but the dropped ones, the ones
// written below and those Pub/Sub would refuse), then Idempotency-Key,
// content-type and subject, then the headers: block, which wins.
func (s *PubSubSink) attributes(env *envelope.Envelope) (map[string]string, error) {
	attrs := make(map[string]string, len(env.Headers)+len(s.opts.headers)+3)
	for k, vs := range env.Headers {
		if len(vs) == 0 || !forwarded(k) || hasPrefixFold(k, "goog") {
			continue
		}
		v := strings.Join(vs, ", ")
		if len(k) > maxAttrKey || len(v) > maxAttrValue {
			continue
		}
		attrs[k] = v
	}
	key := env.Header(attrIdempotencyKey)
	if key == "" {
		key = env.ID
	}
	attrs[attrIdempotencyKey] = key
	if env.ContentType != "" {
		attrs[attrContentType] = env.ContentType
	}
	if env.Subject != "" {
		attrs[attrSubject] = env.Subject
	}
	for k, t := range s.opts.headers {
		v, err := t.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		attrs[k] = v
	}
	return attrs, nil
}

// forwarded reports whether a header is copied onto the message: not a
// dropped one, and not one the destination writes itself.
func forwarded(k string) bool {
	return !dropHeaders[textproto.CanonicalMIMEHeaderKey(k)] &&
		!equalFold(k, attrIdempotencyKey) && !equalFold(k, attrContentType) && !equalFold(k, attrSubject)
}

// publisher returns the topic's publisher, created on first use. An ordering
// key that paused after a failure is resumed first, so this attempt can
// publish.
func (s *PubSubSink) publisher(topic, key string) (*pubsub.Publisher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	p, ok := s.pubs[topic]
	if !ok {
		p = s.client.Publisher(topic)
		p.EnableMessageOrdering = s.opts.key != nil
		s.pubs[topic] = p
	}
	if key != "" && s.paused[pausedKey{topic, key}] {
		p.ResumePublish(key)
		delete(s.paused, pausedKey{topic, key})
	}
	return p, nil
}

// pause remembers that publishing with an ordering key failed, so the
// publisher refuses that key until the next attempt resumes it.
func (s *PubSubSink) pause(topic, key string) {
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused[pausedKey{topic, key}] = true
}

// Health checks that the topic exists. A templated topic is known only per
// message, so it is not checked; neither is a topic the credentials may
// publish to but not read (roles/pubsub.publisher lacks pubsub.topics.get).
func (s *PubSubSink) Health(ctx context.Context) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errClosed
	}
	if s.opts.static == "" {
		return nil
	}
	_, err := s.client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: s.opts.static})
	return existence("topic", s.opts.static, err)
}

// existence turns the error of a get into a health: nil when the resource
// exists, or when the credentials may use it but not read it.
func existence(what, name string, err error) error {
	switch status.Code(err) {
	case codes.OK, codes.PermissionDenied:
		return nil
	case codes.NotFound:
		return fmt.Errorf("%s %s does not exist", what, name)
	}
	return fmt.Errorf("%s %s: %w", what, name, err)
}

// Close flushes and stops every publisher, then closes the client.
func (s *PubSubSink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	pubs := make([]*pubsub.Publisher, 0, len(s.pubs))
	for _, p := range s.pubs {
		pubs = append(pubs, p)
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for _, p := range pubs {
			wg.Go(p.Stop)
		}
		wg.Wait()
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = fmt.Errorf("pubsub destination %s: publishes still pending at close: %w", s.name, ctx.Err())
	}
	if cerr := s.client.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}
