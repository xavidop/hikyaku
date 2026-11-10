package kafka

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Result headers of a delivery.
const (
	resultPartition = "kafka-partition"
	resultOffset    = "kafka-offset"
)

// defaultFlushTimeout bounds the flush at Close.
const defaultFlushTimeout = 3 * time.Second

// Sink is a kafka destination: one producer.
type Sink struct {
	name     string
	opts     sinkOptions
	brokers  string
	cl       *kgo.Client
	throttle *throttle
	watch    *connWatch
	// flushTimeout bounds Close's flush: the relay's shutdown budget is
	// shared, and a record still held at Close has no delivery waiting
	// for it.
	flushTimeout time.Duration

	// ctx is every record's context; it ends at Close. A delivery stops
	// waiting when its own context ends but never cancels its record:
	// with franz-go, a cancelled record fails every record buffered for
	// its partition, other deliveries' included.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // produce calls in flight

	mu     sync.Mutex
	closed bool
}

var (
	_ plugin.Sink      = (*Sink)(nil)
	_ plugin.BatchSink = (*Sink)(nil)
)

func newSink(_ context.Context, spec plugin.Spec) (*Sink, error) {
	opts, err := parseSink(spec)
	if err != nil {
		return nil, err
	}
	th, watch := &throttle{}, newConnWatch(nil)
	kopts := append(opts.client.kgoOpts(opts.brokers, spec.Env.Log()), opts.producerOpts()...)
	cl, err := kgo.NewClient(append(kopts, kgo.WithHooks(th, watch))...)
	if err != nil {
		return nil, fail("destination", spec.Name, "%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Sink{
		name: spec.Name, opts: opts, brokers: strings.Join(opts.brokers, ","),
		cl: cl, throttle: th, watch: watch, flushTimeout: defaultFlushTimeout, ctx: ctx, cancel: cancel,
	}, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver produces the message as one record and waits for the brokers to
// acknowledge it (acks).
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	rec, err := s.record(env)
	if err != nil {
		return nil, err
	}
	errs, err := s.produce(ctx, []*kgo.Record{rec})
	if err != nil {
		return nil, err
	}
	if errs[0] != nil {
		return nil, classify(fmt.Errorf("produce to %s: %w", rec.Topic, errs[0]), s.throttle.remaining())
	}
	return s.result([]*kgo.Record{rec}), nil
}

// DeliverBatch produces every message of a batch in one call and waits for
// all of them. One failed record fails the batch: the relay retries it
// whole, and the Idempotency-Key header lets consumers drop the records
// written twice.
func (s *Sink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	recs := make([]*kgo.Record, len(envs))
	for i, env := range envs {
		rec, err := s.record(env)
		if err != nil {
			return nil, fmt.Errorf("message %d of %d (%s): %w", i+1, len(envs), env.ID, err)
		}
		recs[i] = rec
	}
	if len(recs) == 0 {
		return s.result(nil), nil
	}
	errs, err := s.produce(ctx, recs)
	if err != nil {
		return nil, err
	}
	for i, perr := range errs {
		if perr != nil {
			return nil, classify(fmt.Errorf("produce message %d of %d (%s) to %s: %w", i+1, len(recs), envs[i].ID, recs[i].Topic, perr), s.throttle.remaining())
		}
	}
	return s.result(recs), nil
}

// produce hands the records to the producer and waits for all of them, or
// for ctx. It returns each record's error, in order. The producer retries
// a refused connection on its own; when the brokers refuse the credentials
// or the certificate, produce says so (a permanent failure) rather than
// wait for ctx.
func (s *Sink) produce(ctx context.Context, recs []*kgo.Record) ([]error, error) {
	what := "produce to " + recs[0].Topic
	if ctx.Err() != nil {
		return nil, ctxError(ctx, what, s.throttle.remaining())
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, classify(errClosed, 0)
	}
	s.wg.Add(1)
	s.mu.Unlock()
	refusal, _ := s.watch.refused()
	done := make(chan []error, 1)
	go func() {
		defer s.wg.Done()
		results := s.cl.ProduceSync(s.ctx, recs...)
		byRecord := make(map[*kgo.Record]error, len(results))
		for _, r := range results {
			byRecord[r.Record] = r.Err
		}
		errs := make([]error, len(recs))
		for i, r := range recs {
			errs[i] = byRecord[r]
		}
		done <- errs
	}()
	// Once this returns early, the records stay with the producer, which
	// may still write them (for up to 30s): a retry can write a message
	// twice, and consumers drop the copy by its Idempotency-Key.
	for {
		select {
		case errs := <-done:
			return errs, nil
		case <-refusal:
			var err error
			if refusal, err = s.watch.refused(); err != nil {
				return nil, classify(fmt.Errorf("%s: %w", what, err), 0)
			}
		case <-ctx.Done():
			if _, err := s.watch.refused(); err != nil {
				return nil, classify(fmt.Errorf("%s: %w", what, err), 0)
			}
			return nil, ctxError(ctx, what, s.throttle.remaining())
		}
	}
}

// record builds the record of a message: topic, key, body and headers. A
// template that fails to render fails the message permanently.
func (s *Sink) record(env *envelope.Envelope) (*kgo.Record, error) {
	render := func(what string, t *transform.Template) (string, error) {
		v, err := t.Render(env)
		if err != nil {
			return "", plugin.Wrap(plugin.Permanent, []string{tagTemplate}, fmt.Errorf("%s: %w", what, err))
		}
		return v, nil
	}
	topic, err := render("topic", s.opts.topic)
	if err != nil {
		return nil, err
	}
	if err := validTopic(topic); err != nil {
		return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, fmt.Errorf("topic %v", err))
	}
	rec := &kgo.Record{Topic: topic, Value: env.Body}
	if s.opts.body != nil {
		body, err := render("body", s.opts.body)
		if err != nil {
			return nil, err
		}
		rec.Value = []byte(body)
	}
	if s.opts.key != nil {
		key, err := render("key", s.opts.key)
		if err != nil {
			return nil, err
		}
		if key != "" {
			rec.Key = []byte(key)
		}
	}
	if rec.Headers, err = s.headers(env, render); err != nil {
		return nil, err
	}
	return rec, nil
}

// headers are a record's headers: the message's headers (minus credentials
// and hop-by-hop ones), then the configured ones, then Idempotency-Key and
// content-type. A configured header replaces any other of its name.
func (s *Sink) headers(env *envelope.Envelope, render func(string, *transform.Template) (string, error)) ([]kgo.RecordHeader, error) {
	configured := make(map[string]bool, len(s.opts.headers))
	for _, h := range s.opts.headers {
		configured[strings.ToLower(h.name)] = true
	}
	ours := func(name string) bool {
		l := strings.ToLower(name)
		return configured[l] || l == strings.ToLower(headerIdempotency) || l == headerContentType
	}
	var out []kgo.RecordHeader
	for _, k := range slices.Sorted(maps.Keys(env.Headers)) {
		if dropped(k) || ours(k) {
			continue
		}
		for _, v := range env.Headers[k] {
			out = append(out, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}
	for _, h := range s.opts.headers {
		v, err := render("headers."+h.name, h.tmpl)
		if err != nil {
			return nil, err
		}
		out = append(out, kgo.RecordHeader{Key: h.name, Value: []byte(v)})
	}
	if !configured[strings.ToLower(headerIdempotency)] {
		out = append(out, kgo.RecordHeader{Key: headerIdempotency, Value: []byte(idempotencyKey(env))})
	}
	if env.ContentType != "" && !configured[headerContentType] {
		out = append(out, kgo.RecordHeader{Key: headerContentType, Value: []byte(env.ContentType)})
	}
	return out, nil
}

// idempotencyKey is the key of a message: the one its source took from the
// sender (the idempotency_key attribute), else its ID. The relay sends the
// same one on every attempt.
func idempotencyKey(env *envelope.Envelope) string {
	if v, ok := env.Attribute("idempotency_key"); ok {
		if key, ok := v.(string); ok && key != "" {
			return key
		}
	}
	return env.ID
}

// result describes the records written: their partitions and, when the
// brokers acknowledge them, their offsets.
func (s *Sink) result(recs []*kgo.Record) *plugin.Result {
	partitions := make([]string, len(recs))
	offsets := make([]string, len(recs))
	topic := ""
	for i, r := range recs {
		partitions[i] = strconv.Itoa(int(r.Partition))
		offsets[i] = strconv.FormatInt(r.Offset, 10)
		switch {
		case i == 0:
			topic = r.Topic
		case r.Topic != topic:
			topic = ""
		}
	}
	headers := map[string][]string{resultPartition: partitions}
	if s.opts.acks != acksNone { // unacknowledged records have no offset
		headers[resultOffset] = offsets
	}
	endpoint := "kafka://" + s.brokers
	if topic != "" {
		endpoint += "/" + topic
	}
	return &plugin.Result{Headers: headers, Endpoint: endpoint}
}

// Health pings the brokers.
func (s *Sink) Health(ctx context.Context) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errClosed
	}
	if err := s.cl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka %s: %w", s.brokers, err)
	}
	return nil
}

// Close flushes what the producer still holds (for at most 3s, within ctx)
// and closes the client. Records it could not write fail.
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	fctx, cancel := context.WithTimeout(ctx, s.flushTimeout)
	err := s.cl.Flush(fctx)
	cancel()
	s.cancel()
	s.cl.Close()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	if err != nil {
		return fmt.Errorf("destination %s: flushing: %w", s.name, err)
	}
	return nil
}
