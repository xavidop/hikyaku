package azure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// producerRetry is the SDK's retry policy of a destination: one retry,
// which recovers a link that went stale between deliveries.
var producerRetry = azeventhubs.RetryOptions{MaxRetries: 1, RetryDelay: 100 * time.Millisecond, MaxRetryDelay: time.Second}

// ehSink is an eventhubs destination: a producer for one event hub.
type ehSink struct {
	name     string
	endpoint string // <namespace>/<hub>
	opts     ehSinkOptions
	producer ehProducer
	log      *slog.Logger
	closed   atomic.Bool
}

func newEHSink(_ context.Context, spec plugin.Spec) (*ehSink, error) {
	opts, err := parseEHSink(spec)
	if err != nil {
		return nil, err
	}
	p, err := dialProducer(opts.conn, opts.hub, producerRetry)
	if err != nil {
		return nil, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	return buildEHSink(spec, opts, p), nil
}

// buildEHSink makes the destination on a producer (tests pass a fake).
func buildEHSink(spec plugin.Spec, opts ehSinkOptions, p ehProducer) *ehSink {
	return &ehSink{name: spec.Name, endpoint: opts.conn.namespace + "/" + opts.hub, opts: opts, producer: p, log: spec.Env.Log()}
}

// Name is the destination name.
func (s *ehSink) Name() string { return s.name }

// Deliver sends the event as a batch of one, with the rendered key as its
// partition key (none: Event Hubs picks the partition).
func (s *ehSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	return s.DeliverBatch(ctx, []*envelope.Envelope{env})
}

// DeliverBatch sends the events in as few batches as fit, one partition
// key at a time, in order. A batch succeeds or fails as a whole, so a retry
// may write some events twice.
func (s *ehSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	if s.closed.Load() {
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	var (
		order  []string
		groups = map[string][]*azeventhubs.EventData{}
	)
	for _, env := range envs {
		key, ev, err := s.event(env)
		if err != nil {
			return nil, err
		}
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], ev)
	}
	for _, key := range order {
		if err := s.sendGroup(ctx, key, groups[key]); err != nil {
			return nil, err
		}
	}
	return &plugin.Result{Endpoint: s.endpoint}, nil
}

// sendGroup sends events with one partition key, starting a new batch
// whenever one is full.
func (s *ehSink) sendGroup(ctx context.Context, key string, events []*azeventhubs.EventData) error {
	opts := &azeventhubs.EventDataBatchOptions{}
	if key != "" {
		opts.PartitionKey = &key
	}
	var b ehBatch
	for _, ev := range events {
		for tries := 0; ; tries++ {
			if b == nil {
				nb, err := s.producer.newBatch(ctx, opts)
				if err != nil {
					return classify(ctx, kindEventHubs, "new batch for "+s.endpoint, err)
				}
				b = nb
			}
			err := b.AddEventData(ev, nil)
			if err == nil {
				break
			}
			if !errors.Is(err, azeventhubs.ErrEventDataTooLarge) || b.NumEvents() == 0 || tries > 0 {
				return classify(ctx, kindEventHubs, "event "+*ev.MessageID, err)
			}
			if err := s.producer.sendBatch(ctx, b); err != nil {
				return classify(ctx, kindEventHubs, "send to "+s.endpoint, err)
			}
			b = nil
		}
	}
	if b != nil && b.NumEvents() > 0 {
		if err := s.producer.sendBatch(ctx, b); err != nil {
			return classify(ctx, kindEventHubs, "send to "+s.endpoint, err)
		}
	}
	return nil
}

// event builds the event of an envelope: the envelope ID as MessageId, the
// content type and the properties. It returns the partition key too.
func (s *ehSink) event(env *envelope.Envelope) (string, *azeventhubs.EventData, error) {
	body, props, err := s.opts.out.render(env)
	if err != nil {
		return "", nil, err
	}
	key, err := renderKey(s.opts.key, env)
	if err != nil {
		return "", nil, err
	}
	id := env.ID
	ev := &azeventhubs.EventData{Body: body, Properties: props, MessageID: &id}
	if env.ContentType != "" {
		ct := env.ContentType
		ev.ContentType = &ct
	}
	return key, ev, nil
}

// Health asks for the event hub's properties: it checks the connection,
// the credentials and the hub.
func (s *ehSink) Health(ctx context.Context) error {
	if s.closed.Load() {
		return errClosed
	}
	if _, err := s.producer.GetEventHubProperties(ctx, nil); err != nil {
		return classify(ctx, kindEventHubs, s.endpoint, err)
	}
	return nil
}

// Close closes the producer.
func (s *ehSink) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}
	return s.producer.Close(ctx)
}
