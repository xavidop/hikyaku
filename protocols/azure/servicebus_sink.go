package azure

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// maxSBID is the longest MessageId or SessionId Service Bus accepts.
const maxSBID = 128

// sinkRetry is the SDK's retry policy of a destination: one retry, which
// recovers a link or connection that went stale between deliveries. The
// relay's retry policy owns every other retry.
var sinkRetry = azservicebus.RetryOptions{MaxRetries: 1, RetryDelay: 100 * time.Millisecond, MaxRetryDelay: time.Second}

// sbSink is a servicebus destination: a sender for a queue or topic.
type sbSink struct {
	name     string
	endpoint string // <namespace>/<entity>
	opts     sbSinkOptions
	client   sbClient
	sender   sbSender
	log      *slog.Logger
	closed   atomic.Bool
}

func newSBSink(_ context.Context, spec plugin.Spec) (*sbSink, error) {
	opts, err := parseSBSink(spec)
	if err != nil {
		return nil, err
	}
	client, err := dialServiceBus(opts.conn, sinkRetry)
	if err != nil {
		return nil, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	return buildSBSink(spec, opts, client)
}

// buildSBSink makes the destination on a client (tests pass a fake).
func buildSBSink(spec plugin.Spec, opts sbSinkOptions, client sbClient) (*sbSink, error) {
	sender, err := client.newSender(opts.entity)
	if err != nil {
		_ = client.Close(context.Background())
		return nil, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	return &sbSink{
		name: spec.Name, endpoint: opts.conn.namespace + "/" + opts.entity, opts: opts,
		client: client, sender: sender, log: spec.Env.Log(),
	}, nil
}

// Name is the destination name.
func (s *sbSink) Name() string { return s.name }

// Deliver sends the message: one attempt.
func (s *sbSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if s.closed.Load() {
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	m, err := s.message(env)
	if err != nil {
		return nil, err
	}
	if err := s.sender.SendMessage(ctx, m, nil); err != nil {
		return nil, classify(ctx, kindServiceBus, "send to "+s.endpoint, err)
	}
	return &plugin.Result{Endpoint: s.endpoint}, nil
}

// DeliverBatch sends the messages in as few SendMessageBatch calls as fit,
// one session's messages at a time, in order. A batch succeeds or fails as
// a whole; duplicate detection on the entity drops what a retry sends
// again.
func (s *sbSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	if s.closed.Load() {
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	var (
		order  []string
		groups = map[string][]*azservicebus.Message{}
	)
	for _, env := range envs {
		m, err := s.message(env)
		if err != nil {
			return nil, err
		}
		sid := ""
		if m.SessionID != nil {
			sid = *m.SessionID
		}
		if _, ok := groups[sid]; !ok {
			order = append(order, sid)
		}
		groups[sid] = append(groups[sid], m)
	}
	for _, sid := range order {
		if err := s.sendGroup(ctx, groups[sid]); err != nil {
			return nil, err
		}
	}
	return &plugin.Result{Endpoint: s.endpoint}, nil
}

// sendGroup sends messages in batches, starting a new batch whenever one is
// full.
func (s *sbSink) sendGroup(ctx context.Context, msgs []*azservicebus.Message) error {
	var b sbBatch
	for _, m := range msgs {
		for tries := 0; ; tries++ {
			if b == nil {
				nb, err := s.sender.newBatch(ctx)
				if err != nil {
					return classify(ctx, kindServiceBus, "new batch for "+s.endpoint, err)
				}
				b = nb
			}
			err := b.AddMessage(m, nil)
			if err == nil {
				break
			}
			if !errors.Is(err, azservicebus.ErrMessageTooLarge) || b.NumMessages() == 0 || tries > 0 {
				return classify(ctx, kindServiceBus, "message "+*m.MessageID, err)
			}
			if err := s.sender.sendBatch(ctx, b); err != nil {
				return classify(ctx, kindServiceBus, "send batch to "+s.endpoint, err)
			}
			b = nil
		}
	}
	if b != nil && b.NumMessages() > 0 {
		if err := s.sender.sendBatch(ctx, b); err != nil {
			return classify(ctx, kindServiceBus, "send batch to "+s.endpoint, err)
		}
	}
	return nil
}

// message builds the Service Bus message of an envelope: the envelope ID as
// MessageId (hashed when longer than Service Bus allows), group_id as
// SessionId, the subject, the content type and the application properties.
func (s *sbSink) message(env *envelope.Envelope) (*azservicebus.Message, error) {
	body, props, err := s.opts.out.render(env)
	if err != nil {
		return nil, err
	}
	sid, err := renderKey(s.opts.session, env)
	if err != nil {
		return nil, err
	}
	if len(sid) > maxSBID {
		return nil, plugin.Permanentf("group_id renders a session ID of %d characters; Service Bus allows %d", len(sid), maxSBID)
	}
	id := messageID(env.ID)
	m := &azservicebus.Message{Body: body, ApplicationProperties: props, MessageID: &id}
	if env.ContentType != "" {
		ct := env.ContentType
		m.ContentType = &ct
	}
	if env.Subject != "" {
		subj := env.Subject
		m.Subject = &subj
	}
	if sid != "" {
		m.SessionID = &sid
	}
	return m, nil
}

// messageID is the envelope ID as a Service Bus MessageId: as it is, or its
// SHA-256 when it is too long, so duplicate detection still matches it.
func messageID(id string) string {
	if len(id) <= maxSBID {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

// Health checks the sender's link: creating a batch opens it (connecting
// and authorizing) when it is not open.
func (s *sbSink) Health(ctx context.Context) error {
	if s.closed.Load() {
		return errClosed
	}
	if _, err := s.sender.newBatch(ctx); err != nil {
		return classify(ctx, kindServiceBus, s.endpoint, err)
	}
	return nil
}

// Close closes the sender and the connection.
func (s *sbSink) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}
	return errors.Join(s.sender.Close(ctx), s.client.Close(ctx))
}
