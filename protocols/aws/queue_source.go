package aws

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a queue source; tests shorten them.
const (
	defaultDeleteEvery   = 100 * time.Millisecond
	defaultCallTimeout   = 10 * time.Second
	defaultVisibility    = 30 * time.Second // SQS's default visibility timeout
	minHeartbeat         = 100 * time.Millisecond
	settleBackoffBase    = time.Second
	maxSettleTries       = 3
	maxConcurrentFlushes = 8
	sqsBatchEntries      = 10
)

// errStopped is a source's health once it stopped.
var errStopped = errors.New("stopped")

// queueSource is an sqs, sns or eventbridge source: it long-polls an SQS
// queue and unwraps what the kind puts in it.
type queueSource struct {
	name   string
	kind   string
	opts   queueSourceOptions
	client sqsReceiver
	log    *slog.Logger
	unwrap func(*queueSource, *envelope.Envelope) bool

	rnd         func() float64
	deleteEvery time.Duration
	callTimeout time.Duration

	// visibility is the visibility timeout in seconds (configured, or the
	// queue's), known once Start has run.
	visibility atomic.Int32
	started    atomic.Bool

	mu       sync.Mutex
	status   error
	inflight map[string]*inflightMsg // by receipt handle

	deletes *deleter
	stop    chan struct{} // closed by Close: stops the heartbeat
	closed  sync.Once
}

// inflightMsg is a received message the relay has not settled yet.
type inflightMsg struct {
	receipt string
	// extended is when its visibility timeout last started over: the
	// receive, then each heartbeat.
	extended time.Time
}

func newQueueSource(ctx context.Context, spec plugin.Spec) (*queueSource, error) {
	src, opts, err := parseQueueSource(spec)
	if err != nil {
		return nil, err
	}
	cfg, err := loadAWS(ctx, src.AWS, regionFromQueueURL(opts.queueURL), sourceAttempts, roleSessionName(spec))
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	return buildQueueSource(spec, opts, sqs.NewFromConfig(cfg)), nil
}

// buildQueueSource assembles a source around its client.
func buildQueueSource(spec plugin.Spec, opts queueSourceOptions, client sqsReceiver) *queueSource {
	s := &queueSource{
		name: spec.Name, kind: spec.Kind, opts: opts, client: client, log: spec.Env.Log(),
		rnd: rand.Float64, deleteEvery: defaultDeleteEvery, callTimeout: defaultCallTimeout,
		inflight: map[string]*inflightMsg{}, stop: make(chan struct{}),
	}
	switch spec.Kind {
	case kindSNS:
		s.unwrap = unwrapSNS
	case kindEventBridge:
		s.unwrap = unwrapEventBridge
	}
	s.deletes = newDeleter(s)
	return s
}

// Name is the source name.
func (s *queueSource) Name() string { return s.name }

// Health is the last receive's error once the source runs. Before, it asks
// the queue for its attributes, a cheap call that needs the queue to exist
// and the credentials to work.
func (s *queueSource) Health(ctx context.Context) error {
	if !s.started.Load() {
		_, err := s.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(s.opts.queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
		return classify("sqs", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *queueSource) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

// Start receives until ctx ends, then returns nil. A failing receive is
// retried with the reconnect backoff; Start fails only after
// reconnect.max_attempts failures in a row. Settling goes on after Start
// returns, until Close.
func (s *queueSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	s.visibility.Store(s.resolveVisibility(ctx))
	go s.deletes.run()
	go s.heartbeat()
	s.log.Info(s.kind+" source receiving", "source", s.name, "queue", s.opts.queueURL,
		"visibility_timeout", time.Duration(s.visibility.Load())*time.Second)

	rc := s.opts.reconnect
	failures := 0
	for {
		msgs, err := s.receive(ctx)
		if ctx.Err() != nil {
			s.setStatus(errStopped)
			return nil
		}
		if err != nil {
			s.setStatus(err)
			failures++
			if rc.maxAttempts > 0 && failures >= rc.maxAttempts {
				return fmt.Errorf("giving up after %d failed receives in a row: %w", failures, err)
			}
			wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, failures, s.rnd)
			s.log.Warn(s.kind+" receive failed; retrying", "source", s.name, "queue", s.opts.queueURL,
				"failures", failures, "wait", wait.String(), "err", err)
			if !backoff.Sleep(ctx, wait) {
				s.setStatus(errStopped)
				return nil
			}
			continue
		}
		failures = 0
		s.setStatus(nil)
		if !s.hand(ctx, out, msgs) {
			s.setStatus(errStopped)
			return nil
		}
	}
}

// resolveVisibility is the visibility timeout messages are received with:
// visibility_timeout, else the queue's. A queue whose attributes cannot be
// read (the credentials may lack sqs:GetQueueAttributes) is assumed to have
// SQS's default of 30s.
func (s *queueSource) resolveVisibility(ctx context.Context) int32 {
	if s.opts.visibility > 0 {
		return s.opts.visibility
	}
	cctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()
	out, err := s.client.GetQueueAttributes(cctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(s.opts.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameVisibilityTimeout},
	})
	if err == nil {
		if v, perr := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameVisibilityTimeout)]); perr == nil && v >= 0 {
			return int32(v)
		}
	}
	s.log.Warn("cannot read the queue's visibility timeout; assuming 30s (set visibility_timeout to skip this lookup)",
		"source", s.name, "queue", s.opts.queueURL, "err", err)
	return seconds(defaultVisibility)
}

// receive is one long poll.
func (s *queueSource) receive(ctx context.Context) ([]types.Message, error) {
	in := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(s.opts.queueURL),
		MaxNumberOfMessages:   s.opts.prefetch,
		WaitTimeSeconds:       s.opts.wait,
		VisibilityTimeout:     s.opts.visibility,
		MessageAttributeNames: []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameSentTimestamp,
			types.MessageSystemAttributeNameMessageGroupId,
		},
	}
	out, err := s.client.ReceiveMessage(ctx, in)
	if err != nil {
		return nil, classify("sqs", err)
	}
	now := time.Now()
	s.mu.Lock()
	for _, m := range out.Messages {
		if r := aws.ToString(m.ReceiptHandle); r != "" {
			s.inflight[r] = &inflightMsg{receipt: r, extended: now}
		}
	}
	s.mu.Unlock()
	return out.Messages, nil
}

// hand sends the received messages on out, in order. When ctx ends first,
// the messages not handed over are made visible again at once, for the
// next consumer; hand then reports false.
func (s *queueSource) hand(ctx context.Context, out chan<- *envelope.Envelope, msgs []types.Message) bool {
	for i, m := range msgs {
		env := s.envelope(m)
		if env == nil {
			continue
		}
		select {
		case out <- env:
		case <-ctx.Done():
			s.release(msgs[i:])
			return false
		}
	}
	return true
}

// release makes messages that never reached the relay visible again.
func (s *queueSource) release(msgs []types.Message) {
	var receipts []string
	for _, m := range msgs {
		if r := aws.ToString(m.ReceiptHandle); r != "" && s.untrack(r) {
			receipts = append(receipts, r)
		}
	}
	s.changeVisibility(receipts, 0)
}

// envelope turns a message into an envelope with its settlement wired, or
// returns nil for a message the relay drops: one over max_body_bytes, or an
// SNS subscription confirmation. Those are deleted, since they would come
// back forever.
func (s *queueSource) envelope(m types.Message) *envelope.Envelope {
	receipt := aws.ToString(m.ReceiptHandle)
	env := s.plainEnvelope(m)
	keep := s.unwrap == nil || s.unwrap(s, env)
	if keep && int64(len(env.Body)) > s.opts.maxBytes {
		s.log.Warn("message over max_body_bytes; deleting it", "source", s.name, "queue", s.opts.queueURL,
			"sqs_message_id", aws.ToString(m.MessageId), "bytes", len(env.Body), "max_body_bytes", s.opts.maxBytes)
		keep = false
	}
	if !keep {
		if s.untrack(receipt) {
			s.deletes.add(receipt)
		}
		return nil
	}
	count := receiveCount(m)
	plugin.OnSettle(env, s.opts.ack, func(ctx context.Context, final bool) {
		s.settle(ctx, receipt, count, final)
	})
	return env
}

// plainEnvelope is a message as SQS gave it: the body (base64-decoded when
// its content-transfer-encoding attribute says so), String and Number
// message attributes as headers, and the sqs.* attributes.
func (s *queueSource) plainEnvelope(m types.Message) *envelope.Envelope {
	body := []byte(aws.ToString(m.Body))
	attrs := m.MessageAttributes
	if strings.EqualFold(attrValue(attrs, attrTransferEncoder), encodingBase64) {
		if dec, err := base64.StdEncoding.DecodeString(string(body)); err == nil {
			body = dec
		} else {
			s.log.Warn("message says base64 but its body is not; passing it on as it is", "source", s.name,
				"sqs_message_id", aws.ToString(m.MessageId), "err", err)
		}
	}
	env := envelope.New(s.name, s.kind, body)
	env.ID = aws.ToString(m.MessageId)
	if key := attrValue(attrs, attrIdempotencyKey); key != "" {
		env.ID = key
	}
	if env.ID == "" {
		env.ID = envelope.NewID()
	}
	env.Subject = s.opts.queueName
	for name, v := range attrs {
		if !reservedAttr(name) && v.StringValue != nil && textType(aws.ToString(v.DataType)) {
			env.Headers[name] = []string{*v.StringValue}
		}
	}
	env.ContentType = attrValue(attrs, attrContentType)
	if env.ContentType == "" {
		env.ContentType = detectContentType(body)
	}
	env.Attributes["sqs.message_id"] = aws.ToString(m.MessageId)
	env.Attributes["sqs.receive_count"] = receiveCount(m)
	if ms, err := strconv.ParseInt(m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)], 10, 64); err == nil {
		env.Attributes["sqs.sent_timestamp"] = time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
	}
	if g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]; g != "" {
		env.Attributes["sqs.group_id"] = g
		env.Attributes["ordering_key"] = g
	}
	return env
}

// attrValue is the string value of a message attribute, matched
// case-insensitively.
func attrValue(attrs map[string]types.MessageAttributeValue, name string) string {
	for k, v := range attrs {
		if strings.EqualFold(k, name) && v.StringValue != nil {
			return *v.StringValue
		}
	}
	return ""
}

// reservedAttr reports the attributes the relay consumes itself instead of
// passing them on as headers.
func reservedAttr(name string) bool {
	return strings.EqualFold(name, attrContentType) || strings.EqualFold(name, attrTransferEncoder)
}

// textType reports the attribute data types that become headers: String
// and Number (and their custom subtypes), not Binary.
func textType(dataType string) bool {
	return strings.HasPrefix(dataType, "String") || strings.HasPrefix(dataType, "Number")
}

// receiveCount is how many times SQS handed the message out, 1 for the
// first time.
func receiveCount(m types.Message) int {
	n, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// untrack forgets an in-flight message, reporting whether it was in flight.
func (s *queueSource) untrack(receipt string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.inflight[receipt]
	delete(s.inflight, receipt)
	return ok
}

// settle acknowledges a message: final deletes it (batched); otherwise it
// becomes visible again after a backoff on its receive count, capped at the
// visibility timeout, and SQS redelivers it.
func (s *queueSource) settle(ctx context.Context, receipt string, count int, final bool) {
	if !s.untrack(receipt) {
		return
	}
	if final {
		s.deletes.add(receipt)
		return
	}
	vt := time.Duration(s.visibility.Load()) * time.Second
	delay := min(seconds(backoff.Delay(settleBackoffBase, vt, "equal", count, s.rnd)), s.visibility.Load())
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.callTimeout)
	defer cancel()
	_, err := s.client.ChangeMessageVisibility(cctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(s.opts.queueURL), ReceiptHandle: aws.String(receipt), VisibilityTimeout: delay,
	})
	if err != nil {
		s.log.Warn("cannot make a failed message visible again; SQS redelivers it once its visibility timeout ends",
			"source", s.name, "queue", s.opts.queueURL, "err", classify("sqs", err))
		return
	}
	s.log.Debug("message failed; redelivering", "source", s.name, "receive_count", count, "after", time.Duration(delay)*time.Second)
}

// heartbeat extends the visibility of the messages in flight longer than
// half the visibility timeout, until Close. A message settled while its
// extension is on the way may stay hidden for one more visibility timeout:
// a delay, never a loss.
func (s *queueSource) heartbeat() {
	vt := time.Duration(s.visibility.Load()) * time.Second
	if vt <= 0 {
		return // messages are visible again at once: nothing to extend
	}
	t := time.NewTicker(max(vt/4, minHeartbeat))
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		now := time.Now()
		var due []string
		s.mu.Lock()
		for r, m := range s.inflight {
			if now.Sub(m.extended) >= vt/2 {
				m.extended = now
				due = append(due, r)
			}
		}
		s.mu.Unlock()
		if len(due) > 0 {
			s.changeVisibility(due, s.visibility.Load())
			s.log.Debug("extended the visibility of messages in flight", "source", s.name, "messages", len(due))
		}
	}
}

// changeVisibility sets the visibility timeout of messages, 10 per call.
func (s *queueSource) changeVisibility(receipts []string, secs int32) {
	for chunk := range slices.Chunk(receipts, sqsBatchEntries) {
		entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(chunk))
		for i, r := range chunk {
			entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
				Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(r), VisibilityTimeout: secs,
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.callTimeout)
		out, err := s.client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(s.opts.queueURL), Entries: entries,
		})
		cancel()
		switch {
		case err != nil:
			s.log.Warn("cannot change the visibility of messages", "source", s.name, "queue", s.opts.queueURL,
				"messages", len(chunk), "err", classify("sqs", err))
		case len(out.Failed) > 0:
			s.log.Debug("some visibility changes failed (settled messages)", "source", s.name,
				"failed", len(out.Failed), "code", aws.ToString(out.Failed[0].Code))
		}
	}
}

// Close stops the heartbeat and flushes the deletes still pending. The
// engine calls it once the messages in flight are settled.
func (s *queueSource) Close(ctx context.Context) error {
	s.closed.Do(func() {
		close(s.stop)
		if s.started.Load() {
			s.deletes.close(ctx)
		}
	})
	return nil
}

// deleter batches the deletes of settled messages: one DeleteMessageBatch
// per 10 messages, at least every deleteEvery. A failed delete is retried
// twice; after that SQS redelivers the message, whose ID lets the relay's
// destinations drop the duplicate.
type deleter struct {
	s *queueSource

	mu      sync.Mutex
	pending []pendingDelete
	closed  bool

	kick chan struct{}
	stop chan struct{}
	done chan struct{}
}

type pendingDelete struct {
	receipt string
	tries   int
}

func newDeleter(s *queueSource) *deleter {
	return &deleter{s: s, kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
}

// add queues a delete. After close it deletes at once.
func (d *deleter) add(receipt string) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		d.deleteChunk([]pendingDelete{{receipt: receipt}}, true)
		return
	}
	d.pending = append(d.pending, pendingDelete{receipt: receipt})
	full := len(d.pending) >= sqsBatchEntries
	d.mu.Unlock()
	if full {
		select {
		case d.kick <- struct{}{}:
		default:
		}
	}
}

func (d *deleter) run() {
	defer close(d.done)
	t := time.NewTicker(d.s.deleteEvery)
	defer t.Stop()
	for {
		select {
		case <-d.kick:
		case <-t.C:
		case <-d.stop:
			// Flush, then retry what failed while tries remain.
			for range maxSettleTries {
				if !d.flush() {
					break
				}
			}
			return
		}
		d.flush()
	}
}

// flush deletes what is pending, up to maxConcurrentFlushes calls at once,
// and reports whether retries are pending again.
func (d *deleter) flush() bool {
	d.mu.Lock()
	batch := d.pending
	d.pending = nil
	d.mu.Unlock()
	if len(batch) == 0 {
		return false
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentFlushes)
	for chunk := range slices.Chunk(batch, sqsBatchEntries) {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d.deleteChunk(chunk, false)
		}()
	}
	wg.Wait()
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending) > 0
}

// deleteChunk deletes up to 10 messages in one call. Failures go back to
// pending for another try, unless last is set or they ran out of tries.
func (d *deleter) deleteChunk(chunk []pendingDelete, last bool) {
	s := d.s
	entries := make([]types.DeleteMessageBatchRequestEntry, len(chunk))
	for i, p := range chunk {
		entries[i] = types.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(p.receipt)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.callTimeout)
	out, err := s.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(s.opts.queueURL), Entries: entries})
	cancel()
	var retry []pendingDelete
	switch {
	case err != nil:
		retry = chunk
		s.log.Warn("cannot delete settled messages", "source", s.name, "queue", s.opts.queueURL,
			"messages", len(chunk), "err", classify("sqs", err))
	default:
		for _, f := range out.Failed {
			i, perr := strconv.Atoi(aws.ToString(f.Id))
			if perr != nil || i < 0 || i >= len(chunk) {
				continue
			}
			if code := aws.ToString(f.Code); code == "ReceiptHandleIsInvalid" || code == "InvalidParameterValue" {
				// Expired or already deleted: nothing more to do.
				s.log.Debug("settled message already gone", "source", s.name, "code", code)
				continue
			}
			retry = append(retry, chunk[i])
			s.log.Warn("cannot delete a settled message", "source", s.name, "queue", s.opts.queueURL,
				"code", aws.ToString(f.Code), "err", aws.ToString(f.Message))
		}
	}
	if len(retry) == 0 || last {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, p := range retry {
		if p.tries+1 < maxSettleTries {
			d.pending = append(d.pending, pendingDelete{receipt: p.receipt, tries: p.tries + 1})
		}
	}
}

// close flushes what is pending, retrying failures, and stops. It returns
// early when ctx ends.
func (d *deleter) close(ctx context.Context) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.mu.Unlock()
	close(d.stop)
	select {
	case <-d.done:
	case <-ctx.Done():
	}
}
