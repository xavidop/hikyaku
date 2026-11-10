package aws

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// maxBatchBytes is the payload SendMessageBatch, PublishBatch and PutEvents
// take per call.
const maxBatchBytes = 256 << 10

// Result headers.
const (
	headerSQSMessageID      = "sqs-message-id"
	headerSQSSequenceNumber = "sqs-sequence-number"
)

// sqsSink is an sqs destination: SendMessage, or SendMessageBatch for a
// batch.
type sqsSink struct {
	name     string
	relay    string
	queueURL string
	fifo     bool
	delay    int32
	group    *transform.Template
	build    *messageBuilder
	client   sqsSender
}

func newSQSSink(ctx context.Context, spec plugin.Spec) (*sqsSink, error) {
	ep, fail, err := sinkConfig(spec)
	if err != nil {
		return nil, err
	}
	s, err := parseSQSSink(spec)
	if err != nil {
		return nil, err
	}
	cfg, err := loadAWS(ctx, ep.AWS, regionFromQueueURL(s.queueURL), sinkAttempts, roleSessionName(spec))
	if err != nil {
		return nil, fail("%v", err)
	}
	s.client = sqs.NewFromConfig(cfg)
	return s, nil
}

// parseSQSSink builds an sqs destination without its client.
func parseSQSSink(spec plugin.Spec) (*sqsSink, error) {
	ep, fail, err := sinkConfig(spec)
	if err != nil {
		return nil, err
	}
	if ep.QueueURL == "" {
		return nil, fail("queue_url is required: the queue sent to")
	}
	if err := static("queue_url", ep.QueueURL); err != nil {
		return nil, fail("%v", err)
	}
	s := &sqsSink{name: spec.Name, relay: relayName(spec), queueURL: ep.QueueURL, fifo: strings.HasSuffix(ep.QueueURL, ".fifo")}
	switch d := ep.Delay.Std(); {
	case d < 0 || d > maxDelay:
		return nil, fail("delay must be at most 15m (the SQS limit)")
	case d > 0 && s.fifo:
		return nil, fail("delay applies to standard queues; a FIFO queue's delay is set on the queue (DelaySeconds)")
	default:
		s.delay = seconds(d)
	}
	b := &messageBuilder{service: kindSQS, dest: spec.Name, log: spec.Env.Log()}
	if b.body, err = optionalTemplate("body", ep.Body); err != nil {
		return nil, fail("%v", err)
	}
	if b.headers, err = compileHeaders(ep.Headers); err != nil {
		return nil, fail("%v", err)
	}
	if s.group, err = optionalTemplate("group_id", ep.GroupID); err != nil {
		return nil, fail("%v", err)
	}
	s.build = b
	return s, nil
}

// Name is the destination name.
func (s *sqsSink) Name() string { return s.name }

// message builds the SendMessage fields of one envelope: FIFO queues get
// the group (group_id, default the envelope source) and the idempotency key
// as the deduplication ID; a standard queue gets group_id only when it is
// set (fair queues).
func (s *sqsSink) message(env *envelope.Envelope) (outMessage, *string, *string, error) {
	m, err := s.build.build(env)
	if err != nil {
		return outMessage{}, nil, nil, err
	}
	if !s.fifo && s.group == nil {
		return m, nil, nil, nil
	}
	g, err := groupID(s.group, env, s.relay)
	if err != nil {
		return outMessage{}, nil, nil, err
	}
	if !s.fifo {
		return m, &g, nil, nil
	}
	dedupe := fifoID(idempotencyKey(env))
	return m, &g, &dedupe, nil
}

// Deliver sends one message.
func (s *sqsSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	m, group, dedupe, err := s.message(env)
	if err != nil {
		return nil, err
	}
	out, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(s.queueURL),
		MessageBody:            aws.String(m.body),
		MessageAttributes:      sqsAttributes(m.attrs),
		DelaySeconds:           s.delay,
		MessageGroupId:         group,
		MessageDeduplicationId: dedupe,
	})
	if err != nil {
		return nil, classify(kindSQS, err)
	}
	h := map[string][]string{headerSQSMessageID: {aws.ToString(out.MessageId)}}
	if out.SequenceNumber != nil {
		h[headerSQSSequenceNumber] = []string{*out.SequenceNumber}
	}
	return &plugin.Result{Endpoint: s.queueURL, Headers: h}, nil
}

// DeliverBatch sends a batch with SendMessageBatch, 10 messages and 256KiB
// per call. Any failed entry fails the batch: the relay retries all of it,
// and a FIFO queue's deduplication drops what was already sent.
func (s *sqsSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	type entry struct {
		m      outMessage
		group  *string
		dedupe *string
	}
	entries := make([]entry, len(envs))
	for i, env := range envs {
		m, g, d, err := s.message(env)
		if err != nil {
			return nil, err
		}
		entries[i] = entry{m, g, d}
	}
	var ids, seqs []string
	for _, c := range chunk(entries, func(e entry) int { return e.m.size }, sqsBatchEntries, maxBatchBytes) {
		req := make([]types.SendMessageBatchRequestEntry, len(c))
		for i, e := range c {
			req[i] = types.SendMessageBatchRequestEntry{
				Id:                     aws.String(strconv.Itoa(i)),
				MessageBody:            aws.String(e.m.body),
				MessageAttributes:      sqsAttributes(e.m.attrs),
				DelaySeconds:           s.delay,
				MessageGroupId:         e.group,
				MessageDeduplicationId: e.dedupe,
			}
		}
		out, err := s.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(s.queueURL), Entries: req})
		if err != nil {
			return nil, classify(kindSQS, err)
		}
		if len(out.Failed) > 0 {
			failed := make([]entryFailure, len(out.Failed))
			for i, f := range out.Failed {
				failed[i] = entryFailure{code: aws.ToString(f.Code), message: aws.ToString(f.Message), sender: f.SenderFault}
			}
			return nil, batchError(kindSQS, failed, len(c))
		}
		got, gotSeqs := make([]string, len(c)), make([]string, len(c))
		for _, r := range out.Successful {
			if i, err := strconv.Atoi(aws.ToString(r.Id)); err == nil && i >= 0 && i < len(got) {
				got[i], gotSeqs[i] = aws.ToString(r.MessageId), aws.ToString(r.SequenceNumber)
			}
		}
		ids, seqs = append(ids, got...), append(seqs, gotSeqs...)
	}
	h := map[string][]string{headerSQSMessageID: ids}
	if s.fifo {
		h[headerSQSSequenceNumber] = seqs
	}
	return &plugin.Result{Endpoint: s.queueURL, Headers: h}, nil
}

// Health asks the queue for its ARN: a cheap call that needs the queue to
// exist and the credentials to work.
func (s *sqsSink) Health(ctx context.Context) error {
	_, err := s.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(s.queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("sqs %s: %w", s.queueURL, classify(kindSQS, err))
	}
	return nil
}

// Close has nothing to flush: every delivery is one call.
func (s *sqsSink) Close(context.Context) error { return nil }
