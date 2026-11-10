package aws

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// maxSubject is the longest SNS subject.
const maxSubject = 100

// Result headers.
const (
	headerSNSMessageID      = "sns-message-id"
	headerSNSSequenceNumber = "sns-sequence-number"
)

// snsSink is an sns destination: Publish, or PublishBatch for a batch.
type snsSink struct {
	name    string
	relay   string
	topic   *transform.Template
	subject *transform.Template
	group   *transform.Template
	build   *messageBuilder
	client  snsPublisher
}

func newSNSSink(ctx context.Context, spec plugin.Spec) (*snsSink, error) {
	ep, fail, err := sinkConfig(spec)
	if err != nil {
		return nil, err
	}
	s, err := parseSNSSink(spec)
	if err != nil {
		return nil, err
	}
	hint := ""
	if s.topic.IsStatic() {
		hint = regionFromARN(s.topic.String())
	}
	cfg, err := loadAWS(ctx, ep.AWS, hint, sinkAttempts, roleSessionName(spec))
	if err != nil {
		return nil, fail("%v", err)
	}
	s.client = sns.NewFromConfig(cfg)
	return s, nil
}

// parseSNSSink builds an sns destination without its client.
func parseSNSSink(spec plugin.Spec) (*snsSink, error) {
	ep, fail, err := sinkConfig(spec)
	if err != nil {
		return nil, err
	}
	if ep.Topic == "" {
		return nil, fail("topic is required: the topic ARN published to")
	}
	s := &snsSink{name: spec.Name, relay: relayName(spec)}
	if s.topic, err = transform.CompileTemplate("topic", ep.Topic); err != nil {
		return nil, fail("%v", err)
	}
	if s.topic.IsStatic() && !strings.HasPrefix(ep.Topic, "arn:") {
		return nil, fail("topic %q is not a topic ARN (arn:aws:sns:<region>:<account>:<name>)", ep.Topic)
	}
	if s.subject, err = optionalTemplate("subject", ep.Subject); err != nil {
		return nil, fail("%v", err)
	}
	if s.group, err = optionalTemplate("group_id", ep.GroupID); err != nil {
		return nil, fail("%v", err)
	}
	b := &messageBuilder{service: kindSNS, dest: spec.Name, log: spec.Env.Log()}
	if b.body, err = optionalTemplate("body", ep.Body); err != nil {
		return nil, fail("%v", err)
	}
	if b.headers, err = compileHeaders(ep.Headers); err != nil {
		return nil, fail("%v", err)
	}
	s.build = b
	return s, nil
}

// Name is the destination name.
func (s *snsSink) Name() string { return s.name }

// snsMessage is one envelope ready to publish.
type snsMessage struct {
	index   int // in the batch
	topic   string
	m       outMessage
	subject *string
	group   *string
	dedupe  *string
}

func (s *snsSink) message(env *envelope.Envelope) (snsMessage, error) {
	topic, err := s.topic.Render(env)
	if err != nil {
		return snsMessage{}, templateError(err)
	}
	if topic = strings.TrimSpace(topic); !strings.HasPrefix(topic, "arn:") {
		return snsMessage{}, plugin.Permanentf("topic %q is not a topic ARN", topic)
	}
	m, err := s.build.build(env)
	if err != nil {
		return snsMessage{}, err
	}
	out := snsMessage{topic: topic, m: m}
	if s.subject != nil {
		v, err := s.subject.Render(env)
		if err != nil {
			return snsMessage{}, templateError(err)
		}
		if v = snsSubject(v); v != "" {
			out.subject = &v
			out.m.size += len(v)
		}
	}
	fifo := strings.HasSuffix(topic, ".fifo")
	if fifo || s.group != nil {
		g, err := groupID(s.group, env, s.relay)
		if err != nil {
			return snsMessage{}, err
		}
		out.group = &g
	}
	if fifo {
		d := fifoID(idempotencyKey(env))
		out.dedupe = &d
	}
	return out, nil
}

// snsSubject makes a subject SNS accepts: no line breaks or control
// characters, at most 100 characters.
func snsSubject(v string) string {
	v = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, v)
	v = strings.TrimSpace(v)
	if utf8.RuneCountInString(v) > maxSubject {
		v = strings.TrimSpace(string([]rune(v)[:maxSubject]))
	}
	return v
}

// Deliver publishes one message.
func (s *snsSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	m, err := s.message(env)
	if err != nil {
		return nil, err
	}
	out, err := s.client.Publish(ctx, &sns.PublishInput{
		TopicArn:               aws.String(m.topic),
		Message:                aws.String(m.m.body),
		Subject:                m.subject,
		MessageAttributes:      snsAttributes(m.m.attrs),
		MessageGroupId:         m.group,
		MessageDeduplicationId: m.dedupe,
	})
	if err != nil {
		return nil, classify(kindSNS, err)
	}
	h := map[string][]string{headerSNSMessageID: {aws.ToString(out.MessageId)}}
	if out.SequenceNumber != nil {
		h[headerSNSSequenceNumber] = []string{*out.SequenceNumber}
	}
	return &plugin.Result{Endpoint: m.topic, Headers: h}, nil
}

// DeliverBatch publishes a batch with PublishBatch, per topic, 10 messages
// and 256KiB per call. Any failed entry fails the batch: the relay retries
// all of it, and a FIFO topic's deduplication drops what was already sent.
func (s *snsSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	var topics []string
	byTopic := map[string][]snsMessage{}
	for i, env := range envs {
		m, err := s.message(env)
		if err != nil {
			return nil, err
		}
		m.index = i
		if _, ok := byTopic[m.topic]; !ok {
			topics = append(topics, m.topic)
		}
		byTopic[m.topic] = append(byTopic[m.topic], m)
	}
	ids, seqs := make([]string, len(envs)), make([]string, len(envs))
	for _, topic := range topics {
		for _, c := range chunk(byTopic[topic], func(m snsMessage) int { return m.m.size }, sqsBatchEntries, maxBatchBytes) {
			req := make([]types.PublishBatchRequestEntry, len(c))
			for i, m := range c {
				req[i] = types.PublishBatchRequestEntry{
					Id:                     aws.String(strconv.Itoa(i)),
					Message:                aws.String(m.m.body),
					Subject:                m.subject,
					MessageAttributes:      snsAttributes(m.m.attrs),
					MessageGroupId:         m.group,
					MessageDeduplicationId: m.dedupe,
				}
			}
			out, err := s.client.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(topic), PublishBatchRequestEntries: req})
			if err != nil {
				return nil, classify(kindSNS, err)
			}
			if len(out.Failed) > 0 {
				failed := make([]entryFailure, len(out.Failed))
				for i, f := range out.Failed {
					failed[i] = entryFailure{code: aws.ToString(f.Code), message: aws.ToString(f.Message), sender: f.SenderFault}
				}
				return nil, batchError(kindSNS, failed, len(c))
			}
			for _, r := range out.Successful {
				if i, err := strconv.Atoi(aws.ToString(r.Id)); err == nil && i >= 0 && i < len(c) {
					ids[c[i].index], seqs[c[i].index] = aws.ToString(r.MessageId), aws.ToString(r.SequenceNumber)
				}
			}
		}
	}
	h := map[string][]string{headerSNSMessageID: ids}
	if slices.ContainsFunc(seqs, func(s string) bool { return s != "" }) {
		h[headerSNSSequenceNumber] = seqs
	}
	return &plugin.Result{Endpoint: strings.Join(topics, ","), Headers: h}, nil
}

// Health reads the topic's attributes: a cheap call that needs the topic to
// exist and the credentials to work. A per-message topic has nothing to
// check before a message names it.
func (s *snsSink) Health(ctx context.Context) error {
	if !s.topic.IsStatic() {
		return nil
	}
	topic := s.topic.String()
	if _, err := s.client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(topic)}); err != nil {
		return fmt.Errorf("sns %s: %w", topic, classify(kindSNS, err))
	}
	return nil
}

// Close has nothing to flush: every delivery is one call.
func (s *snsSink) Close(context.Context) error { return nil }
