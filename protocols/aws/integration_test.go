//go:build integration

package aws

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// localstackImage is the AWS emulator the suite runs against.
const localstackImage = "localstack/localstack:4"

// stack is a LocalStack container and admin clients to it.
type stack struct {
	endpoint string
	sqs      *sqs.Client
	sns      *sns.Client
	events   *eventbridge.Client
	s3       *s3.Client
}

func startLocalStack(t *testing.T) *stack {
	t.Helper()
	hermeticAWS(t)
	ctx := context.Background()
	ctr, err := localstack.Run(ctx, localstackImage,
		testcontainers.WithEnv(map[string]string{"SERVICES": "sqs,sns,events,s3,sts"}))
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	port, err := ctr.MappedPort(ctx, "4566/tcp")
	require.NoError(t, err)
	st := &stack{endpoint: fmt.Sprintf("http://%s:%s", host, port.Port())}
	cfg, err := loadAWS(ctx, testAWS(st.endpoint), "", 0, "admin")
	require.NoError(t, err)
	st.sqs, st.sns, st.events = sqs.NewFromConfig(cfg), sns.NewFromConfig(cfg), eventbridge.NewFromConfig(cfg)
	st.s3 = newS3Client(cfg, true)
	return st
}

func (st *stack) aws() *config.AWS { return testAWS(st.endpoint) }

// queue creates a queue and returns its URL and ARN.
func (st *stack) queue(t *testing.T, name string, attrs map[string]string) (string, string) {
	t.Helper()
	ctx := context.Background()
	out, err := st.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	require.NoError(t, err)
	a, err := st.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: out.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	return aws.ToString(out.QueueUrl), a.Attributes["QueueArn"]
}

// depth is how many messages a queue holds, visible or in flight.
func (st *stack) depth(t *testing.T, url string) int {
	t.Helper()
	a, err := st.sqs.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	require.NoError(t, err)
	n := 0
	for _, k := range []string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"} {
		var v int
		_, _ = fmt.Sscan(a.Attributes[k], &v)
		n += v
	}
	return n
}

// consume starts a queue source; the test's cleanup stops and closes it.
func consume(t *testing.T, kind string, src *config.Source) chan *envelope.Envelope {
	t.Helper()
	s, err := newQueueSource(context.Background(), sourceSpec(kind, src))
	require.NoError(t, err)
	require.NoError(t, s.Health(context.Background()))
	out := make(chan *envelope.Envelope, 16)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, out) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
		assert.NoError(t, s.Close(context.Background()))
	})
	return out
}

func TestLocalStack(t *testing.T) {
	st := startLocalStack(t)
	ctx := context.Background()

	t.Run("sqs round trip with redelivery after Failed", func(t *testing.T) {
		url, _ := st.queue(t, "it-orders", nil)
		sink, err := newSQSSink(ctx, sinkSpec(kindSQS, &config.Endpoint{QueueURL: url, AWS: st.aws(),
			Headers: map[string]string{"tenant": "{{ .json.tenant }}"}}))
		require.NoError(t, err)
		require.NoError(t, sink.Health(ctx))
		env := newEnv(`{"tenant":"acme","order":1}`)
		env.SetHeader("X-Trace", "t-1")
		res, err := sink.Deliver(ctx, env)
		require.NoError(t, err)
		require.NotEmpty(t, res.Headers["sqs-message-id"])
		_, err = sink.Deliver(ctx, envelope.New("in", "http", []byte{0xff, 0x00, 0x01}))
		require.NoError(t, err)

		out := consume(t, kindSQS, &config.Source{QueueURL: url, AWS: st.aws(), WaitTime: config.Duration(time.Second),
			VisibilityTimeout: config.Duration(10 * time.Second)})
		got := recv(t, out, 10*time.Second)
		assert.Equal(t, env.ID, got.ID, "the Idempotency-Key attribute carries the ID across")
		assert.Equal(t, `{"tenant":"acme","order":1}`, string(got.Body))
		assert.Equal(t, contentJSON, got.ContentType)
		assert.Equal(t, "acme", got.Header("tenant"))
		assert.Equal(t, "t-1", got.Header("X-Trace"))
		assert.Equal(t, res.Headers["sqs-message-id"][0], got.Attributes["sqs.message_id"])
		assert.Equal(t, 1, got.Attributes["sqs.receive_count"])

		bin := recv(t, out, 10*time.Second)
		assert.Equal(t, []byte{0xff, 0x00, 0x01}, bin.Body, "binary bodies travel as base64")
		ack(bin, envelope.Delivered)

		ack(got, envelope.Failed)
		again := recv(t, out, 10*time.Second)
		assert.Equal(t, env.ID, again.ID)
		assert.Equal(t, 2, again.Attributes["sqs.receive_count"], "SQS redelivered it")
		ack(again, envelope.Delivered)
		require.Eventually(t, func() bool { return st.depth(t, url) == 0 }, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("sqs batches and FIFO deduplication", func(t *testing.T) {
		url, _ := st.queue(t, "it-orders.fifo", map[string]string{"FifoQueue": "true"})
		sink, err := newSQSSink(ctx, sinkSpec(kindSQS, &config.Endpoint{QueueURL: url, AWS: st.aws(), GroupID: "{{ .json.customer }}"}))
		require.NoError(t, err)
		envs := []*envelope.Envelope{newEnv(`{"customer":"c1","n":1}`), newEnv(`{"customer":"c1","n":2}`), newEnv(`{"customer":"c2","n":3}`)}
		res, err := sink.DeliverBatch(ctx, envs)
		require.NoError(t, err)
		assert.Len(t, res.Headers["sqs-message-id"], 3)
		_, err = sink.Deliver(ctx, envs[0]) // a retry: the dedupe ID drops it
		require.NoError(t, err)

		out := consume(t, kindSQS, &config.Source{QueueURL: url, AWS: st.aws(), WaitTime: config.Duration(time.Second)})
		seen := map[string]string{}
		for range 3 {
			got := recv(t, out, 10*time.Second)
			seen[string(got.Body)] = fmt.Sprint(got.Attributes["ordering_key"])
			ack(got, envelope.Delivered)
		}
		assert.Equal(t, map[string]string{`{"customer":"c1","n":1}`: "c1", `{"customer":"c1","n":2}`: "c1", `{"customer":"c2","n":3}`: "c2"}, seen)
		noMessage(t, out, 2*time.Second)
	})

	t.Run("sns topic to an sqs subscription to the sns source", func(t *testing.T) {
		topic, err := st.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("it-topic")})
		require.NoError(t, err)
		url, arn := st.queue(t, "it-topic-q", nil)
		_, err = st.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(arn)})
		require.NoError(t, err)
		rawURL, rawARN := st.queue(t, "it-topic-raw", nil)
		_, err = st.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(rawARN),
			Attributes: map[string]string{"RawMessageDelivery": "true"}})
		require.NoError(t, err)

		sink, err := newSNSSink(ctx, sinkSpec(kindSNS, &config.Endpoint{Topic: aws.ToString(topic.TopicArn), AWS: st.aws(),
			Subject: "Order {{ .json.order }}", Headers: map[string]string{"tenant": "acme"}}))
		require.NoError(t, err)
		require.NoError(t, sink.Health(ctx))
		env := newEnv(`{"order":1}`)
		_, err = sink.Deliver(ctx, env)
		require.NoError(t, err)

		got := recv(t, consume(t, kindSNS, &config.Source{QueueURL: url, AWS: st.aws(), WaitTime: config.Duration(time.Second)}), 15*time.Second)
		assert.Equal(t, `{"order":1}`, string(got.Body), "unwrapped from the notification")
		assert.Equal(t, env.ID, got.ID)
		assert.Equal(t, "Order 1", got.Subject)
		assert.Equal(t, "acme", got.Header("tenant"))
		assert.Equal(t, contentJSON, got.ContentType)
		assert.Equal(t, aws.ToString(topic.TopicArn), got.Attributes["sns.topic_arn"])
		assert.NotEmpty(t, got.Attributes["sns.message_id"])
		ack(got, envelope.Delivered)

		raw := recv(t, consume(t, kindSNS, &config.Source{QueueURL: rawURL, AWS: st.aws(), WaitTime: config.Duration(time.Second)}), 15*time.Second)
		assert.Equal(t, `{"order":1}`, string(raw.Body), "raw delivery passes through")
		assert.Equal(t, env.ID, raw.ID)
		assert.Equal(t, "acme", raw.Header("tenant"))
		ack(raw, envelope.Delivered)
		require.Eventually(t, func() bool { return st.depth(t, url) == 0 && st.depth(t, rawURL) == 0 }, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("eventbridge rule to an sqs target to the eventbridge source", func(t *testing.T) {
		url, arn := st.queue(t, "it-events-q", nil)
		_, err := st.events.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("it-rule"), EventPattern: aws.String(`{"source":["hikyaku.relay"]}`)})
		require.NoError(t, err)
		_, err = st.events.PutTargets(ctx, &eventbridge.PutTargetsInput{Rule: aws.String("it-rule"),
			Targets: []ebtypes.Target{{Id: aws.String("q"), Arn: aws.String(arn)}}})
		require.NoError(t, err)

		sink, err := newEventBridgeSink(ctx, sinkSpec(kindEventBridge, &config.Endpoint{AWS: st.aws()}))
		require.NoError(t, err)
		require.NoError(t, sink.Health(ctx))
		env := newEnv(`{"order":1}`)
		env.Subject = "Order Placed"
		res, err := sink.Deliver(ctx, env)
		require.NoError(t, err)
		_, err = sink.DeliverBatch(ctx, []*envelope.Envelope{newEnv("plain text")})
		require.NoError(t, err)

		out := consume(t, kindEventBridge, &config.Source{QueueURL: url, AWS: st.aws(), WaitTime: config.Duration(time.Second)})
		got := map[string]*envelope.Envelope{}
		for range 2 {
			e := recv(t, out, 15*time.Second)
			got[e.Subject] = e
			ack(e, envelope.Delivered)
		}
		placed := got["Order Placed"]
		require.NotNil(t, placed)
		assert.JSONEq(t, `{"order":1}`, string(placed.Body), "the detail is the body")
		assert.Equal(t, res.Headers["eventbridge-event-id"][0], placed.ID)
		assert.Equal(t, "hikyaku.relay", placed.Attributes["eventbridge.source"])
		assert.NotEmpty(t, placed.Attributes["eventbridge.time"])
		text := got["hikyaku.message"]
		require.NotNil(t, text)
		assert.JSONEq(t, `{"body":"plain text"}`, string(text.Body))
	})

	t.Run("s3 sink to the s3 poll source with after move", func(t *testing.T) {
		_, err := st.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("it-archive")})
		require.NoError(t, err)
		sink, err := newS3Sink(ctx, sinkSpec(kindS3, &config.Endpoint{Bucket: "it-archive", AWS: st.aws()}))
		require.NoError(t, err)
		require.NoError(t, sink.Health(ctx))
		env := newEnv(`{"order":1}`)
		env.SetHeader("X-Trace", "t-1")
		res, err := sink.Deliver(ctx, env)
		require.NoError(t, err)
		key := res.Headers["s3-key"][0]
		assert.True(t, strings.HasPrefix(key, "relay/"))
		_, err = sink.DeliverBatch(ctx, []*envelope.Envelope{newEnv(`{"n":1}`), newEnv(`{"n":2}`)})
		require.NoError(t, err)

		src, err := newS3Source(ctx, sourceSpec(kindS3, &config.Source{Bucket: "it-archive", Prefix: "relay/", After: "move",
			MoveTo: "done/", Split: "lines", Interval: config.Duration(200 * time.Millisecond), AWS: st.aws()}))
		require.NoError(t, err)
		require.NoError(t, src.Health(ctx))
		out := make(chan *envelope.Envelope, 16)
		sctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- src.Start(sctx, out) }()
		defer func() { cancel(); require.NoError(t, <-done) }()

		bodies := map[string]*envelope.Envelope{}
		for range 3 {
			e := recv(t, out, 10*time.Second)
			bodies[string(e.Body)] = e
			ack(e, envelope.Delivered)
		}
		single := bodies[`{"order":1}`]
		require.NotNil(t, single)
		assert.Equal(t, key, single.Subject)
		assert.Equal(t, "t-1", single.Header("x-trace"), "metadata comes back as headers")
		assert.Contains(t, bodies, `{"n":1}`)
		assert.Contains(t, bodies, `{"n":2}`)

		list := func(prefix string) []string {
			out, err := st.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("it-archive"), Prefix: aws.String(prefix)})
			require.NoError(t, err)
			var keys []string
			for _, o := range out.Contents {
				keys = append(keys, aws.ToString(o.Key))
			}
			return keys
		}
		require.Eventually(t, func() bool { return len(list("relay/")) == 0 && len(list("done/relay/")) == 2 },
			10*time.Second, 100*time.Millisecond, "both objects moved under done/")
		assert.Contains(t, list("done/"), "done/"+key)
		noMessage(t, out, time.Second)
	})

	t.Run("errors from the service", func(t *testing.T) {
		sink, err := newSQSSink(ctx, sinkSpec(kindSQS, &config.Endpoint{
			QueueURL: "http://localhost:4566/000000000000/it-missing", AWS: st.aws()}))
		require.NoError(t, err)
		_, err = sink.Deliver(ctx, newEnv("x"))
		class, tags := errorTags(err)
		assert.Equal(t, plugin.Permanent, class)
		assert.True(t, hasTag(tags, "sqs:"), "tags %v", tags)
		assert.Error(t, sink.Health(ctx))

		s3sink, err := newS3Sink(ctx, sinkSpec(kindS3, &config.Endpoint{Bucket: "it-missing", AWS: st.aws()}))
		require.NoError(t, err)
		_, err = s3sink.Deliver(ctx, newEnv("x"))
		class, tags = errorTags(err)
		assert.Equal(t, plugin.Permanent, class)
		assert.Contains(t, tags, "s3:NoSuchBucket")
	})
}

func hasTag(tags []string, prefix string) bool {
	for _, t := range tags {
		if strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return false
}
