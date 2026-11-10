package aws

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func newEnv(body string) *envelope.Envelope {
	env := envelope.New("orders-in", "http", []byte(body))
	env.ContentType = detectContentType(env.Body)
	return env
}

func newSQSSinkWith(t *testing.T, ep *config.Endpoint, fake *fakeSQS) (*sqsSink, *logBuffer) {
	t.Helper()
	spec := sinkSpec(kindSQS, ep)
	logs := &logBuffer{}
	spec.Env.Logger = logs.logger()
	s, err := parseSQSSink(spec)
	require.NoError(t, err)
	s.client = fake
	return s, logs
}

func TestSQSSinkDeliver(t *testing.T) {
	fake := newFakeSQS()
	s, _ := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL, Delay: config.Duration(1500 * time.Millisecond),
		Headers: map[string]string{"tenant": "{{ .json.tenant }}", "empty": "{{ .json.missing }}"}}, fake)
	env := newEnv(`{"tenant":"acme"}`)
	env.SetHeader("Idempotency-Key", "idem-1")
	env.SetHeader("X-Trace", "t-1")
	env.SetHeader("Authorization", "Bearer secret")
	env.SetHeader("Cookie", "c=1")
	env.SetHeader("Content-Type", "application/json")

	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, queueURL, res.Endpoint)
	assert.Equal(t, []string{"sent-1"}, res.Headers["sqs-message-id"])
	require.Len(t, fake.sends, 1)
	in := fake.sends[0]
	assert.Equal(t, queueURL, aws.ToString(in.QueueUrl))
	assert.Equal(t, `{"tenant":"acme"}`, aws.ToString(in.MessageBody))
	assert.Equal(t, int32(2), in.DelaySeconds)
	assert.Nil(t, in.MessageGroupId, "a standard queue has no group")
	assert.Nil(t, in.MessageDeduplicationId)
	got := map[string]string{}
	for k, v := range in.MessageAttributes {
		assert.Equal(t, "String", aws.ToString(v.DataType))
		got[k] = aws.ToString(v.StringValue)
	}
	assert.Equal(t, map[string]string{
		"content-type": "application/json", "Idempotency-Key": "idem-1", "tenant": "acme", "X-Trace": "t-1",
	}, got, "credentials, framing and empty values do not travel")
	assert.Equal(t, "out", s.Name())
	require.NoError(t, s.Close(context.Background()))
}

func TestSQSSinkEncodesBinaryBodies(t *testing.T) {
	fake := newFakeSQS()
	s, _ := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL}, fake)
	for _, body := range [][]byte{{0xff, 0xfe}, []byte("bell\x07"), []byte("nul\x00")} {
		env := envelope.New("in", "http", body)
		_, err := s.Deliver(context.Background(), env)
		require.NoError(t, err)
		in := fake.sends[len(fake.sends)-1]
		assert.Equal(t, base64.StdEncoding.EncodeToString(body), aws.ToString(in.MessageBody))
		assert.Equal(t, "base64", aws.ToString(in.MessageAttributes["content-transfer-encoding"].StringValue))
		assert.Equal(t, env.ID, aws.ToString(in.MessageAttributes["Idempotency-Key"].StringValue), "the envelope ID by default")
	}
	_, err := s.Deliver(context.Background(), envelope.New("in", "http", []byte("tabs\tand\nlines\r\n ok ✓")))
	require.NoError(t, err)
	assert.Equal(t, "tabs\tand\nlines\r\n ok ✓", aws.ToString(fake.sends[len(fake.sends)-1].MessageBody))
	assert.NotContains(t, fake.sends[len(fake.sends)-1].MessageAttributes, "content-transfer-encoding")
}

func TestSQSSinkKeepsTenAttributes(t *testing.T) {
	fake := newFakeSQS()
	s, logs := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL, Headers: map[string]string{"zz-configured": "yes"}}, fake)
	build := func() []string {
		env := envelope.New("in", "http", []byte{0xff})
		env.ContentType = "application/octet-stream"
		for i := range 12 {
			env.SetHeader(fmt.Sprintf("H-%02d", i), "v")
		}
		env.Headers["bad name!"] = []string{"v"}
		env.Headers["AWS.reserved"] = []string{"v"}
		env.Headers["X-Bad-Value"] = []string{"nul\x00"}
		m, err := s.build.build(env)
		require.NoError(t, err)
		return attrNames(m.attrs)
	}
	names := build()
	assert.Equal(t, []string{"content-transfer-encoding", "content-type", "Idempotency-Key", "zz-configured",
		"H-00", "H-01", "H-02", "H-03", "H-04", "H-05"}, names,
		"the relay's own attributes, the headers block, then the message's headers in name order")
	build()
	out := logs.String()
	assert.Equal(t, 1, strings.Count(out, "over the 10 message attributes"), "logged once")
	assert.Equal(t, 1, strings.Count(out, "bad name or value"), "logged once")
	assert.Contains(t, out, "H-11")
	assert.Contains(t, out, "bad name!")
}

func TestSQSSinkHeadersBlockOverridesTheRelayAttributes(t *testing.T) {
	fake := newFakeSQS()
	s, _ := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL, Headers: map[string]string{
		"Content-Type": "text/csv", "idempotency-key": "{{ .json.id }}", "content-transfer-encoding": "gzip",
	}}, fake)
	m, err := s.build.build(newEnv(`{"id":"order-1"}`))
	require.NoError(t, err)
	assert.Equal(t, []attribute{{"content-type", "text/csv"}, {"Idempotency-Key", "order-1"}}, m.attrs,
		"content-transfer-encoding describes the body, so it is never configured")
}

func TestSQSSinkFIFO(t *testing.T) {
	fake := newFakeSQS()
	s, _ := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL + ".fifo"}, fake)
	env := newEnv(`{"customer":"c-1"}`)
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	in := fake.sends[0]
	assert.Equal(t, "orders-in", aws.ToString(in.MessageGroupId), "the envelope source by default")
	assert.Equal(t, env.ID, aws.ToString(in.MessageDeduplicationId), "the envelope ID dedupes")
	assert.Zero(t, in.DelaySeconds)
	assert.NotEmpty(t, res.Headers["sqs-sequence-number"])

	s, _ = newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL + ".fifo", GroupID: "{{ .json.customer }}"}, fake)
	env = newEnv(`{"customer":"c-1"}`)
	long := strings.Repeat("k", 200)
	env.SetHeader("Idempotency-Key", long)
	_, err = s.Deliver(context.Background(), env)
	require.NoError(t, err)
	in = fake.sends[1]
	assert.Equal(t, "c-1", aws.ToString(in.MessageGroupId))
	assert.Equal(t, fifoID(long), aws.ToString(in.MessageDeduplicationId))
	assert.Len(t, aws.ToString(in.MessageDeduplicationId), 64, "too long: its SHA-256")

	env = newEnv(`{"customer":"Customer Seven"}`)
	_, err = s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, fifoID("Customer Seven"), aws.ToString(fake.sends[2].MessageGroupId), "spaces do not fit a group ID")

	// A standard queue gets a group only when group_id asks (fair queues).
	s, _ = newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL, GroupID: "{{ .json.customer }}"}, fake)
	_, err = s.Deliver(context.Background(), newEnv(`{"customer":"c-2"}`))
	require.NoError(t, err)
	assert.Equal(t, "c-2", aws.ToString(fake.sends[3].MessageGroupId))
	assert.Nil(t, fake.sends[3].MessageDeduplicationId)
}

func TestSQSSinkErrors(t *testing.T) {
	fake := newFakeSQS()
	s, _ := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL, Body: "{{ .json.text }}",
		Headers: map[string]string{"x": "{{ index .json 1 }}"}}, fake)

	_, err := s.Deliver(context.Background(), newEnv(`{"text":""}`))
	class, _ := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.ErrorContains(t, err, "empty message body")

	_, err = s.Deliver(context.Background(), newEnv(`{"text":"hi"}`))
	class, tags := errorTags(err)
	assert.Equal(t, plugin.Permanent, class, "a template that fails will fail again")
	assert.Equal(t, []string{"template"}, tags)

	s, _ = newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL}, fake)
	fake.sendErr = apiError("AWS.SimpleQueueService.NonExistentQueue", 400, smithy.FaultClient)
	_, err = s.Deliver(context.Background(), newEnv("x"))
	class, tags = errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, tags, "sqs:AWS.SimpleQueueService.NonExistentQueue")
	assert.Contains(t, tags, "aws:AWS.SimpleQueueService.NonExistentQueue")

	fake.sendErr = apiError("RequestThrottled", 403, smithy.FaultClient)
	_, err = s.Deliver(context.Background(), newEnv("x"))
	class, tags = errorTags(err)
	assert.Equal(t, plugin.RateLimited, class)
	assert.Contains(t, tags, "429")
}

func TestSQSSinkBatch(t *testing.T) {
	fake := newFakeSQS()
	s, _ := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL + ".fifo"}, fake)
	var envs []*envelope.Envelope
	for i := range 23 {
		envs = append(envs, newEnv(strconv.Itoa(i)))
	}
	res, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	require.Len(t, fake.batchEntries, 3, "10 entries per call")
	assert.Len(t, fake.batchEntries[0], 10)
	assert.Len(t, fake.batchEntries[2], 3)
	assert.Len(t, res.Headers["sqs-message-id"], 23)
	assert.Len(t, res.Headers["sqs-sequence-number"], 23, "a FIFO queue numbers what it took")
	e := fake.batchEntries[1][4]
	assert.Equal(t, "4", aws.ToString(e.Id))
	assert.Equal(t, "14", aws.ToString(e.MessageBody))
	assert.Equal(t, envs[14].ID, aws.ToString(e.MessageDeduplicationId))
	assert.Equal(t, "orders-in", aws.ToString(e.MessageGroupId))

	// 256KiB per call.
	fake = newFakeSQS()
	s, _ = newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL}, fake)
	big := strings.Repeat("x", 100<<10)
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{newEnv(big), newEnv(big), newEnv(big), newEnv("small")})
	require.NoError(t, err)
	require.Len(t, fake.batchEntries, 2)
	assert.Len(t, fake.batchEntries[0], 2)
	assert.Len(t, fake.batchEntries[1], 2)
}

func TestSQSSinkBatchPartialFailureFailsTheBatch(t *testing.T) {
	fake := newFakeSQS()
	fake.failBodies = map[string]string{"bad": "InternalError"}
	s, _ := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL}, fake)
	_, err := s.DeliverBatch(context.Background(), []*envelope.Envelope{newEnv("ok"), newEnv("bad")})
	require.Error(t, err)
	class, tags := errorTags(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{"aws:InternalError", "sqs:InternalError", "5xx"}, tags)
	assert.ErrorContains(t, err, "1 of 2 entries failed")

	fake.failBodies = map[string]string{"bad": "InvalidMessageContents"}
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{newEnv("bad")})
	class, _ = errorTags(err)
	assert.Equal(t, plugin.Permanent, class)

	fake.failBodies = nil
	fake.sendErr = apiError("ServiceUnavailable", 503, smithy.FaultServer)
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{newEnv("x")})
	class, tags = errorTags(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Contains(t, tags, "503")
}

func TestSQSSinkHealth(t *testing.T) {
	fake := newFakeSQS()
	s, _ := newSQSSinkWith(t, &config.Endpoint{QueueURL: queueURL}, fake)
	require.NoError(t, s.Health(context.Background()))
	fake.attrsErr = apiError("QueueDoesNotExist", 400, smithy.FaultClient)
	err := s.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), queueURL)
	class, _ := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
}

func newSNSSinkWith(t *testing.T, ep *config.Endpoint, fake *fakeSNS) *snsSink {
	t.Helper()
	s, err := parseSNSSink(sinkSpec(kindSNS, ep))
	require.NoError(t, err)
	s.client = fake
	return s
}

func TestSNSSinkDeliver(t *testing.T) {
	fake := &fakeSNS{}
	s := newSNSSinkWith(t, &config.Endpoint{Topic: topicARN, Subject: "{{ .json.title }}", Headers: map[string]string{"tenant": "acme"}}, fake)
	env := newEnv(`{"title":"Order\nplaced` + strings.Repeat("!", 120) + `"}`)
	env.SetHeader("X-Trace", "t-1")
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, []string{"sns-1"}, res.Headers["sns-message-id"])
	assert.Equal(t, topicARN, res.Endpoint)
	require.Len(t, fake.publishes, 1)
	in := fake.publishes[0]
	assert.Equal(t, topicARN, aws.ToString(in.TopicArn))
	assert.Equal(t, string(env.Body), aws.ToString(in.Message))
	subject := aws.ToString(in.Subject)
	assert.Len(t, []rune(subject), 100, "at most 100 characters")
	assert.True(t, strings.HasPrefix(subject, "Order placed!"), "no line breaks")
	assert.Equal(t, "acme", aws.ToString(in.MessageAttributes["tenant"].StringValue))
	assert.Equal(t, "t-1", aws.ToString(in.MessageAttributes["X-Trace"].StringValue))
	assert.Equal(t, env.ID, aws.ToString(in.MessageAttributes["Idempotency-Key"].StringValue))
	assert.Nil(t, in.MessageGroupId)

	// No subject template, or one that renders empty: no subject.
	s = newSNSSinkWith(t, &config.Endpoint{Topic: topicARN, Subject: "{{ .json.missing }}"}, fake)
	_, err = s.Deliver(context.Background(), newEnv(`{}`))
	require.NoError(t, err)
	assert.Nil(t, fake.publishes[1].Subject)
	require.NoError(t, s.Close(context.Background()))
	assert.Equal(t, "out", s.Name())
}

func TestSNSSinkFIFOAndTemplatedTopics(t *testing.T) {
	fake := &fakeSNS{}
	s := newSNSSinkWith(t, &config.Endpoint{Topic: "arn:aws:sns:us-east-1:1:{{ .json.topic }}", GroupID: "{{ .json.g }}"}, fake)
	env := newEnv(`{"topic":"orders.fifo","g":"grp"}`)
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	in := fake.publishes[0]
	assert.Equal(t, "arn:aws:sns:us-east-1:1:orders.fifo", aws.ToString(in.TopicArn))
	assert.Equal(t, "grp", aws.ToString(in.MessageGroupId))
	assert.Equal(t, env.ID, aws.ToString(in.MessageDeduplicationId))
	assert.NotEmpty(t, res.Headers["sns-sequence-number"])

	_, err = s.Deliver(context.Background(), newEnv(`{"topic":"plain","g":"grp"}`))
	require.NoError(t, err)
	assert.Equal(t, "grp", aws.ToString(fake.publishes[1].MessageGroupId), "group_id set: sent on standard topics too")
	assert.Nil(t, fake.publishes[1].MessageDeduplicationId)

	s = newSNSSinkWith(t, &config.Endpoint{Topic: "{{ .json.topic }}"}, fake)
	_, err = s.Deliver(context.Background(), newEnv(`{"topic":"not-an-arn"}`))
	class, _ := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
	require.NoError(t, s.Health(context.Background()), "nothing to check before a message names the topic")
}

func TestSNSSinkBatch(t *testing.T) {
	fake := &fakeSNS{}
	s := newSNSSinkWith(t, &config.Endpoint{Topic: "arn:aws:sns:us-east-1:1:{{ .json.t }}"}, fake)
	var envs []*envelope.Envelope
	for i := range 12 {
		envs = append(envs, newEnv(fmt.Sprintf(`{"t":"%s","i":%d}`, []string{"a", "b"}[i%2], i)))
	}
	res, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	require.Len(t, fake.batches, 2, "one call per topic (6 each)")
	assert.Equal(t, "arn:aws:sns:us-east-1:1:a", aws.ToString(fake.batches[0].TopicArn))
	assert.Len(t, fake.batches[0].PublishBatchRequestEntries, 6)
	ids := res.Headers["sns-message-id"]
	require.Len(t, ids, 12)
	assert.Equal(t, "sns-1", ids[0], "IDs in the batch's order")
	assert.Equal(t, "sns-7", ids[1])
	assert.NotContains(t, res.Headers, "sns-sequence-number", "standard topics do not number messages")

	s = newSNSSinkWith(t, &config.Endpoint{Topic: topicARN + ".fifo"}, fake)
	res, err = s.DeliverBatch(context.Background(), envs[:2])
	require.NoError(t, err)
	assert.Len(t, res.Headers["sns-sequence-number"], 2)

	fake = &fakeSNS{failBodies: map[string]string{"bad": "Throttling"}}
	s = newSNSSinkWith(t, &config.Endpoint{Topic: topicARN}, fake)
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{newEnv("ok"), newEnv("bad")})
	class, tags := errorTags(err)
	assert.Equal(t, plugin.RateLimited, class)
	assert.Contains(t, tags, "sns:Throttling")

	fake = &fakeSNS{err: apiError("NotFound", 404, smithy.FaultClient)}
	s = newSNSSinkWith(t, &config.Endpoint{Topic: topicARN}, fake)
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{newEnv("ok")})
	class, _ = errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
}

func TestSNSSinkHealth(t *testing.T) {
	fake := &fakeSNS{}
	s := newSNSSinkWith(t, &config.Endpoint{Topic: topicARN}, fake)
	require.NoError(t, s.Health(context.Background()))
	fake.attrsErr = apiError("AuthorizationError", 403, smithy.FaultClient)
	err := s.Health(context.Background())
	assert.ErrorContains(t, err, topicARN)
	class, _ := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
}

func newEventBridgeSinkWith(t *testing.T, ep *config.Endpoint, fake *fakeEventBridge) *eventBridgeSink {
	t.Helper()
	s, err := parseEventBridgeSink(sinkSpec(kindEventBridge, ep))
	require.NoError(t, err)
	s.client = fake
	return s
}

func TestEventBridgeSinkDeliver(t *testing.T) {
	fake := &fakeEventBridge{}
	s := newEventBridgeSinkWith(t, &config.Endpoint{}, fake)
	env := newEnv(` {"order":7} `)
	env.Subject = "orders/placed"
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, []string{"ev-1"}, res.Headers["eventbridge-event-id"])
	assert.Equal(t, "eventbridge:default", res.Endpoint)
	e := fake.puts[0].Entries[0]
	assert.Equal(t, "hikyaku.relay", aws.ToString(e.Source), "hikyaku.<relay> by default")
	assert.Equal(t, "orders/placed", aws.ToString(e.DetailType), "the subject by default")
	assert.Equal(t, `{"order":7}`, aws.ToString(e.Detail), "a JSON object is the detail")
	assert.Nil(t, e.EventBusName, "the default bus")

	for body, want := range map[string]string{
		"plain text": `{"body":"plain text"}`,
		`[1,2]`:      `{"body":"[1,2]"}`,
		"\xff\x00":   `{"body_base64":"/wA="}`,
	} {
		_, err := s.Deliver(context.Background(), envelope.New("in", "http", []byte(body)))
		require.NoError(t, err)
		last := fake.puts[len(fake.puts)-1].Entries[0]
		assert.Equal(t, want, aws.ToString(last.Detail))
		assert.Equal(t, "hikyaku.message", aws.ToString(last.DetailType), "without a subject")
	}

	s = newEventBridgeSinkWith(t, &config.Endpoint{EventBus: "orders-bus", EventSource: "shop.orders",
		DetailType: "Order {{ .json.kind }}"}, fake)
	res, err = s.Deliver(context.Background(), newEnv(`{"kind":"Placed"}`))
	require.NoError(t, err)
	e = fake.puts[len(fake.puts)-1].Entries[0]
	assert.Equal(t, "orders-bus", aws.ToString(e.EventBusName))
	assert.Equal(t, "shop.orders", aws.ToString(e.Source))
	assert.Equal(t, "Order Placed", aws.ToString(e.DetailType))
	assert.Equal(t, "eventbridge:orders-bus", res.Endpoint)

	s = newEventBridgeSinkWith(t, &config.Endpoint{DetailType: "{{ .json.t }}"}, fake)
	_, err = s.Deliver(context.Background(), newEnv(fmt.Sprintf(`{"t":%q}`, strings.Repeat("d", 200))))
	require.NoError(t, err)
	assert.Len(t, aws.ToString(fake.puts[len(fake.puts)-1].Entries[0].DetailType), 128)
	require.NoError(t, s.Close(context.Background()))
	assert.Equal(t, "out", s.Name())
}

func TestEventBridgeSinkFailedEntries(t *testing.T) {
	fake := &fakeEventBridge{failDetails: map[string]string{`{"bad":true}`: "InternalFailure"}}
	s := newEventBridgeSinkWith(t, &config.Endpoint{}, fake)
	_, err := s.Deliver(context.Background(), newEnv(`{"bad":true}`))
	class, tags := errorTags(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{"aws:InternalFailure", "eventbridge:InternalFailure", "5xx"}, tags)

	fake.failDetails = map[string]string{`{"bad":true}`: "ThrottlingException"}
	_, err = s.Deliver(context.Background(), newEnv(`{"bad":true}`))
	class, tags = errorTags(err)
	assert.Equal(t, plugin.RateLimited, class)
	assert.Contains(t, tags, "eventbridge:ThrottlingException")

	fake.failDetails = map[string]string{`{"bad":true}`: "MalformedDetail"}
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{newEnv(`{"ok":1}`), newEnv(`{"bad":true}`)})
	class, _ = errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.ErrorContains(t, err, "1 of 2 entries failed")

	fake.err = apiError("ResourceNotFoundException", 400, smithy.FaultClient)
	_, err = s.Deliver(context.Background(), newEnv(`{}`))
	class, tags = errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, tags, "eventbridge:ResourceNotFoundException")
}

func TestEventBridgeSinkBatch(t *testing.T) {
	fake := &fakeEventBridge{}
	s := newEventBridgeSinkWith(t, &config.Endpoint{}, fake)
	var envs []*envelope.Envelope
	for i := range 25 {
		envs = append(envs, newEnv(fmt.Sprintf(`{"i":%d}`, i)))
	}
	res, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	require.Len(t, fake.puts, 3)
	assert.Len(t, fake.puts[0].Entries, 10)
	assert.Len(t, fake.puts[2].Entries, 5)
	assert.Len(t, res.Headers["eventbridge-event-id"], 25)
}

func TestEventBridgeSinkHealth(t *testing.T) {
	fake := &fakeEventBridge{}
	s := newEventBridgeSinkWith(t, &config.Endpoint{}, fake)
	require.NoError(t, s.Health(context.Background()))
	s = newEventBridgeSinkWith(t, &config.Endpoint{EventBus: "orders-bus"}, fake)
	require.NoError(t, s.Health(context.Background()))
	assert.Equal(t, []string{"default", "orders-bus"}, fake.described)
	fake.describeErr = apiError("ResourceNotFoundException", 400, smithy.FaultClient)
	assert.ErrorContains(t, s.Health(context.Background()), "orders-bus")
}

func TestRegisterAddsTheKinds(t *testing.T) {
	Register()
	for _, kind := range []string{kindSQS, kindSNS, kindEventBridge, kindS3} {
		_, ok := plugin.LookupSource(kind)
		assert.True(t, ok, kind)
		_, ok = plugin.LookupSink(kind)
		assert.True(t, ok, kind)
	}
}

func TestFactoriesBuildClients(t *testing.T) {
	hermeticAWS(t)
	Register()
	ctx := context.Background()
	for _, kind := range []string{kindSQS, kindSNS, kindEventBridge} {
		src, err := plugin.NewSource(ctx, sourceSpec(kind, &config.Source{QueueURL: queueURL, AWS: &config.AWS{AccessKeyID: "a", SecretAccessKey: "b"}}))
		require.NoError(t, err, kind)
		assert.Equal(t, "in", src.Name())
		_, ok := src.(plugin.Closer)
		assert.True(t, ok, "%s acks need the client after Start returns", kind)
	}
	for kind, ep := range map[string]*config.Endpoint{
		kindSQS:         {QueueURL: queueURL},
		kindSNS:         {Topic: topicARN},
		kindEventBridge: {EventBus: "arn:aws:events:us-east-1:1:event-bus/b"},
		kindS3:          {Bucket: "b", AWS: testAWS("http://localhost:4566")},
	} {
		if ep.AWS == nil {
			ep.AWS = &config.AWS{AccessKeyID: "a", SecretAccessKey: "b"}
		}
		sink, err := plugin.NewSink(ctx, sinkSpec(kind, ep))
		require.NoError(t, err, kind)
		_, ok := sink.(plugin.BatchSink)
		assert.True(t, ok, "%s has a native batch call", kind)
	}
	_, err := plugin.NewSink(ctx, sinkSpec(kindS3, &config.Endpoint{Bucket: "b"}))
	assert.ErrorContains(t, err, "no region")
	_, err = plugin.NewSource(ctx, sourceSpec(kindSQS, &config.Source{QueueURL: "http://localhost:4566/000000000000/q"}))
	assert.ErrorContains(t, err, "no region")
	_, err = plugin.NewSink(ctx, sinkSpec(kindSQS, &config.Endpoint{}))
	assert.ErrorContains(t, err, "queue_url is required")
	_, err = plugin.NewSource(ctx, sourceSpec(kindS3, &config.Source{}))
	assert.ErrorContains(t, err, "bucket is required")
}
