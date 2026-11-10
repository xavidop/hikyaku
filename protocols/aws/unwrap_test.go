package aws

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

const topicARN = "arn:aws:sns:us-east-1:123456789012:orders"

func snsBody(t *testing.T, typ, message string, subject *string, attrs map[string]snsAttribute) string {
	t.Helper()
	n := map[string]any{
		"Type": typ, "MessageId": "sns-msg-1", "TopicArn": topicARN, "Message": message,
		"Timestamp": "2026-11-09T10:00:00.000Z", "SignatureVersion": "1",
	}
	if subject != nil {
		n["Subject"] = *subject
	}
	if attrs != nil {
		n["MessageAttributes"] = attrs
	}
	b, err := json.Marshal(n)
	require.NoError(t, err)
	return string(b)
}

func TestSNSSourceUnwrapsNotifications(t *testing.T) {
	fake := newFakeSQS()
	subject := "Order placed"
	fake.put(snsBody(t, "Notification", `{"order":7}`, &subject, map[string]snsAttribute{
		"tenant":       {Type: "String", Value: "acme"},
		"priority":     {Type: "Number", Value: "2"},
		"blob":         {Type: "Binary", Value: "AAEC"},
		"content-type": {Type: "String", Value: "application/vnd.order+json"},
	}), nil)
	fake.put(snsBody(t, "Notification", base64.StdEncoding.EncodeToString([]byte{0, 1, 2}), nil, map[string]snsAttribute{
		"content-transfer-encoding": {Type: "String", Value: "base64"},
		"Idempotency-Key":           {Type: "String", Value: "idem-9"},
	}), nil)
	r := runQueue(t, kindSNS, fake, &config.Source{}, nil)

	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, `{"order":7}`, string(env.Body))
	assert.Equal(t, "sns-msg-1", env.ID, "the SNS message ID")
	assert.Equal(t, "Order placed", env.Subject)
	assert.Equal(t, "application/vnd.order+json", env.ContentType)
	assert.Equal(t, map[string][]string{"tenant": {"acme"}, "priority": {"2"}}, env.Headers)
	assert.Equal(t, topicARN, env.Attributes["sns.topic_arn"])
	assert.Equal(t, "sns-msg-1", env.Attributes["sns.message_id"])
	assert.Equal(t, "msg-1", env.Attributes["sqs.message_id"], "the SQS attributes stay")
	assert.Equal(t, kindSNS, env.Protocol)
	v, err := env.JSON()
	require.NoError(t, err, "the cached JSON view follows the new body")
	assert.Equal(t, map[string]any{"order": float64(7)}, v)

	env = recv(t, r.out, 2*time.Second)
	assert.Equal(t, []byte{0, 1, 2}, env.Body)
	assert.Equal(t, "idem-9", env.ID, "an Idempotency-Key attribute wins")
	assert.Equal(t, "orders", env.Subject, "the topic name without a subject")
	assert.Equal(t, contentBinary, env.ContentType)
	assert.Equal(t, map[string][]string{"Idempotency-Key": {"idem-9"}}, env.Headers)
}

func TestSNSSourceDropsSubscriptionConfirmations(t *testing.T) {
	fake := newFakeSQS()
	fake.put(snsBody(t, "SubscriptionConfirmation", "You have chosen to subscribe", nil, nil), nil)
	fake.put(snsBody(t, "UnsubscribeConfirmation", "bye", nil, nil), nil)
	fake.put("raw delivery body", map[string]string{"tenant": "acme"})
	fake.put(`{"Type":"Notification","note":"not from SNS"}`, nil)
	r := runQueue(t, kindSNS, fake, &config.Source{}, nil)

	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, "raw delivery body", string(env.Body), "raw message delivery passes through")
	assert.Equal(t, map[string][]string{"tenant": {"acme"}}, env.Headers)
	assert.NotContains(t, env.Attributes, "sns.topic_arn")
	ack(env, envelope.Delivered)

	env = recv(t, r.out, 2*time.Second)
	assert.Equal(t, `{"Type":"Notification","note":"not from SNS"}`, string(env.Body))
	ack(env, envelope.Delivered)

	require.Eventually(t, func() bool { return fake.remaining() == 0 }, 2*time.Second, 5*time.Millisecond,
		"the confirmations are deleted")
	assert.Contains(t, r.logs.String(), "SubscriptionConfirmation")
	assert.Contains(t, r.logs.String(), "UnsubscribeConfirmation")
}

func TestEventBridgeSourceUnwrapsEvents(t *testing.T) {
	fake := newFakeSQS()
	fake.put(`{"version":"0","id":"ev-1","detail-type":"Order Placed","source":"shop.orders","account":"123456789012",`+
		`"time":"2026-11-09T10:00:00Z","region":"eu-west-1","resources":["arn:aws:s3:::b"],"detail":{"order":7,"total":12.5}}`, nil)
	fake.put(`{"id":"x","detail-type":"t","source":"s","detail":"not an object"}`, nil)
	fake.put(`{"order":8}`, nil)
	r := runQueue(t, kindEventBridge, fake, &config.Source{}, nil)

	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, `{"order":7,"total":12.5}`, string(env.Body))
	assert.Equal(t, "ev-1", env.ID)
	assert.Equal(t, "Order Placed", env.Subject)
	assert.Equal(t, contentJSON, env.ContentType)
	assert.Equal(t, "shop.orders", env.Attributes["eventbridge.source"])
	assert.Equal(t, "ev-1", env.Attributes["eventbridge.id"])
	assert.Equal(t, "2026-11-09T10:00:00Z", env.Attributes["eventbridge.time"])
	assert.Equal(t, "123456789012", env.Attributes["eventbridge.account"])
	assert.Equal(t, "eu-west-1", env.Attributes["eventbridge.region"])
	assert.Equal(t, []string{"arn:aws:s3:::b"}, env.Attributes["eventbridge.resources"])

	env = recv(t, r.out, 2*time.Second)
	assert.Equal(t, `"not an object"`, string(env.Body), "any detail is the body")
	assert.Equal(t, "x", env.ID)

	env = recv(t, r.out, 2*time.Second)
	assert.Equal(t, `{"order":8}`, string(env.Body), "an input transformer's output passes through")
	assert.Equal(t, "msg-3", env.ID)
	assert.NotContains(t, env.Attributes, "eventbridge.id")
}

func TestLooksLikeObject(t *testing.T) {
	assert.True(t, looksLikeObject([]byte(" {\"a\":1}\n")))
	assert.False(t, looksLikeObject([]byte("[1]")))
	assert.False(t, looksLikeObject([]byte("{")))
	assert.False(t, looksLikeObject(nil))
}
