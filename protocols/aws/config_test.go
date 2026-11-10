package aws

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

const queueURL = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"

func TestParseQueueSourceDefaults(t *testing.T) {
	src, o, err := parseQueueSource(sourceSpec(kindSQS, &config.Source{QueueURL: queueURL}))
	require.NoError(t, err)
	assert.Equal(t, queueURL, src.QueueURL)
	assert.Equal(t, "orders", o.queueName)
	assert.Equal(t, int32(20), o.wait)
	assert.Equal(t, int32(10), o.prefetch)
	assert.Zero(t, o.visibility, "the queue's own")
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.Equal(t, reconnectOptions{backoff: 500 * time.Millisecond, maxBackoff: 30 * time.Second, jitter: "full"}, o.reconnect)

	_, o, err = parseQueueSource(sourceSpec(kindSNS, &config.Source{
		QueueURL: queueURL, Mode: "subscribe", WaitTime: config.Duration(1500 * time.Millisecond), Prefetch: 3,
		VisibilityTimeout: config.Duration(2 * time.Minute), MaxBodyBytes: 1024, Ack: "accepted",
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(time.Millisecond), Jitter: "none", MaxAttempts: 4},
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(2), o.wait, "rounded up to whole seconds")
	assert.Equal(t, int32(3), o.prefetch)
	assert.Equal(t, int32(120), o.visibility)
	assert.Equal(t, int64(1024), o.maxBytes)
	assert.Equal(t, "accepted", o.ack)
	assert.Equal(t, reconnectOptions{backoff: time.Second, maxBackoff: time.Second, jitter: "none", maxAttempts: 4}, o.reconnect)
}

func TestParseQueueSourceErrors(t *testing.T) {
	for want, src := range map[string]*config.Source{
		"queue_url is required":            {},
		"is not an SQS queue URL":          {QueueURL: "sqs://nope"},
		"not an SQS queue URL (https":      {QueueURL: "https://sqs.us-east-1.amazonaws.com/"},
		"subscribe (read an SQS queue)":    {QueueURL: queueURL, Mode: "poll"},
		"wait_time must be at most 20s":    {QueueURL: queueURL, WaitTime: config.Duration(21 * time.Second)},
		"prefetch must be 1 to 10":         {QueueURL: queueURL, Prefetch: 11},
		"visibility_timeout must be at":    {QueueURL: queueURL, VisibilityTimeout: config.Duration(13 * time.Hour)},
		"max_body_bytes must not be":       {QueueURL: queueURL, MaxBodyBytes: -1},
		"reconnect.jitter must be":         {QueueURL: queueURL, Reconnect: &config.Reconnect{Jitter: "some"}},
		"reconnect.max_attempts must not":  {QueueURL: queueURL, Reconnect: &config.Reconnect{MaxAttempts: -1}},
		"options: applies to plugin types": {QueueURL: queueURL, Options: map[string]any{"x": 1}},
	} {
		_, _, err := parseQueueSource(sourceSpec(kindSQS, src))
		require.Error(t, err, want)
		assert.Contains(t, err.Error(), want)
		assert.Contains(t, err.Error(), "source in:")
	}
	_, _, err := parseQueueSource(plugin.Spec{Name: "x", Kind: kindSQS})
	assert.ErrorContains(t, err, "missing config")
}

func TestParseS3Source(t *testing.T) {
	_, o, err := parseS3Source(sourceSpec(kindS3, &config.Source{Bucket: "b"}))
	require.NoError(t, err)
	assert.Equal(t, afterDelete, o.after)
	assert.Equal(t, config.DefaultPollInterval, o.interval)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.False(t, o.lines)
	assert.Nil(t, o.split)
	assert.Equal(t, 3, o.retry.Attempts)

	_, o, err = parseS3Source(sourceSpec(kindS3, &config.Source{Bucket: "b", Prefix: "in/", After: "move", MoveTo: "done/",
		Split: "lines", Interval: config.Duration(time.Second), StateStore: "redis"}))
	require.NoError(t, err)
	assert.Equal(t, afterMove, o.after)
	assert.Equal(t, "done/", o.moveTo)
	assert.True(t, o.lines)
	assert.Equal(t, time.Second, o.interval)

	_, o, err = parseS3Source(sourceSpec(kindS3, &config.Source{Bucket: "b", Split: ".records[]", After: "keep"}))
	require.NoError(t, err)
	require.NotNil(t, o.split, "anything but lines is a jq split, as for http poll")
	assert.Equal(t, ".records[]", o.split.String())

	_, o, err = parseS3Source(sourceSpec(kindS3, &config.Source{Bucket: "b", Timeout: config.Duration(3 * time.Second)}))
	require.NoError(t, err)
	s := buildS3Source(sourceSpec(kindS3, &config.Source{Bucket: "b"}), o, nil, nil)
	assert.Equal(t, 3*time.Second, s.callTimeout, "timeout bounds each S3 call")
	assert.Equal(t, defaultReadTimeout, s.readTimeout, "a download keeps its longer bound")
	s = buildS3Source(sourceSpec(kindS3, &config.Source{Bucket: "b"}), s3SourceOptions{}, nil, nil)
	assert.Equal(t, defaultCallTimeout, s.callTimeout, "the default without timeout")

	for want, src := range map[string]*config.Source{
		"bucket is required":                 {},
		"is a bucket name":                   {Bucket: "b/key"},
		"poll a bucket":                      {Bucket: "b", Mode: "subscribe"},
		"after must be keep, delete or move": {Bucket: "b", After: "archive"},
		"move_to is required":                {Bucket: "b", After: "move"},
		"move_to applies with after: move":   {Bucket: "b", MoveTo: "done/"},
		"would look moved already":           {Bucket: "b", Prefix: "in/", After: "move", MoveTo: "in/"},
		"is under move_to":                   {Bucket: "b", Prefix: "data/in/", After: "move", MoveTo: "data/"},
		"split must be lines or a jq":        {Bucket: "b", Split: ".[["},
		"interval must not be negative":      {Bucket: "b", Interval: config.Duration(-time.Second)},
		"max_body_bytes must not be":         {Bucket: "b", MaxBodyBytes: -1},
	} {
		_, _, err := parseS3Source(sourceSpec(kindS3, src))
		require.Error(t, err, want)
		assert.Contains(t, err.Error(), want)
	}
}

func TestSplitLinesIsNoJQProgram(t *testing.T) {
	// lines cannot be mistaken for a jq split: jq has no such function.
	_, err := transform.CompileJQ(splitLines)
	assert.Error(t, err)
}

func TestNewS3SourceKeepNeedsAStore(t *testing.T) {
	hermeticAWS(t)
	spec := sourceSpec(kindS3, &config.Source{Bucket: "b", After: "keep", AWS: testAWS("")})
	_, err := newS3Source(context.Background(), spec)
	assert.ErrorContains(t, err, "after: keep needs a store")

	spec.Env.Store = func(name string) (store.Store, error) {
		return nil, assert.AnError
	}
	_, err = newS3Source(context.Background(), spec)
	assert.ErrorContains(t, err, "state_store")

	src, err := newS3Source(context.Background(), withStore(spec, store.NewMemory("m")))
	require.NoError(t, err)
	assert.NotNil(t, src.kv)
}

func TestSinkConfigErrors(t *testing.T) {
	for _, c := range []struct {
		kind string
		ep   *config.Endpoint
		want string
	}{
		{kindSQS, &config.Endpoint{}, "queue_url is required"},
		{kindSQS, &config.Endpoint{QueueURL: "{{ .json.q }}"}, "queue_url cannot be a per-message template"},
		{kindSQS, &config.Endpoint{QueueURL: queueURL, Delay: config.Duration(16 * time.Minute)}, "delay must be at most 15m"},
		{kindSQS, &config.Endpoint{QueueURL: queueURL + ".fifo", Delay: config.Duration(time.Second)}, "delay applies to standard queues"},
		{kindSQS, &config.Endpoint{QueueURL: queueURL, Body: "{{ .json"}, "body"},
		{kindSQS, &config.Endpoint{QueueURL: queueURL, Headers: map[string]string{"x": "{{ .nope"}}, "headers.x"},
		{kindSQS, &config.Endpoint{QueueURL: queueURL, GroupID: "{{ end }}"}, "group_id"},
		{kindSQS, &config.Endpoint{QueueURL: queueURL, Options: map[string]any{"a": 1}}, "options: applies to plugin types"},
		{kindSNS, &config.Endpoint{}, "topic is required"},
		{kindSNS, &config.Endpoint{Topic: "orders"}, "is not a topic ARN"},
		{kindSNS, &config.Endpoint{Topic: "arn:aws:sns:us-east-1:1:t", Subject: "{{ if }}"}, "subject"},
		{kindEventBridge, &config.Endpoint{EventBus: "{{ .json.bus }}"}, "event_bus cannot be a per-message template"},
		{kindEventBridge, &config.Endpoint{EventSource: "aws.s3"}, "reserved for AWS services"},
		{kindEventBridge, &config.Endpoint{DetailType: "{{ x"}, "detail_type"},
		{kindEventBridge, &config.Endpoint{Headers: map[string]string{"a": "b"}}, "events carry no headers"},
		{kindS3, &config.Endpoint{}, "bucket is required"},
		{kindS3, &config.Endpoint{Bucket: "b/k"}, "is a bucket name"},
		{kindS3, &config.Endpoint{Bucket: "{{ .json.b }}"}, "bucket cannot be a per-message template"},
		{kindS3, &config.Endpoint{Bucket: "b", Key: "{{ .x"}, "key"},
	} {
		spec := sinkSpec(c.kind, c.ep)
		var err error
		switch c.kind {
		case kindSQS:
			_, err = parseSQSSink(spec)
		case kindSNS:
			_, err = parseSNSSink(spec)
		case kindEventBridge:
			_, err = parseEventBridgeSink(spec)
		case kindS3:
			_, err = parseS3Sink(spec)
		}
		require.Error(t, err, c.want)
		assert.Contains(t, err.Error(), c.want)
		assert.Contains(t, err.Error(), "destination out:")
	}
}

func TestLoadAWSRegionAndStaticCredentials(t *testing.T) {
	hermeticAWS(t)
	ctx := context.Background()

	cfg, err := loadAWS(ctx, &config.AWS{Region: "eu-west-1", AccessKeyID: "AKID", SecretAccessKey: "SECRET", SessionToken: "TOKEN"}, "us-east-1", 1, "s")
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", cfg.Region, "the block wins over the hint")
	creds, err := cfg.Credentials.Retrieve(ctx)
	require.NoError(t, err)
	assert.Equal(t, "AKID", creds.AccessKeyID)
	assert.Equal(t, "SECRET", creds.SecretAccessKey)
	assert.Equal(t, "TOKEN", creds.SessionToken)
	assert.Nil(t, cfg.BaseEndpoint)
	assert.Equal(t, 1, sqsClientAttempts(cfg), "deliveries make one attempt; the engine retries")

	cfg, err = loadAWS(ctx, testAWS(""), "", 0, "s")
	require.NoError(t, err)
	assert.Nil(t, cfg.Retryer, "sources keep the SDK's retries")
	assert.Equal(t, retry.DefaultMaxAttempts, sqsClientAttempts(cfg))

	cfg, err = loadAWS(ctx, &config.AWS{AccessKeyID: "a", SecretAccessKey: "b"}, "ap-south-1", 1, "s")
	require.NoError(t, err)
	assert.Equal(t, "ap-south-1", cfg.Region, "the queue URL's or ARN's region when nothing else names one")

	t.Setenv("AWS_REGION", "sa-east-1")
	cfg, err = loadAWS(ctx, &config.AWS{AccessKeyID: "a", SecretAccessKey: "b"}, "ap-south-1", 1, "s")
	require.NoError(t, err)
	assert.Equal(t, "sa-east-1", cfg.Region, "the SDK's chain before the hint")
	t.Setenv("AWS_REGION", "")

	_, err = loadAWS(ctx, &config.AWS{AccessKeyID: "a", SecretAccessKey: "b"}, "", 1, "s")
	assert.ErrorContains(t, err, "no region")
	_, err = loadAWS(ctx, &config.AWS{Region: "us-east-1", AccessKeyID: "a"}, "", 1, "s")
	assert.ErrorContains(t, err, "access_key_id and secret_access_key go together")
	_, err = loadAWS(ctx, &config.AWS{Region: "us-east-1", SessionToken: "t"}, "", 1, "s")
	assert.ErrorContains(t, err, "session_token needs")
	_, err = loadAWS(ctx, &config.AWS{Region: "us-east-1", Endpoint: "localhost:4566"}, "", 1, "s")
	assert.ErrorContains(t, err, "must be an http:// or https:// URL")
	_, err = loadAWS(ctx, &config.AWS{Region: "us-east-1", RoleARN: "role/x"}, "", 1, "s")
	assert.ErrorContains(t, err, "is not an ARN")
	_, err = loadAWS(ctx, &config.AWS{Region: "us-east-1", Profile: "missing"}, "", 1, "s")
	assert.ErrorContains(t, err, "missing")
}

func TestLoadAWSEndpointAndProfile(t *testing.T) {
	hermeticAWS(t)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config"), []byte("[profile relay]\nregion = eu-central-1\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "credentials"), []byte("[relay]\naws_access_key_id = PROFILEKEY\naws_secret_access_key = PROFILESECRET\n"), 0o600))
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))

	cfg, err := loadAWS(context.Background(), &config.AWS{Profile: "relay", Endpoint: "http://localhost:4566"}, "", 1, "s")
	require.NoError(t, err)
	assert.Equal(t, "eu-central-1", cfg.Region)
	assert.Equal(t, "http://localhost:4566", aws.ToString(cfg.BaseEndpoint))
	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "PROFILEKEY", creds.AccessKeyID)
}

// fakeSTS answers AssumeRole, the way STS does (query protocol).
type fakeSTS struct {
	mu    sync.Mutex
	forms []url.Values
}

func (f *fakeSTS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	form, _ := url.ParseQuery(string(body))
	f.mu.Lock()
	f.forms = append(f.forms, form)
	f.mu.Unlock()
	w.Header().Set("Content-Type", "text/xml")
	_, _ = io.WriteString(w, `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIAROLE</AccessKeyId>
      <SecretAccessKey>rolesecret</SecretAccessKey>
      <SessionToken>roletoken</SessionToken>
      <Expiration>2099-01-01T00:00:00Z</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/relay/s</Arn>
      <AssumedRoleId>AROAEXAMPLE:s</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>
</AssumeRoleResponse>`)
}

func TestLoadAWSAssumesTheRole(t *testing.T) {
	hermeticAWS(t)
	sts := &fakeSTS{}
	srv := httptest.NewServer(sts)
	defer srv.Close()
	a := testAWS(srv.URL)
	a.RoleARN = "arn:aws:iam::123456789012:role/relay"
	spec := plugin.Spec{Name: "orders in", Env: plugin.Env{Relay: "billing"}}
	cfg, err := loadAWS(context.Background(), a, "", 1, roleSessionName(spec))
	require.NoError(t, err)
	creds, err := cfg.Credentials.Retrieve(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "ASIAROLE", creds.AccessKeyID)
	assert.Equal(t, "roletoken", creds.SessionToken)

	sts.mu.Lock()
	defer sts.mu.Unlock()
	require.Len(t, sts.forms, 1, "the endpoint override reaches STS too")
	assert.Equal(t, "AssumeRole", sts.forms[0].Get("Action"))
	assert.Equal(t, a.RoleARN, sts.forms[0].Get("RoleArn"))
	assert.Equal(t, "hikyaku-billing-orders-in", sts.forms[0].Get("RoleSessionName"))
}

// sqsClientAttempts is the retry budget an SQS client built from cfg gets.
func sqsClientAttempts(cfg aws.Config) int {
	return sqs.NewFromConfig(cfg).Options().Retryer.MaxAttempts()
}

func TestRoleSessionName(t *testing.T) {
	assert.Equal(t, "hikyaku-relay-in", roleSessionName(plugin.Spec{Name: "in", Env: plugin.Env{Relay: "relay"}}))
	assert.Equal(t, "hikyaku-hikyaku-a-b-c", roleSessionName(plugin.Spec{Name: "a/b c"}))
	long := roleSessionName(plugin.Spec{Name: strings.Repeat("x", 100)})
	assert.Len(t, long, roleSessionMaxName)
}

func TestRegionHints(t *testing.T) {
	for raw, want := range map[string]string{
		"https://sqs.eu-west-1.amazonaws.com/1/q":                    "eu-west-1",
		"https://us-gov-west-1.queue.amazonaws.com/1/q":              "us-gov-west-1",
		"http://sqs.us-east-1.localhost.localstack.cloud:4566/000/q": "us-east-1",
		"http://localhost:4566/000000000000/q":                       "",
		"https://vpce-1.sqs.us-east-1.vpce.amazonaws.com/1/q":        "",
		"::not a url": "",
		"https://sqs.cn-north-1.amazonaws.com.cn/123456789012/orders": "cn-north-1",
	} {
		assert.Equal(t, want, regionFromQueueURL(raw), raw)
	}
	assert.Equal(t, "us-east-2", regionFromARN("arn:aws:sns:us-east-2:123456789012:orders"))
	assert.Equal(t, "eu-west-3", regionFromARN("arn:aws:events:eu-west-3:123456789012:event-bus/b"))
	assert.Equal(t, "", regionFromARN("my-bus"))
	assert.Equal(t, "", regionFromARN("arn:aws:iam::123456789012:role/x"))
}

func TestSeconds(t *testing.T) {
	assert.Equal(t, int32(0), seconds(0))
	assert.Equal(t, int32(1), seconds(time.Millisecond))
	assert.Equal(t, int32(2), seconds(1500*time.Millisecond))
	assert.Equal(t, int32(20), seconds(20*time.Second))
}
