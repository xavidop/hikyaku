package aws

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Modes.
const (
	modeSubscribe = "subscribe"
	modePoll      = "poll"
)

// Defaults and limits.
const (
	defaultWaitTime    = 20 * time.Second
	maxWaitTime        = 20 * time.Second
	defaultPrefetch    = 10
	maxVisibility      = 12 * time.Hour
	maxDelay           = 15 * time.Minute
	defaultBackoff     = 500 * time.Millisecond
	defaultMaxBackoff  = 30 * time.Second
	defaultJitter      = "full"
	afterDelete        = "delete"
	afterMove          = "move"
	afterKeep          = "keep"
	splitLines         = "lines"
	sinkAttempts       = 1 // the engine owns delivery retries
	sourceAttempts     = 0 // 0: the SDK's standard retries for receives, deletes and lists
	roleSessionMaxName = 64
	// roleRefreshEarly renews assumed-role credentials this long before
	// they expire, so no call goes out with credentials about to lapse.
	roleRefreshEarly = time.Minute
)

// loadAWS builds the SDK configuration of one source or destination: the
// SDK's default chain (environment, shared config, IMDS...), then the aws
// block on top. attempts > 0 caps the SDK's retries (1: none); hint is a
// region read from the queue URL or ARN, used when nothing else names one.
func loadAWS(ctx context.Context, a *config.AWS, hint string, attempts int, sessionName string) (aws.Config, error) {
	if a == nil {
		a = &config.AWS{}
	}
	var opts []func(*awsconfig.LoadOptions) error
	if a.Region != "" {
		opts = append(opts, awsconfig.WithRegion(a.Region))
	}
	if a.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(a.Profile))
	}
	switch {
	case (a.AccessKeyID == "") != (a.SecretAccessKey == ""):
		return aws.Config{}, fmt.Errorf("aws: access_key_id and secret_access_key go together")
	case a.SessionToken != "" && a.AccessKeyID == "":
		return aws.Config{}, fmt.Errorf("aws: session_token needs access_key_id and secret_access_key")
	case a.AccessKeyID != "":
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(a.AccessKeyID, a.SecretAccessKey, a.SessionToken)))
	}
	if a.Endpoint != "" {
		if err := checkEndpoint(a.Endpoint); err != nil {
			return aws.Config{}, err
		}
		opts = append(opts, awsconfig.WithBaseEndpoint(a.Endpoint))
	}
	if attempts > 0 {
		opts = append(opts, awsconfig.WithRetryer(func() aws.Retryer {
			return retry.AddWithMaxAttempts(retry.NewStandard(), attempts)
		}))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("aws: %w", err)
	}
	if cfg.Region == "" {
		cfg.Region = hint
	}
	if cfg.Region == "" {
		return aws.Config{}, fmt.Errorf("aws: no region: set aws.region (or AWS_REGION, or a profile with a region)")
	}
	if a.RoleARN != "" {
		if !strings.HasPrefix(a.RoleARN, "arn:") {
			return aws.Config{}, fmt.Errorf("aws: role_arn %q is not an ARN (arn:aws:iam::<account>:role/<name>)", a.RoleARN)
		}
		provider := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), a.RoleARN, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = sessionName
		})
		cfg.Credentials = aws.NewCredentialsCache(provider, func(o *aws.CredentialsCacheOptions) {
			o.ExpiryWindow = roleRefreshEarly
		})
	}
	return cfg, nil
}

// checkEndpoint checks a custom endpoint: an http or https URL with a host.
func checkEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("aws: endpoint %q must be an http:// or https:// URL, such as http://localhost:4566", redactURL(raw))
	}
	return nil
}

// redactURL hides a URL's password, for errors and logs.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(invalid url)"
	}
	return u.Redacted()
}

// sessionNameChars is what an STS role session name may hold.
var sessionNameChars = regexp.MustCompile(`[^\w+=,.@-]`)

// roleSessionName names the assumed-role session of a source or
// destination: hikyaku-<relay>-<name>, cleaned up for STS.
func roleSessionName(spec plugin.Spec) string {
	n := sessionNameChars.ReplaceAllString("hikyaku-"+relayName(spec)+"-"+spec.Name, "-")
	if len(n) > roleSessionMaxName {
		n = n[:roleSessionMaxName]
	}
	return n
}

// regionPattern matches an AWS region name, such as us-east-1.
var regionPattern = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d+$`)

// regionFromQueueURL reads the region of an SQS queue URL:
// https://sqs.<region>.amazonaws.com/..., https://<region>.queue.amazonaws.com/...
// or LocalStack's http://sqs.<region>.localhost.localstack.cloud:4566/....
func regionFromQueueURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	labels := strings.Split(u.Hostname(), ".")
	switch {
	case len(labels) > 2 && labels[0] == "sqs" && regionPattern.MatchString(labels[1]):
		return labels[1]
	case len(labels) > 2 && labels[1] == "queue" && regionPattern.MatchString(labels[0]):
		return labels[0]
	}
	return ""
}

// regionFromARN reads the region of an ARN, arn:<partition>:<service>:<region>:....
func regionFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) == 6 && parts[0] == "arn" && regionPattern.MatchString(parts[3]) {
		return parts[3]
	}
	return ""
}

// seconds rounds a duration up to whole seconds, the unit SQS counts in.
func seconds(d time.Duration) int32 {
	if d <= 0 {
		return 0
	}
	return int32(math.Ceil(d.Seconds()))
}

// queueSourceOptions is an sqs, sns or eventbridge source's configuration,
// defaults applied.
type queueSourceOptions struct {
	queueURL   string
	queueName  string
	wait       int32 // long-poll seconds
	prefetch   int32
	visibility int32 // seconds; 0: the queue's own
	maxBytes   int64
	ack        string
	reconnect  reconnectOptions
}

// reconnectOptions is the backoff of a source's failing receives or polls.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // 0 = forever
}

func sourceConfig(spec plugin.Spec) (*config.Source, func(string, ...any) error, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return nil, nil, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("source %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if len(src.Options) > 0 {
		return nil, nil, fail("options: applies to plugin types only; %s takes its settings as top-level keys", spec.Kind)
	}
	return src, fail, nil
}

// parseQueueSource turns the spec of an sqs, sns or eventbridge source into
// its options.
func parseQueueSource(spec plugin.Spec) (*config.Source, queueSourceOptions, error) {
	src, fail, err := sourceConfig(spec)
	if err != nil {
		return nil, queueSourceOptions{}, err
	}
	if src.Mode != "" && src.Mode != modeSubscribe {
		return nil, queueSourceOptions{}, fail("%s sources subscribe (read an SQS queue), not %q", spec.Kind, src.Mode)
	}
	if src.QueueURL == "" {
		return nil, queueSourceOptions{}, fail("queue_url is required: the SQS queue read")
	}
	u, err := url.Parse(src.QueueURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || strings.Trim(u.Path, "/") == "" {
		return nil, queueSourceOptions{}, fail("queue_url %q is not an SQS queue URL (https://sqs.<region>.amazonaws.com/<account>/<queue>)", redactURL(src.QueueURL))
	}
	path := strings.Trim(u.Path, "/")
	o := queueSourceOptions{
		queueURL:  src.QueueURL,
		queueName: path[strings.LastIndex(path, "/")+1:],
		wait:      seconds(defaultWaitTime),
		prefetch:  defaultPrefetch,
		maxBytes:  int64(src.MaxBodyBytes),
		ack:       src.Ack,
	}
	switch w := src.WaitTime.Std(); {
	case w < 0 || w > maxWaitTime:
		return nil, queueSourceOptions{}, fail("wait_time must be at most 20s (the SQS long-poll limit)")
	case w > 0:
		o.wait = seconds(w)
	}
	switch {
	case src.Prefetch < 0 || src.Prefetch > 10:
		return nil, queueSourceOptions{}, fail("prefetch must be 1 to 10 for %s (SQS receives at most 10 messages at once)", spec.Kind)
	case src.Prefetch > 0:
		o.prefetch = int32(src.Prefetch)
	}
	switch v := src.VisibilityTimeout.Std(); {
	case v < 0 || v > maxVisibility:
		return nil, queueSourceOptions{}, fail("visibility_timeout must be at most 12h (the SQS limit)")
	case v > 0:
		o.visibility = seconds(v)
	}
	switch {
	case o.maxBytes < 0:
		return nil, queueSourceOptions{}, fail("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = config.DefaultMaxBodyBytes
	}
	if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
		return nil, queueSourceOptions{}, fail("%v", err)
	}
	return src, o, nil
}

// parseReconnect applies the reconnect defaults: backoff 500ms, max_backoff
// 30s, jitter full, max_attempts 0 (forever).
func parseReconnect(r *config.Reconnect) (reconnectOptions, error) {
	o := reconnectOptions{backoff: defaultBackoff, maxBackoff: defaultMaxBackoff, jitter: defaultJitter}
	if r == nil {
		return o, nil
	}
	if r.Backoff > 0 {
		o.backoff = r.Backoff.Std()
	}
	if r.MaxBackoff > 0 {
		o.maxBackoff = r.MaxBackoff.Std()
	}
	o.maxBackoff = max(o.maxBackoff, o.backoff)
	switch r.Jitter {
	case "":
	case "full", "equal", "none":
		o.jitter = r.Jitter
	default:
		return reconnectOptions{}, fmt.Errorf("reconnect.jitter must be full, equal or none, not %q", r.Jitter)
	}
	if r.MaxAttempts < 0 {
		return reconnectOptions{}, fmt.Errorf("reconnect.max_attempts must not be negative (0 retries forever)")
	}
	o.maxAttempts = r.MaxAttempts
	return o, nil
}

// s3SourceOptions is an s3 source's configuration, defaults applied.
type s3SourceOptions struct {
	bucket     string
	prefix     string
	after      string
	moveTo     string
	stateStore string
	interval   time.Duration
	maxBytes   int64
	lines      bool
	split      *transform.JQ
	retry      config.Retry
	timeout    time.Duration // each list, state, delete and move call (0: the default)
}

// parseS3Source turns the spec of an s3 source into its options.
func parseS3Source(spec plugin.Spec) (*config.Source, s3SourceOptions, error) {
	src, fail, err := sourceConfig(spec)
	if err != nil {
		return nil, s3SourceOptions{}, err
	}
	if src.Mode != "" && src.Mode != modePoll {
		return nil, s3SourceOptions{}, fail("s3 sources poll a bucket, not %q", src.Mode)
	}
	if src.Bucket == "" {
		return nil, s3SourceOptions{}, fail("bucket is required: the bucket polled")
	}
	if strings.Contains(src.Bucket, "/") {
		return nil, s3SourceOptions{}, fail("bucket %q is a bucket name, without a key or prefix (use prefix)", src.Bucket)
	}
	o := s3SourceOptions{
		bucket:     src.Bucket,
		prefix:     src.Prefix,
		after:      src.After,
		moveTo:     src.MoveTo,
		stateStore: src.StateStore,
		interval:   src.Interval.Std(),
		maxBytes:   int64(src.MaxBodyBytes),
		retry:      config.ResolveRetry(src.Retry, &config.Retry{Attempts: 3, Backoff: config.Duration(time.Second)}),
		timeout:    src.Timeout.Std(),
	}
	if o.timeout < 0 {
		return nil, s3SourceOptions{}, fail("timeout must not be negative")
	}
	switch o.after {
	case "":
		o.after = afterDelete
	case afterDelete, afterKeep:
	case afterMove:
		if o.moveTo == "" {
			return nil, s3SourceOptions{}, fail("move_to is required with after: move")
		}
		// Moved objects are told apart from new ones by move_to, which a
		// listing skips: it cannot hold the whole prefix.
		if strings.HasPrefix(o.prefix, o.moveTo) {
			return nil, s3SourceOptions{}, fail("prefix %q is under move_to %q: every object polled would look moved already", o.prefix, o.moveTo)
		}
	default:
		return nil, s3SourceOptions{}, fail("after must be keep, delete or move, not %q", o.after)
	}
	if o.moveTo != "" && o.after != afterMove {
		return nil, s3SourceOptions{}, fail("move_to applies with after: move only")
	}
	if o.interval < 0 {
		return nil, s3SourceOptions{}, fail("interval must not be negative")
	}
	if o.interval == 0 {
		o.interval = config.DefaultPollInterval
	}
	switch {
	case o.maxBytes < 0:
		return nil, s3SourceOptions{}, fail("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = config.DefaultMaxBodyBytes
	}
	switch split := strings.TrimSpace(src.Split); split {
	case "":
	case splitLines:
		o.lines = true
	default:
		// Anything else is a jq split, as for http poll: one message per
		// output of the program over the object's JSON.
		if o.split, err = transform.CompileJQ(split); err != nil {
			return nil, s3SourceOptions{}, fail("split must be lines or a jq expression: %v", err)
		}
	}
	return src, o, nil
}

// sinkConfig is the endpoint of a destination, with its shared checks.
func sinkConfig(spec plugin.Spec) (*config.Endpoint, func(string, ...any) error, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return nil, nil, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if len(ep.Options) > 0 {
		return nil, nil, fail("options: applies to plugin types only; %s takes its settings as top-level keys", spec.Kind)
	}
	return ep, fail, nil
}

// static rejects a per-message template where a value is fixed when the
// destination is built.
func static(field, v string) error {
	if strings.Contains(v, "{{") {
		return fmt.Errorf("%s cannot be a per-message template", field)
	}
	return nil
}

// headerTemplates compiles the headers block: per-message templates, in
// name order.
type headerTemplate struct {
	name string
	tmpl *transform.Template
}

func compileHeaders(headers map[string]string) ([]headerTemplate, error) {
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]headerTemplate, 0, len(names))
	for _, k := range names {
		t, err := transform.CompileTemplate("headers."+k, headers[k])
		if err != nil {
			return nil, err
		}
		out = append(out, headerTemplate{name: k, tmpl: t})
	}
	return out, nil
}

// optionalTemplate compiles a template that may be empty (nil then).
func optionalTemplate(name, text string) (*transform.Template, error) {
	if text == "" {
		return nil, nil
	}
	return transform.CompileTemplate(name, text)
}
