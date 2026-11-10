package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

// hermeticAWS keeps the SDK's default chain away from the machine's AWS
// settings: no environment credentials or region, no shared files, no IMDS.
func hermeticAWS(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	for k, v := range map[string]string{
		"AWS_CONFIG_FILE":             dir + "/config",
		"AWS_SHARED_CREDENTIALS_FILE": dir + "/credentials",
		"AWS_EC2_METADATA_DISABLED":   "true",
		"AWS_REGION":                  "",
		"AWS_DEFAULT_REGION":          "",
		"AWS_PROFILE":                 "",
		"AWS_DEFAULT_PROFILE":         "",
		"AWS_ACCESS_KEY_ID":           "",
		"AWS_SECRET_ACCESS_KEY":       "",
		"AWS_SESSION_TOKEN":           "",
		"AWS_ENDPOINT_URL":            "",
		"AWS_ROLE_ARN":                "",
		"AWS_WEB_IDENTITY_TOKEN_FILE": "",
	} {
		t.Setenv(k, v)
	}
}

// testAWS is an aws block with static test credentials.
func testAWS(endpoint string) *config.AWS {
	return &config.AWS{Region: "us-east-1", Endpoint: endpoint, AccessKeyID: "test", SecretAccessKey: "test"}
}

// logBuffer is a logger whose output a test can read.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *logBuffer) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func sourceSpec(kind string, src *config.Source) plugin.Spec {
	src.Type = kind
	if src.Name == "" {
		src.Name = "in"
	}
	return plugin.Spec{Name: src.Name, Kind: kind, Config: src, Env: plugin.Env{Relay: "relay"}}
}

func sinkSpec(kind string, ep *config.Endpoint) plugin.Spec {
	ep.Type = kind
	return plugin.Spec{Name: "out", Kind: kind, Config: ep, Env: plugin.Env{Relay: "relay"}}
}

// withStore lends a spec a memory store.
func withStore(spec plugin.Spec, st store.Store) plugin.Spec {
	spec.Env.Store = func(string) (store.Store, error) { return st, nil }
	return spec
}

// apiError is an error the way the SDK returns a service's answer: an
// operation error around an HTTP response error around the API error.
func apiError(code string, status int, fault smithy.ErrorFault) error {
	return &smithy.OperationError{ServiceID: "SQS", OperationName: "SendMessage", Err: &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      &smithy.GenericAPIError{Code: code, Message: code + " happened", Fault: fault},
		},
		RequestID: "req-1",
	}}
}

// recv reads the next envelope or fails the test.
func recv(t *testing.T, out <-chan *envelope.Envelope, within time.Duration) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(within):
		require.FailNow(t, "no message within "+within.String())
		return nil
	}
}

// noMessage fails the test when an envelope arrives within d.
func noMessage(t *testing.T, out <-chan *envelope.Envelope, d time.Duration) {
	t.Helper()
	select {
	case env := <-out:
		require.FailNow(t, "unexpected message", "id %s body %q", env.ID, env.Body)
	case <-time.After(d):
	}
}

// fakeSQS is an in-memory SQS queue: messages, receipt handles, receive
// counts and visibility timeouts, enough to see what a source and a sink do
// with it.
type fakeSQS struct {
	mu         sync.Mutex
	seq        int
	msgs       []*fakeMsg
	visibility time.Duration // the queue's

	// failures to inject
	receiveErrs []error // one per call, in order
	attrsErr    error
	deleteErr   error
	deleteFail  string // DeleteMessageBatch: every entry fails with this code
	visErr      error
	sendErr     error
	failBodies  map[string]string // SendMessageBatch: body → error code

	// what was called
	calls        map[string]int
	deleteCalls  [][]string // receipt handles per DeleteMessageBatch
	visCalls     []visCall
	sends        []*sqs.SendMessageInput
	batchEntries [][]types.SendMessageBatchRequestEntry
}

type fakeMsg struct {
	id        string
	body      string
	attrs     map[string]types.MessageAttributeValue
	group     string
	sent      time.Time
	receives  int
	receipt   string
	visibleAt time.Time
	deleted   bool
}

type visCall struct {
	receipt string
	secs    int32
	batch   bool
}

func newFakeSQS() *fakeSQS {
	return &fakeSQS{visibility: 30 * time.Second, calls: map[string]int{}}
}

// put enqueues a message the way a producer would.
func (f *fakeSQS) put(body string, attrs map[string]string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	m := &fakeMsg{id: fmt.Sprintf("msg-%d", f.seq), body: body, attrs: map[string]types.MessageAttributeValue{}, sent: time.Now()}
	for k, v := range attrs {
		m.attrs[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	f.msgs = append(f.msgs, m)
	return m.id
}

func (f *fakeSQS) putRaw(m *fakeMsg) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	if m.id == "" {
		m.id = fmt.Sprintf("msg-%d", f.seq)
	}
	if m.sent.IsZero() {
		m.sent = time.Now()
	}
	f.msgs = append(f.msgs, m)
}

func (f *fakeSQS) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op]
}

// remaining is how many messages are not deleted.
func (f *fakeSQS) remaining() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.msgs {
		if !m.deleted {
			n++
		}
	}
	return n
}

func (f *fakeSQS) visibilityCalls() []visCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.visCalls)
}

func (f *fakeSQS) byReceipt(r string) *fakeMsg {
	for _, m := range f.msgs {
		if m.receipt == r && !m.deleted {
			return m
		}
	}
	return nil
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	f.calls["ReceiveMessage"]++
	if len(f.receiveErrs) > 0 {
		err := f.receiveErrs[0]
		f.receiveErrs = f.receiveErrs[1:]
		f.mu.Unlock()
		if err != nil {
			return nil, err
		}
	} else {
		f.mu.Unlock()
	}
	// A short long poll: tests do not wait 20s for an empty queue.
	deadline := time.Now().Add(50 * time.Millisecond)
	for {
		if err := ctx.Err(); err != nil {
			return nil, &smithyhttp.RequestSendError{Err: err}
		}
		if got := f.take(in); len(got) > 0 || time.Now().After(deadline) {
			return &sqs.ReceiveMessageOutput{Messages: got}, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *fakeSQS) take(in *sqs.ReceiveMessageInput) []types.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	vt := f.visibility
	if in.VisibilityTimeout > 0 {
		vt = time.Duration(in.VisibilityTimeout) * time.Second
	}
	var out []types.Message
	for _, m := range f.msgs {
		if len(out) == int(in.MaxNumberOfMessages) {
			break
		}
		if m.deleted || now.Before(m.visibleAt) {
			continue
		}
		m.receives++
		f.seq++
		m.receipt = fmt.Sprintf("rh-%s-%d", m.id, f.seq)
		m.visibleAt = now.Add(vt)
		sys := map[string]string{
			"ApproximateReceiveCount": strconv.Itoa(m.receives),
			"SentTimestamp":           strconv.FormatInt(m.sent.UnixMilli(), 10),
		}
		if m.group != "" {
			sys["MessageGroupId"] = m.group
		}
		out = append(out, types.Message{
			MessageId: aws.String(m.id), ReceiptHandle: aws.String(m.receipt), Body: aws.String(m.body),
			MessageAttributes: m.attrs, Attributes: sys,
		})
	}
	return out
}

func (f *fakeSQS) DeleteMessageBatch(_ context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["DeleteMessageBatch"]++
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	var receipts []string
	out := &sqs.DeleteMessageBatchOutput{}
	for _, e := range in.Entries {
		receipts = append(receipts, aws.ToString(e.ReceiptHandle))
		if f.deleteFail != "" {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String(f.deleteFail), Message: aws.String("no")})
			continue
		}
		if m := f.byReceipt(aws.ToString(e.ReceiptHandle)); m != nil {
			m.deleted = true
			out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: e.Id})
			continue
		}
		out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("ReceiptHandleIsInvalid"), SenderFault: true})
	}
	f.deleteCalls = append(f.deleteCalls, receipts)
	return out, nil
}

func (f *fakeSQS) setVisibility(receipt string, secs int32) bool {
	m := f.byReceipt(receipt)
	if m == nil {
		return false
	}
	m.visibleAt = time.Now().Add(time.Duration(secs) * time.Second)
	return true
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["ChangeMessageVisibility"]++
	f.visCalls = append(f.visCalls, visCall{receipt: aws.ToString(in.ReceiptHandle), secs: in.VisibilityTimeout})
	if f.visErr != nil {
		return nil, f.visErr
	}
	if !f.setVisibility(aws.ToString(in.ReceiptHandle), in.VisibilityTimeout) {
		return nil, apiError("ReceiptHandleIsInvalid", 400, smithy.FaultClient)
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibilityBatch(_ context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["ChangeMessageVisibilityBatch"]++
	if f.visErr != nil {
		return nil, f.visErr
	}
	out := &sqs.ChangeMessageVisibilityBatchOutput{}
	for _, e := range in.Entries {
		f.visCalls = append(f.visCalls, visCall{receipt: aws.ToString(e.ReceiptHandle), secs: e.VisibilityTimeout, batch: true})
		if f.setVisibility(aws.ToString(e.ReceiptHandle), e.VisibilityTimeout) {
			out.Successful = append(out.Successful, types.ChangeMessageVisibilityBatchResultEntry{Id: e.Id})
		} else {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("ReceiptHandleIsInvalid"), SenderFault: true})
		}
	}
	return out, nil
}

func (f *fakeSQS) GetQueueAttributes(_ context.Context, in *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["GetQueueAttributes"]++
	if f.attrsErr != nil {
		return nil, f.attrsErr
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{
		"VisibilityTimeout": strconv.Itoa(int(f.visibility / time.Second)),
		"QueueArn":          "arn:aws:sqs:us-east-1:000000000000:q",
	}}, nil
}

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["SendMessage"]++
	f.sends = append(f.sends, in)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	f.seq++
	id := fmt.Sprintf("sent-%d", f.seq)
	out := &sqs.SendMessageOutput{MessageId: aws.String(id)}
	if in.MessageDeduplicationId != nil {
		out.SequenceNumber = aws.String(strconv.Itoa(f.seq))
	}
	return out, nil
}

func (f *fakeSQS) SendMessageBatch(_ context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["SendMessageBatch"]++
	f.batchEntries = append(f.batchEntries, in.Entries)
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	out := &sqs.SendMessageBatchOutput{}
	for _, e := range in.Entries {
		if code, ok := f.failBodies[aws.ToString(e.MessageBody)]; ok {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{
				Id: e.Id, Code: aws.String(code), Message: aws.String("refused"), SenderFault: !transientCodes[code],
			})
			continue
		}
		f.seq++
		r := types.SendMessageBatchResultEntry{Id: e.Id, MessageId: aws.String(fmt.Sprintf("sent-%d", f.seq))}
		if e.MessageDeduplicationId != nil {
			r.SequenceNumber = aws.String(strconv.Itoa(f.seq))
		}
		out.Successful = append(out.Successful, r)
	}
	return out, nil
}

// fakeSNS records what an sns destination publishes.
type fakeSNS struct {
	mu         sync.Mutex
	seq        int
	publishes  []*sns.PublishInput
	batches    []*sns.PublishBatchInput
	err        error
	failBodies map[string]string
	attrsErr   error
}

func (f *fakeSNS) Publish(_ context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishes = append(f.publishes, in)
	if f.err != nil {
		return nil, f.err
	}
	f.seq++
	out := &sns.PublishOutput{MessageId: aws.String(fmt.Sprintf("sns-%d", f.seq))}
	if in.MessageDeduplicationId != nil {
		out.SequenceNumber = aws.String(strconv.Itoa(f.seq))
	}
	return out, nil
}

func (f *fakeSNS) PublishBatch(_ context.Context, in *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, in)
	if f.err != nil {
		return nil, f.err
	}
	out := &sns.PublishBatchOutput{}
	for _, e := range in.PublishBatchRequestEntries {
		if code, ok := f.failBodies[aws.ToString(e.Message)]; ok {
			out.Failed = append(out.Failed, snstypes.BatchResultErrorEntry{Id: e.Id, Code: aws.String(code), SenderFault: !transientCodes[code]})
			continue
		}
		f.seq++
		r := snstypes.PublishBatchResultEntry{Id: e.Id, MessageId: aws.String(fmt.Sprintf("sns-%d", f.seq))}
		if e.MessageDeduplicationId != nil {
			r.SequenceNumber = aws.String(strconv.Itoa(f.seq))
		}
		out.Successful = append(out.Successful, r)
	}
	return out, nil
}

func (f *fakeSNS) GetTopicAttributes(_ context.Context, _ *sns.GetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	if f.attrsErr != nil {
		return nil, f.attrsErr
	}
	return &sns.GetTopicAttributesOutput{}, nil
}

// fakeEventBridge records what an eventbridge destination puts.
type fakeEventBridge struct {
	mu          sync.Mutex
	seq         int
	puts        []*eventbridge.PutEventsInput
	err         error
	failDetails map[string]string // detail → error code
	describeErr error
	described   []string
}

func (f *fakeEventBridge) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, in)
	if f.err != nil {
		return nil, f.err
	}
	out := &eventbridge.PutEventsOutput{}
	for _, e := range in.Entries {
		if code, ok := f.failDetails[aws.ToString(e.Detail)]; ok {
			out.FailedEntryCount++
			out.Entries = append(out.Entries, ebtypes.PutEventsResultEntry{ErrorCode: aws.String(code), ErrorMessage: aws.String("refused")})
			continue
		}
		f.seq++
		out.Entries = append(out.Entries, ebtypes.PutEventsResultEntry{EventId: aws.String(fmt.Sprintf("ev-%d", f.seq))})
	}
	return out, nil
}

func (f *fakeEventBridge) DescribeEventBus(_ context.Context, in *eventbridge.DescribeEventBusInput, _ ...func(*eventbridge.Options)) (*eventbridge.DescribeEventBusOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.described = append(f.described, aws.ToString(in.Name))
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return &eventbridge.DescribeEventBusOutput{Name: in.Name}, nil
}

// errorTags is the class and tags of an error, the way the engine reads it.
func errorTags(err error) (plugin.Class, []string) {
	c, _ := plugin.Classify(err)
	return c, plugin.Tags(err)
}

// isPluginError reports whether err is a *plugin.Error.
func isPluginError(err error) bool {
	var pe *plugin.Error
	return errors.As(err, &pe)
}

// attrNames lists message attribute names in order of a builder's output.
func attrNames(attrs []attribute) []string {
	out := make([]string, len(attrs))
	for i, a := range attrs {
		out[i] = a.name
	}
	return out
}
