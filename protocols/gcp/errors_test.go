package gcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/xavidop/hikyaku/plugin"
)

func TestPubSubErrorClasses(t *testing.T) {
	cases := []struct {
		code  codes.Code
		class plugin.Class
		tags  []string
	}{
		{codes.Unavailable, plugin.Transient, []string{"pubsub:UNAVAILABLE", "grpc:UNAVAILABLE", "5xx"}},
		{codes.Internal, plugin.Transient, []string{"pubsub:INTERNAL", "grpc:INTERNAL", "5xx"}},
		{codes.Aborted, plugin.Transient, []string{"pubsub:ABORTED", "grpc:ABORTED", "5xx"}},
		{codes.Unknown, plugin.Transient, []string{"pubsub:UNKNOWN", "grpc:UNKNOWN", "5xx"}},
		{codes.ResourceExhausted, plugin.RateLimited, []string{"pubsub:RESOURCE_EXHAUSTED", "grpc:RESOURCE_EXHAUSTED", "429"}},
		{codes.DeadlineExceeded, plugin.Timeout, []string{"pubsub:DEADLINE_EXCEEDED", "grpc:DEADLINE_EXCEEDED", "timeout"}},
		{codes.NotFound, plugin.Permanent, []string{"pubsub:NOT_FOUND", "grpc:NOT_FOUND", "4xx"}},
		{codes.PermissionDenied, plugin.Permanent, []string{"pubsub:PERMISSION_DENIED", "grpc:PERMISSION_DENIED", "4xx"}},
		{codes.InvalidArgument, plugin.Permanent, []string{"pubsub:INVALID_ARGUMENT", "grpc:INVALID_ARGUMENT", "4xx"}},
		{codes.FailedPrecondition, plugin.Permanent, []string{"pubsub:FAILED_PRECONDITION", "grpc:FAILED_PRECONDITION", "4xx"}},
		{codes.Unauthenticated, plugin.Permanent, []string{"pubsub:UNAUTHENTICATED", "grpc:UNAUTHENTICATED", "4xx"}},
		{codes.Canceled, plugin.Transient, []string{"pubsub:CANCELLED", "grpc:CANCELLED"}},
	}
	for _, c := range cases {
		t.Run(c.code.String(), func(t *testing.T) {
			err := pubsubError("projects/p/topics/t", status.Error(c.code, "boom"))
			class, tags := classOf(t, err)
			assert.Equal(t, c.class, class)
			assert.ElementsMatch(t, c.tags, tags)
			assert.Contains(t, err.Error(), "projects/p/topics/t")
		})
	}
}

func TestPubSubErrorDetails(t *testing.T) {
	st, err := status.New(codes.ResourceExhausted, "slow down").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(3 * time.Second)})
	require.NoError(t, err)
	class, ra := plugin.Classify(pubsubError("t", st.Err()))
	assert.Equal(t, plugin.RateLimited, class)
	assert.Equal(t, 3*time.Second, ra, "the server's RetryInfo is the retry-after hint")

	dial := status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:1: connect: connection refused"`)
	_, tags := classOf(t, pubsubError("t", dial))
	assert.Contains(t, tags, plugin.TagConnect)

	wrapped := fmt.Errorf("publish: %w", status.Error(codes.NotFound, "no topic"))
	class, _ = classOf(t, pubsubError("t", wrapped))
	assert.Equal(t, plugin.Permanent, class, "a wrapped status is still read")
}

func TestPubSubErrorsWithoutStatus(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		class plugin.Class
		tags  []string
	}{
		{"deadline", context.DeadlineExceeded, plugin.Timeout, []string{"timeout"}},
		{"canceled", context.Canceled, plugin.Transient, nil},
		{"stopped", pubsub.ErrPublisherStopped, plugin.Transient, nil},
		{"paused", pubsub.ErrPublishingPaused{OrderingKey: "k"}, plugin.Transient, nil},
		{"too large", pubsub.ErrOversizedMessage, plugin.Permanent, []string{tagTooLarge, "4xx"}},
		{"refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, plugin.Transient, []string{"connect"}},
		{"other", errors.New("odd"), plugin.Transient, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			class, tags := classOf(t, pubsubError("t", c.err))
			assert.Equal(t, c.class, class)
			assert.ElementsMatch(t, c.tags, tags)
		})
	}
	assert.NoError(t, pubsubError("t", nil))
	already := plugin.Permanentf("x")
	assert.Same(t, already, pubsubError("t", already))
}

func TestGCSErrorClasses(t *testing.T) {
	retry := http.Header{"Retry-After": {"7"}}
	cases := []struct {
		name  string
		err   error
		class plugin.Class
		tags  []string
		ra    time.Duration
	}{
		{"429", &googleapi.Error{Code: 429, Header: retry}, plugin.RateLimited, []string{"429", "4xx", "gcs:429"}, 7 * time.Second},
		{"503", &googleapi.Error{Code: 503}, plugin.Transient, []string{"503", "5xx", "gcs:503"}, 0},
		{"500", &googleapi.Error{Code: 500}, plugin.Transient, []string{"500", "5xx", "gcs:500"}, 0},
		{"504", &googleapi.Error{Code: 504}, plugin.Timeout, []string{"504", "5xx", "gcs:504", "timeout"}, 0},
		{"400", &googleapi.Error{Code: 400}, plugin.Permanent, []string{"400", "4xx", "gcs:400"}, 0},
		{"403", &googleapi.Error{Code: 403}, plugin.Permanent, []string{"403", "4xx", "gcs:403"}, 0},
		{"no bucket", storage.ErrBucketNotExist, plugin.Permanent, []string{"404", "4xx", "gcs:404"}, 0},
		{"deadline", context.DeadlineExceeded, plugin.Timeout, []string{"timeout"}, 0},
		{"canceled", context.Canceled, plugin.Transient, nil, 0},
		{"reset", fmt.Errorf("post: %w", syscall.ECONNRESET), plugin.Transient, []string{"connect"}, 0},
		{"dns", &net.DNSError{Err: "no such host", Name: "x"}, plugin.Transient, []string{"connect"}, 0},
		{"other", errors.New("odd"), plugin.Transient, nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := gcsError("write gs://b/k", c.err)
			class, tags := classOf(t, err)
			assert.Equal(t, c.class, class)
			assert.ElementsMatch(t, c.tags, tags)
			_, ra := plugin.Classify(err)
			assert.Equal(t, c.ra, ra)
			assert.Contains(t, err.Error(), "write gs://b/k")
		})
	}
	assert.NoError(t, gcsError("x", nil))
}

func TestGCSErrorPredicates(t *testing.T) {
	assert.True(t, isPrecondition(fmt.Errorf("close: %w", &googleapi.Error{Code: 412})))
	assert.False(t, isPrecondition(&googleapi.Error{Code: 409}))
	assert.True(t, isNotFound(storage.ErrObjectNotExist))
	assert.True(t, isNotFound(&googleapi.Error{Code: 404}))
	assert.False(t, isNotFound(&googleapi.Error{Code: 403}))
}
