package grpc

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/plugin"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		code    codes.Code
		reached bool
		class   plugin.Class
		tags    []string
	}{
		{codes.Unavailable, false, plugin.Transient, []string{"grpc:UNAVAILABLE", "5xx", "connect"}},
		{codes.Unavailable, true, plugin.Transient, []string{"grpc:UNAVAILABLE", "5xx"}},
		{codes.Aborted, false, plugin.Transient, []string{"grpc:ABORTED", "5xx"}},
		{codes.Internal, true, plugin.Transient, []string{"grpc:INTERNAL", "5xx"}},
		{codes.Unknown, true, plugin.Transient, []string{"grpc:UNKNOWN", "5xx"}},
		{codes.ResourceExhausted, true, plugin.RateLimited, []string{"grpc:RESOURCE_EXHAUSTED", "429"}},
		{codes.DeadlineExceeded, false, plugin.Timeout, []string{"grpc:DEADLINE_EXCEEDED", "timeout"}},
		{codes.Canceled, true, plugin.Permanent, []string{"grpc:CANCELLED", "4xx"}},
		{codes.InvalidArgument, true, plugin.Permanent, []string{"grpc:INVALID_ARGUMENT", "4xx"}},
		{codes.NotFound, true, plugin.Permanent, []string{"grpc:NOT_FOUND", "4xx"}},
		{codes.AlreadyExists, true, plugin.Permanent, []string{"grpc:ALREADY_EXISTS", "4xx"}},
		{codes.PermissionDenied, true, plugin.Permanent, []string{"grpc:PERMISSION_DENIED", "4xx"}},
		{codes.Unauthenticated, true, plugin.Permanent, []string{"grpc:UNAUTHENTICATED", "4xx"}},
		{codes.FailedPrecondition, true, plugin.Permanent, []string{"grpc:FAILED_PRECONDITION", "4xx"}},
		{codes.Unimplemented, true, plugin.Permanent, []string{"grpc:UNIMPLEMENTED", "4xx"}},
		{codes.OutOfRange, true, plugin.Permanent, []string{"grpc:OUT_OF_RANGE", "4xx"}},
		{codes.DataLoss, true, plugin.Permanent, []string{"grpc:DATA_LOSS", "4xx"}},
		{codes.Code(42), true, plugin.Transient, []string{"grpc:CODE_42"}},
	}
	for _, c := range cases {
		err := classify("/a.B/C", status.Error(c.code, "why"), c.reached)
		var pe *plugin.Error
		if !assert.True(t, errors.As(err, &pe), c.code.String()) {
			continue
		}
		assert.Equal(t, c.class, pe.Class, c.code.String())
		assert.Equal(t, c.tags, pe.Tags, c.code.String())
		assert.Equal(t, "/a.B/C: "+codeName(c.code)+": why", err.Error())
	}
	assert.Equal(t, "/a.B/C: UNAVAILABLE: unavailable", classify("/a.B/C", status.Error(codes.Unavailable, ""), true).Error(),
		"a status without a message")
	assert.Equal(t, []string{"grpc:UNKNOWN", "5xx"}, plugin.Tags(classify("x", errors.New("plain"), true)), "not a status: unknown")
	assert.Equal(t, "OK", codeName(codes.OK))
}

// The default retry policy (retry_on: connect, timeout, 5xx, 429; never_on:
// 4xx) retries the transient codes and stops at the permanent ones.
func TestClassifyMatchesTheDefaultPolicy(t *testing.T) {
	retryOn := map[string]bool{"connect": true, "timeout": true, "5xx": true, "429": true}
	for code := codes.Canceled; code <= codes.Unauthenticated; code++ {
		err := classify("x", status.Error(code, "x"), true)
		class, _ := plugin.Classify(err)
		retried := false
		for _, tag := range plugin.Tags(err) {
			retried = retried || retryOn[tag]
		}
		assert.Equal(t, class != plugin.Permanent, retried, code.String())
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, 1500*time.Millisecond, retryAfter(now, metadata.Pairs("grpc-retry-pushback-ms", "1500")))
	assert.Equal(t, 3*time.Second, retryAfter(now, metadata.Pairs("retry-after", "3")))
	assert.Equal(t, 10*time.Second, retryAfter(now, metadata.Pairs("retry-after", now.Add(10*time.Second).Format("Mon, 02 Jan 2006 15:04:05 GMT"))))
	assert.Equal(t, 2*time.Second, retryAfter(now, nil, metadata.Pairs("retry-after", "2")), "the header after the trailer")
	assert.Equal(t, time.Duration(0), retryAfter(now, metadata.Pairs("grpc-retry-pushback-ms", "-1", "retry-after", "soon")))
	assert.Equal(t, time.Duration(0), retryAfter(now))
}
