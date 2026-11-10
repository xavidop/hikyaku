package grpc

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/plugin"
)

// codeNames are the canonical names of the status codes, as tags use them.
var codeNames = map[codes.Code]string{
	codes.OK:                 "OK",
	codes.Canceled:           "CANCELLED",
	codes.Unknown:            "UNKNOWN",
	codes.InvalidArgument:    "INVALID_ARGUMENT",
	codes.DeadlineExceeded:   "DEADLINE_EXCEEDED",
	codes.NotFound:           "NOT_FOUND",
	codes.AlreadyExists:      "ALREADY_EXISTS",
	codes.PermissionDenied:   "PERMISSION_DENIED",
	codes.ResourceExhausted:  "RESOURCE_EXHAUSTED",
	codes.FailedPrecondition: "FAILED_PRECONDITION",
	codes.Aborted:            "ABORTED",
	codes.OutOfRange:         "OUT_OF_RANGE",
	codes.Unimplemented:      "UNIMPLEMENTED",
	codes.Internal:           "INTERNAL",
	codes.Unavailable:        "UNAVAILABLE",
	codes.DataLoss:           "DATA_LOSS",
	codes.Unauthenticated:    "UNAUTHENTICATED",
}

// codeName is a status code's canonical name, such as UNAVAILABLE.
func codeName(c codes.Code) string {
	if n, ok := codeNames[c]; ok {
		return n
	}
	return "CODE_" + strconv.FormatUint(uint64(c), 10)
}

// codeTag is the error tag of a status code, such as grpc:UNAVAILABLE.
func codeTag(c codes.Code) string { return "grpc:" + codeName(c) }

// classify turns a failed call into a *plugin.Error:
//
//	UNAVAILABLE, ABORTED, INTERNAL, UNKNOWN  transient, tagged 5xx
//	RESOURCE_EXHAUSTED                       rate limited, tagged 429
//	DEADLINE_EXCEEDED                        timeout, tagged timeout
//	every other code                         permanent, tagged 4xx
//
// plus grpc:<CODE>, and connect when the call never reached the server.
// The retry hint comes from the grpc-retry-pushback-ms or retry-after
// metadata of the answer (trailers first). what names the call in the
// error message.
func classify(what string, err error, reached bool, mds ...metadata.MD) error {
	st := status.Convert(err)
	code := st.Code()
	tags := []string{codeTag(code)}
	var class plugin.Class
	switch code {
	case codes.Unavailable, codes.Aborted, codes.Internal, codes.Unknown:
		class = plugin.Transient
		tags = append(tags, plugin.Tag5xx)
	case codes.ResourceExhausted:
		class = plugin.RateLimited
		tags = append(tags, plugin.Tag429)
	case codes.DeadlineExceeded:
		class = plugin.Timeout
		tags = append(tags, plugin.TagTimeout)
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied,
		codes.Unauthenticated, codes.FailedPrecondition, codes.Unimplemented, codes.OutOfRange,
		codes.DataLoss, codes.Canceled:
		class = plugin.Permanent
		tags = append(tags, plugin.Tag4xx)
	default:
		class = plugin.Transient
	}
	if code == codes.Unavailable && !reached {
		tags = append(tags, plugin.TagConnect)
	}
	msg := st.Message()
	if msg == "" {
		msg = strings.ToLower(strings.ReplaceAll(codeName(code), "_", " "))
	}
	return &plugin.Error{
		Class:      class,
		RetryAfter: retryAfter(time.Now(), mds...),
		Tags:       tags,
		Err:        fmt.Errorf("%s: %s: %s", what, codeName(code), msg),
	}
}

// retryAfter reads a server's retry hint: grpc-retry-pushback-ms
// (milliseconds, gRFC A6) or retry-after (seconds or an HTTP date).
func retryAfter(now time.Time, mds ...metadata.MD) time.Duration {
	for _, md := range mds {
		if v := md.Get("grpc-retry-pushback-ms"); len(v) > 0 {
			if ms, err := strconv.ParseInt(strings.TrimSpace(v[0]), 10, 64); err == nil && ms > 0 {
				return time.Duration(ms) * time.Millisecond
			}
		}
		if v := md.Get("retry-after"); len(v) > 0 {
			if d := plugin.ParseRetryAfter(v[0], now); d > 0 {
				return d
			}
		}
	}
	return 0
}
