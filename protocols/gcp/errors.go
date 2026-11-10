package gcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/plugin"
)

// errClosed is what a destination answers once it is closed.
var errClosed = errors.New("destination is closed")

// Tags of failures that have no gRPC or HTTP status.
const (
	tagTooLarge = "pubsub:MESSAGE_TOO_LARGE"
	tagTemplate = "template"
)

// codeNames are the canonical names of the gRPC status codes, as in
// pubsub:UNAVAILABLE.
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

func codeName(c codes.Code) string {
	if n, ok := codeNames[c]; ok {
		return n
	}
	return "CODE_" + strconv.Itoa(int(c))
}

// pubsubError classifies a failed publish to topic.
func pubsubError(topic string, err error) error {
	if err == nil {
		return nil
	}
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return err
	}
	wrapped := fmt.Errorf("publish to %s: %w", topic, err)
	var paused pubsub.ErrPublishingPaused
	switch {
	case errors.Is(err, pubsub.ErrPublisherStopped):
		return plugin.Wrap(plugin.Transient, nil, errClosed)
	case errors.As(err, &paused):
		// An earlier failure paused the ordering key; the next attempt
		// resumes it.
		return plugin.Wrap(plugin.Transient, nil, wrapped)
	case errors.Is(err, pubsub.ErrOversizedMessage):
		return plugin.Wrap(plugin.Permanent, []string{tagTooLarge, plugin.Tag4xx}, wrapped)
	case errors.Is(err, context.DeadlineExceeded):
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, wrapped)
	case errors.Is(err, context.Canceled):
		return plugin.Wrap(plugin.Transient, nil, wrapped)
	}
	return grpcError(wrapped)
}

// grpcError classifies an RPC failure by its gRPC status: Unavailable,
// Internal, Aborted, Unknown and DataLoss are transient (and tagged 5xx, so
// the default retry_on retries them), ResourceExhausted is rate limited,
// DeadlineExceeded a timeout, and the other codes permanent (tagged 4xx).
// Every status is tagged pubsub:<CODE> and grpc:<CODE>; a failed dial is
// tagged connect too.
func grpcError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		if isConnect(err) {
			return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
		}
		return plugin.Wrap(plugin.Transient, nil, err)
	}
	name := codeName(st.Code())
	tags := []string{"pubsub:" + name, "grpc:" + name}
	switch st.Code() {
	case codes.Unavailable, codes.Internal, codes.Aborted, codes.Unknown, codes.DataLoss:
		tags = append(tags, plugin.Tag5xx)
		if dialFailure(st.Message()) {
			tags = append(tags, plugin.TagConnect)
		}
		return &plugin.Error{Class: plugin.Transient, Tags: tags, Err: err}
	case codes.ResourceExhausted:
		return &plugin.Error{Class: plugin.RateLimited, RetryAfter: retryDelay(st), Tags: tags, Err: err}
	case codes.DeadlineExceeded:
		return &plugin.Error{Class: plugin.Timeout, Tags: append(tags, plugin.TagTimeout), Err: err}
	case codes.Canceled:
		return &plugin.Error{Class: plugin.Transient, Tags: tags, Err: err}
	}
	return &plugin.Error{Class: plugin.Permanent, Tags: append(tags, plugin.Tag4xx), Err: err}
}

// retryDelay is the server's RetryInfo hint, zero without one.
func retryDelay(st *status.Status) time.Duration {
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok && ri.GetRetryDelay() != nil {
			return ri.GetRetryDelay().AsDuration()
		}
	}
	return 0
}

// dialMarkers are what gRPC says when it could not reach the server.
var dialMarkers = []string{
	"connection error", "error while dialing", "connection refused", "no such host",
	"produced zero addresses", "connection reset", "transport is closing",
}

// dialFailure reports whether an Unavailable status says the connection
// failed rather than the service.
func dialFailure(msg string) bool {
	msg = strings.ToLower(msg)
	for _, m := range dialMarkers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

// gcsError classifies a failed Cloud Storage call. An HTTP status follows
// HTTP's semantics (429 rate limited with Retry-After, 408 and 504 timeouts,
// other 5xx transient, other 4xx permanent) and is tagged gcs:<code> too; a
// failed connection is transient and tagged connect.
func gcsError(what string, err error) error {
	if err == nil {
		return nil
	}
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return err
	}
	wrapped := fmt.Errorf("%s: %w", what, err)
	var ge *googleapi.Error
	switch {
	case errors.As(err, &ge):
		return gcsStatus(ge.Code, plugin.ParseRetryAfter(ge.Header.Get("Retry-After"), time.Now()), wrapped)
	case errors.Is(err, storage.ErrBucketNotExist), errors.Is(err, storage.ErrObjectNotExist):
		return gcsStatus(http.StatusNotFound, 0, wrapped)
	case isTimeout(err):
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, wrapped)
	case errors.Is(err, context.Canceled):
		return plugin.Wrap(plugin.Transient, nil, wrapped)
	case isConnect(err):
		return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, wrapped)
	}
	return plugin.Wrap(plugin.Transient, nil, wrapped)
}

// gcsStatus classifies an HTTP status as an HTTP destination would, and
// tags it gcs:<code>.
func gcsStatus(code int, retryAfter time.Duration, err error) error {
	var pe *plugin.Error
	if !errors.As(plugin.StatusError(code, retryAfter, ""), &pe) {
		return plugin.Wrap(plugin.Transient, nil, err)
	}
	pe.Tags = append(pe.Tags, "gcs:"+strconv.Itoa(code))
	pe.Err = err
	return pe
}

// isPrecondition reports whether a write failed its precondition (412): the
// object already exists.
func isPrecondition(err error) bool {
	var ge *googleapi.Error
	return errors.As(err, &ge) && ge.Code == http.StatusPreconditionFailed
}

// isNotFound reports whether the object (or bucket) does not exist.
func isNotFound(err error) bool {
	var ge *googleapi.Error
	return errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) ||
		(errors.As(err, &ge) && ge.Code == http.StatusNotFound)
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// isConnect reports a failure to reach the server: refused, reset or
// unresolved, or a connection that ended mid-call.
func isConnect(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) {
		return true
	}
	var oe *net.OpError
	if errors.As(err, &oe) {
		return true
	}
	var de *net.DNSError
	return errors.As(err, &de)
}
