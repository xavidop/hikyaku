package tcp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/xavidop/hikyaku/plugin"
)

// errClosed is what a destination answers once it is closed.
var errClosed = errors.New("the destination is closed")

// errnoCodes are the socket errors a connection failure is tagged with, as
// <kind>:<NAME> (tcp:ECONNREFUSED), next to connect.
var errnoCodes = map[syscall.Errno]string{
	syscall.ECONNREFUSED: "ECONNREFUSED",
	syscall.ECONNRESET:   "ECONNRESET",
	syscall.EPIPE:        "EPIPE",
	syscall.ETIMEDOUT:    "ETIMEDOUT",
	syscall.EHOSTUNREACH: "EHOSTUNREACH",
	syscall.ENETUNREACH:  "ENETUNREACH",
}

// Tags of the failures a destination finds before it writes anything.
const (
	tagFrameTooLarge = "frame_too_large" // <kind>:frame_too_large
	tagNewline       = "newline_in_body" // <kind>:newline_in_body
	tagEmpty         = "empty_body"      // <kind>:empty_body
	tagTemplate      = "template"
)

// connectTags tags a connection failure: connect, and the socket error
// when there is one the relay knows.
func connectTags(kind string, err error) []string {
	tags := []string{plugin.TagConnect}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		if name, ok := errnoCodes[errno]; ok {
			tags = append(tags, kind+":"+name)
		}
	}
	return tags
}

// ctxError classifies a failure caused by the attempt's context: a deadline
// is a timeout; a cancellation (shutdown) is transient.
func ctxError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	return plugin.Wrap(plugin.Transient, nil, err)
}

// dialError classifies a failed dial or TLS (or DTLS) handshake: it did not
// connect.
func dialError(ctx context.Context, kind, endpoint string, err error) error {
	err = fmt.Errorf("dial %s: %w", endpoint, err)
	if ctx.Err() != nil {
		return ctxError(ctx, err)
	}
	return plugin.Wrap(plugin.Transient, connectTags(kind, err), err)
}

// writeError classifies a failed write. The connection is gone either way:
// a part of the frame may have been written.
func writeError(ctx context.Context, kind, endpoint string, err error) error {
	err = fmt.Errorf("write to %s: %w", endpoint, err)
	switch {
	case ctx.Err() != nil:
		return ctxError(ctx, err)
	case errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		// A socket's write deadline; pion/dtls reports its own as
		// context.DeadlineExceeded.
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	case errors.Is(err, syscall.EMSGSIZE):
		// A datagram over what the network carries: it will never fit.
		return plugin.Wrap(plugin.Permanent, []string{kind + ":" + tagFrameTooLarge}, err)
	}
	return plugin.Wrap(plugin.Transient, connectTags(kind, err), err)
}

// encodeError classifies a message the framing cannot carry: it never
// will, so it is permanent.
func encodeError(kind, endpoint string, err error) error {
	tag := kind + ":" + tagFrameTooLarge
	switch {
	case errors.Is(err, errNewlineInBody):
		tag = kind + ":" + tagNewline
	case errors.Is(err, errEmptyBody):
		tag = kind + ":" + tagEmpty
	}
	return plugin.Wrap(plugin.Permanent, []string{tag}, fmt.Errorf("%s: %w", endpoint, err))
}
