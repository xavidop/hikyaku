package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xavidop/hikyaku/plugin"
)

// Error tags of NATS failures, besides connect and timeout.
const (
	tagPermissions  = "nats:permissions"
	tagMaxPayload   = "nats:max_payload"
	tagNoResponders = "jetstream:no_responders"
	tagTemplate     = "template"
)

var (
	errClosed       = errors.New("nats destination is closed")
	errNotConnected = errors.New("not connected")
)

// connErrors are the failures of a connection that is down, closed or
// reconnecting: the message may go through once it is back.
var connErrors = []error{
	nats.ErrNoServers, nats.ErrConnectionClosed, nats.ErrDisconnected, nats.ErrConnectionReconnecting,
	nats.ErrConnectionDraining, nats.ErrReconnectBufExceeded, nats.ErrStaleConnection,
	nats.ErrInvalidConnection, jetstream.ErrConnectionClosed, jetstream.ErrJetStreamPublisherClosed,
	errClosed, errNotConnected,
}

// classify turns a publish failure into a *plugin.Error:
//
//   - timeouts (the attempt's deadline, a missing ack) are Timeout;
//   - a connection that is down, closed or reconnecting is Transient,
//     tagged connect;
//   - a permission violation is Permanent, tagged nats:permissions;
//   - jetstream: no stream takes the subject (no responders) is Permanent,
//     tagged jetstream:no_responders; an API error is tagged
//     jetstream:<err_code> and classed by its code like HTTP (503
//     transient, 429 rate limited, other 4xx permanent);
//   - a message over the server's max_payload is Permanent, tagged
//     nats:max_payload.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return err
	}
	var apiErr *jetstream.APIError
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout),
		errors.Is(err, jetstream.ErrAsyncPublishTimeout):
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	case errors.Is(err, context.Canceled):
		return plugin.Wrap(plugin.Transient, nil, err)
	case errors.Is(err, nats.ErrPermissionViolation):
		return plugin.Wrap(plugin.Permanent, []string{tagPermissions}, err)
	case errors.Is(err, jetstream.ErrNoStreamResponse), errors.Is(err, nats.ErrNoResponders):
		return plugin.Wrap(plugin.Permanent, []string{tagNoResponders}, fmt.Errorf("no stream takes the subject: %w", err))
	case errors.As(err, &apiErr):
		return apiError(apiErr, err)
	case errors.Is(err, jetstream.ErrTooManyStalledMsgs):
		return plugin.Wrap(plugin.RateLimited, nil, err)
	case errors.Is(err, nats.ErrMaxPayload):
		return plugin.Wrap(plugin.Permanent, []string{tagMaxPayload}, err)
	case errors.Is(err, nats.ErrBadSubject), errors.Is(err, nats.ErrBadHeaderMsg), errors.Is(err, nats.ErrHeadersNotSupported):
		return plugin.Wrap(plugin.Permanent, nil, err)
	case isConnError(err):
		return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
	}
	return plugin.Wrap(plugin.Transient, plugin.Tags(err), err)
}

func isConnError(err error) bool {
	for _, e := range connErrors {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// apiError classes a JetStream API error by its code, the way HTTP status
// codes are, and tags it jetstream:<err_code> (jetstream:10060 is "expected
// stream does not match").
func apiError(ae *jetstream.APIError, err error) error {
	var tags []string
	if ae.ErrorCode != 0 {
		tags = append(tags, "jetstream:"+strconv.Itoa(int(ae.ErrorCode)))
	}
	if ae.Code < 400 || ae.Code > 599 {
		return &plugin.Error{Class: plugin.Transient, Tags: tags, Err: err}
	}
	var se *plugin.Error
	if !errors.As(plugin.StatusError(ae.Code, 0, ""), &se) {
		return &plugin.Error{Class: plugin.Transient, Tags: tags, Err: err}
	}
	return &plugin.Error{Class: se.Class, Tags: append(tags, se.Tags...), Err: err}
}

// deliverError classifies a failure of an attempt, using the attempt's
// context: a failure after its deadline passed is a timeout, whatever the
// client made of it.
func deliverError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		var pe *plugin.Error
		if !errors.As(err, &pe) {
			return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
		}
	}
	return classify(err)
}
