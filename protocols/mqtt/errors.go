package mqtt

import (
	"context"
	"errors"
	"fmt"

	"github.com/xavidop/hikyaku/plugin"
)

// errUnsupported: the broker does not offer what a publish needs (a QoS above
// its maximum, retain when it has none). Retrying cannot help.
var errUnsupported = errors.New("the broker does not support this publish")

// errClosed is what a destination answers once it is closed.
var errClosed = errors.New("mqtt destination is closed")

// reasonError is an MQTT 5 reason code of 0x80 or more on a PUBACK, PUBREC
// or SUBACK: the broker refused the message or the subscription.
type reasonError struct {
	code   byte
	reason string // the broker's reason string, if it sent one
}

func (e *reasonError) Error() string {
	return "the broker refused it (" + describeCode(e.code, e.reason) + ")"
}

// connackError: the broker refused the connection.
type connackError struct {
	code   byte
	reason string
	v3     bool // a 3.1.1 return code, not an MQTT 5 reason code
}

func (e *connackError) Error() string {
	if e.v3 {
		return fmt.Sprintf("the broker refused the connection (return code %d %s)", e.code, v3ConnackName(e.code))
	}
	return "the broker refused the connection (" + describeCode(e.code, e.reason) + ")"
}

// disconnectedError: the broker ended the connection with DISCONNECT.
type disconnectedError struct {
	code   byte
	reason string
}

func (e *disconnectedError) Error() string {
	return "the broker disconnected (" + describeCode(e.code, e.reason) + ")"
}

func describeCode(code byte, reason string) string {
	s := fmt.Sprintf("0x%02x %s", code, reasonName(code))
	if reason != "" {
		s += ": " + reason
	}
	return s
}

// reasonName names an MQTT 5 reason code (MQTT 5 §2.4).
func reasonName(code byte) string {
	if n, ok := reasonNames[code]; ok {
		return n
	}
	return "error"
}

var reasonNames = map[byte]string{
	0x00: "success", 0x10: "no matching subscribers",
	0x80: "unspecified error", 0x81: "malformed packet", 0x82: "protocol error",
	0x83: "implementation specific error", 0x84: "unsupported protocol version",
	0x85: "client identifier not valid", 0x86: "bad user name or password", 0x87: "not authorized",
	0x88: "server unavailable", 0x89: "server busy", 0x8a: "banned", 0x8b: "server shutting down",
	0x8c: "bad authentication method", 0x8d: "keep alive timeout", 0x8e: "session taken over",
	0x8f: "topic filter invalid", 0x90: "topic name invalid", 0x91: "packet identifier in use",
	0x92: "packet identifier not found", 0x93: "receive maximum exceeded", 0x94: "topic alias invalid",
	0x95: "packet too large", 0x96: "message rate too high", 0x97: "quota exceeded",
	0x98: "administrative action", 0x99: "payload format invalid", 0x9a: "retain not supported",
	0x9b: "QoS not supported", 0x9c: "use another server", 0x9d: "server moved",
	0x9e: "shared subscriptions not supported", 0x9f: "connection rate exceeded",
	0xa0: "maximum connect time", 0xa1: "subscription identifiers not supported",
	0xa2: "wildcard subscriptions not supported",
}

// v3ConnackName names an MQTT 3.1.1 CONNACK return code.
func v3ConnackName(code byte) string {
	switch code {
	case 1:
		return "unacceptable protocol version"
	case 2:
		return "identifier rejected"
	case 3:
		return "server unavailable"
	case 4:
		return "bad user name or password"
	case 5:
		return "not authorized"
	}
	return "refused"
}

// reasonTag is the error tag of a reason code, such as mqtt:0x97.
func reasonTag(code byte) string { return fmt.Sprintf("mqtt:0x%02x", code) }

// reasonClass classifies a refused publish. The broker is up and said no:
// not authorized (0x87), an invalid topic name (0x90), a packet too large
// (0x95), an invalid payload format (0x99) or an implementation specific
// refusal (0x83) will happen again; a quota (0x97) is a throttle; anything
// else, such as server busy (0x89), server unavailable (0x88) or an
// unspecified error (0x80), may not.
func reasonClass(code byte) plugin.Class {
	switch code {
	case 0x83, 0x87, 0x90, 0x95, 0x99:
		return plugin.Permanent
	case 0x97:
		return plugin.RateLimited
	}
	return plugin.Transient
}

// ctxError classifies a failure caused by the attempt's context: a deadline
// is a timeout; a cancellation (shutdown) is transient.
func ctxError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	return plugin.Wrap(plugin.Transient, nil, err)
}

// notConnected is a delivery's failure while the destination has no
// connection; why is the last connect failure or loss.
func notConnected(endpoint string, why error) error {
	if why == nil {
		why = errNotConnected
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, fmt.Errorf("not connected to %s: %w", endpoint, why))
}

// publishError classifies a failed publish: a reason code is mapped by
// reasonClass and tagged mqtt:0x<code>; a deadline is a timeout; what the
// broker cannot support is permanent; anything else lost the connection.
func publishError(ctx context.Context, endpoint string, err error) error {
	wrapped := fmt.Errorf("publish to %s: %w", endpoint, err)
	var re *reasonError
	switch {
	case errors.As(err, &re):
		return &plugin.Error{Class: reasonClass(re.code), Tags: []string{reasonTag(re.code)}, Err: wrapped}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), ctx.Err() != nil:
		return ctxError(ctx, wrapped)
	case errors.Is(err, errUnsupported):
		return plugin.Wrap(plugin.Permanent, nil, wrapped)
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, wrapped)
}
