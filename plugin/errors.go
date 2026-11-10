package plugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Class is the broad kind of a delivery failure.
type Class int

const (
	// Transient failures may not happen again: a dropped connection, a 5xx.
	Transient Class = iota
	// RateLimited: the endpoint asked us to slow down (HTTP 429, a broker
	// throttle); RetryAfter says for how long when it said.
	RateLimited
	// Permanent failures will happen again: a 4xx, a message the endpoint
	// rejects. They go straight to the fallback or the DLQ.
	Permanent
	// Timeout: the attempt ran out of time.
	Timeout
)

func (c Class) String() string {
	switch c {
	case Transient:
		return "transient"
	case RateLimited:
		return "rate_limited"
	case Permanent:
		return "permanent"
	case Timeout:
		return "timeout"
	}
	return "unknown"
}

// Error tags. A failure carries every tag that describes it, from the broad
// to the exact: an HTTP 503 is "5xx" and "503"; a 408 is "4xx", "408" and
// "timeout". retry.retry_on and retry.never_on match against them. Protocols
// add their own as "<protocol>:<code>", such as "ws:close_1011".
const (
	TagConnect = "connect"
	TagTimeout = "timeout"
	Tag4xx     = "4xx"
	Tag5xx     = "5xx"
	Tag429     = "429"
)

// Tags of the routing failures that dead-letter a message. The engine puts
// them on the error of a DeadLettered outcome, so a source can tell them
// from a delivery failure (the openai source answers 404 for no_route).
const (
	TagNoRoute   = "no_route"
	TagTransform = "transform"
	// TagSchema: a route's validate step refused the body. The http source
	// answers 422 for it, the openai source 400.
	TagSchema = "schema"
)

// Error is a classified delivery failure.
type Error struct {
	Class Class
	// RetryAfter is the endpoint's own hint (Retry-After, a broker backoff),
	// zero when it gave none.
	RetryAfter time.Duration
	Tags       []string
	Err        error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return e.Class.String()
	}
	return e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// Errorf returns a classified error.
func Errorf(class Class, tags []string, format string, args ...any) error {
	return &Error{Class: class, Tags: tags, Err: fmt.Errorf(format, args...)}
}

// Wrap classifies an existing error.
func Wrap(class Class, tags []string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Class: class, Tags: tags, Err: err}
}

// Permanentf returns a permanent error: the message will never be accepted.
func Permanentf(format string, args ...any) error {
	return &Error{Class: Permanent, Err: fmt.Errorf(format, args...)}
}

// StatusError classifies a protocol status code that follows HTTP's
// semantics: 429 is rate limited (with retryAfter), 408 and 504 are timeouts,
// other 5xx are transient, and other 4xx are permanent.
func StatusError(code int, retryAfter time.Duration, msg string) error {
	tags := []string{strconv.Itoa(code)}
	class := Transient
	switch {
	case code == http.StatusTooManyRequests:
		class = RateLimited
		tags = append(tags, Tag4xx)
	case code == http.StatusRequestTimeout:
		class = Timeout
		tags = append(tags, Tag4xx, TagTimeout)
	case code >= 400 && code < 500:
		class = Permanent
		tags = append(tags, Tag4xx)
	case code == http.StatusGatewayTimeout:
		class = Timeout
		tags = append(tags, Tag5xx)
	case code >= 500 && code < 600:
		tags = append(tags, Tag5xx)
	}
	text := http.StatusText(code)
	if msg != "" {
		text += ": " + msg
	}
	return &Error{Class: class, RetryAfter: retryAfter, Tags: tags, Err: fmt.Errorf("status %d %s", code, text)}
}

// Classify returns a failure's class and the endpoint's retry-after hint. An
// unclassified error is classified from what it is: a timeout, a refused or
// reset connection, or else transient.
func Classify(err error) (Class, time.Duration) {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Class, pe.RetryAfter
	}
	if isTimeout(err) {
		return Timeout, 0
	}
	return Transient, 0
}

// Tags returns the tags of a failure. Unclassified errors get "timeout" or
// "connect" when that is what they are, and no tags otherwise.
func Tags(err error) []string {
	var pe *Error
	if errors.As(err, &pe) {
		tags := slices.Clone(pe.Tags)
		if pe.Class == Timeout && !slices.Contains(tags, TagTimeout) {
			tags = append(tags, TagTimeout)
		}
		if pe.Class == RateLimited && !slices.Contains(tags, Tag429) {
			tags = append(tags, Tag429)
		}
		return tags
	}
	switch {
	case isTimeout(err):
		return []string{TagTimeout}
	case isConnect(err):
		return []string{TagConnect}
	}
	return nil
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

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

// ParseRetryAfter reads a Retry-After value: delay-seconds or an HTTP date.
// Anything unparsable, or in the past, is zero.
func ParseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseFloat(v, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// Retryable reports whether a failure with class and tags is retried under
// a retry policy's retry_on and never_on tag lists. The more specific
// matching tag wins: a code (429) or a protocol tag (ws:close_1011) beats a
// range (4xx, 5xx), and a tie goes to retry_on. A failure whose tags match
// neither list falls back to its class: anything but Permanent is retried.
// Tags compare case-insensitively.
func Retryable(class Class, tags, retryOn, neverOn []string) bool {
	r := tagSpecificity(retryOn, tags)
	n := tagSpecificity(neverOn, tags)
	if r == 0 && n == 0 {
		return class != Permanent
	}
	return r > 0 && r >= n
}

func tagSpecificity(list, tags []string) int {
	best := 0
	for _, t := range tags {
		if !slices.ContainsFunc(list, func(l string) bool { return strings.EqualFold(strings.TrimSpace(l), t) }) {
			continue
		}
		s := 2
		if strings.EqualFold(t, Tag4xx) || strings.EqualFold(t, Tag5xx) {
			s = 1
		}
		best = max(best, s)
	}
	return best
}
