package llm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/firebase/genkit/go/core/status"

	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
)

// ErrBadOutput marks a reply that was not the answer asked for: invalid
// JSON, or destinations that are not candidates. It is retried like a
// transport failure, because the same model usually gets it right the next
// time (design §6.3).
var ErrBadOutput = errors.New("the model did not return a valid routing answer")

// modelRetry is models.retry with its defaults.
type modelRetry struct {
	attempts   int
	backoff    time.Duration
	maxBackoff time.Duration
}

func newModelRetry(r config.ModelRetry) modelRetry {
	out := modelRetry{attempts: r.Attempts, backoff: r.Backoff.Std(), maxBackoff: r.MaxBackoff.Std()}
	if out.attempts <= 0 {
		out.attempts = config.DefaultModelAttempts
	}
	if out.backoff <= 0 {
		out.backoff = config.DefaultModelBackoff
	}
	if out.maxBackoff <= 0 {
		out.maxBackoff = config.DefaultModelMaxBackoff
	}
	return out
}

// chainCall is one model call of a chain.
type chainCall[T any] func(ctx context.Context, model string) (T, error)

// callChain runs fn with each model of the chain until one answers (design
// §7.3). A transient failure (rate limit, outage, timeout, bad output) is
// retried on the same model up to models.retry.attempts times with jittered
// backoff; a failure the model will repeat (a bad request, an unknown
// model, a refused key) moves straight to the next model. It returns the
// result, the model that answered, and the hops before it.
//
// The System One plugin retries transport failures twice inside one call,
// so one attempt here can be up to three HTTP requests to a decision server.
func callChain[T any](ctx context.Context, route string, models []string, r modelRetry, m *metrics.Metrics, log *slog.Logger, fn chainCall[T]) (T, string, []string, error) {
	var zero T
	var hops []string
	var last error
	for i, model := range models {
		for n := 1; ; n++ {
			out, err := fn(ctx, model)
			if err == nil {
				return out, model, hops, nil
			}
			last = err
			if ctx.Err() != nil {
				return zero, "", hops, fmt.Errorf("%w (with %s: %v)", ctx.Err(), model, err)
			}
			if retryable(err) && n < r.attempts {
				wait := backoff.Delay(r.backoff, r.maxBackoff, "equal", n, nil)
				log.Warn("model call failed, retrying", "route", route, "model", model, "attempt", n, "of", r.attempts, "wait", wait.Round(time.Millisecond).String(), "err", err)
				if !backoff.Sleep(ctx, wait) {
					return zero, "", hops, fmt.Errorf("%w (waiting to retry %s: %v)", ctx.Err(), model, err)
				}
				continue
			}
			hops = append(hops, fmt.Sprintf("%s: %s", model, short(err.Error(), 200)))
			if i+1 < len(models) {
				reason := reasonOf(err)
				if m != nil {
					m.AIModelFallback.WithLabelValues(route, model, models[i+1], reason).Inc()
				}
				log.Warn("model failed, trying the fallback model", "route", route, "model", model, "fallback", models[i+1], "reason", reason, "err", err)
			}
			break
		}
	}
	if last == nil {
		last = errors.New("no model to call")
	}
	return zero, "", hops, fmt.Errorf("every model failed (%s): %w", strings.Join(models, ", "), last)
}

// retryable reports a failure the same model may not repeat.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrBadOutput) || errors.Is(err, status.ErrInvalidOutput) {
		return true
	}
	if s, ok := status.Classified(err); ok {
		switch s {
		case status.Unavailable, status.ResourceExhausted, status.DeadlineExceeded, status.Aborted, status.Internal, status.Unknown:
			return true
		}
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, hint := range []string{" 429", "rate limit", "overloaded", " 500", " 502", " 503", " 504", "unavailable", "timeout", "timed out", "try again", "resource exhausted"} {
		if strings.Contains(msg, hint) {
			return true
		}
	}
	return false
}

// reasonOf labels a model fallback hop.
func reasonOf(err error) string {
	switch {
	case errors.Is(err, ErrBadOutput) || errors.Is(err, status.ErrInvalidOutput):
		return "bad_output"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	if s, ok := status.Classified(err); ok {
		switch s {
		case status.ResourceExhausted:
			return "rate_limited"
		case status.Unavailable, status.Internal, status.Unknown, status.Aborted:
			return "unavailable"
		case status.DeadlineExceeded:
			return "timeout"
		case status.NotFound:
			return "not_found"
		case status.PermissionDenied, status.Unauthenticated:
			return "auth"
		}
		return strings.ToLower(string(s))
	}
	return "error"
}

func short(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
