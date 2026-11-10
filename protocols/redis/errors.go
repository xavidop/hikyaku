package redis

import (
	"context"
	"errors"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xavidop/hikyaku/plugin"
)

// oomRetryAfter is the wait suggested after an OOM: Redis evicts or frees
// memory on its own schedule and gives no hint.
const oomRetryAfter = time.Second

// classify turns a go-redis failure into a *plugin.Error:
//
//   - a Redis error reply, by its prefix, tagged redis:<PREFIX>: LOADING,
//     BUSY, TRYAGAIN, CLUSTERDOWN, MASTERDOWN and READONLY are transient,
//     OOM is rate limited (retry after 1s), WRONGTYPE, NOPERM, NOAUTH,
//     WRONGPASS and ERR are permanent, and any other prefix is transient;
//   - the attempt's deadline: timeout;
//   - anything else (refused or broken connection, i/o or pool timeout,
//     TLS, closed client): transient, tagged connect.
func classify(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return err
	}
	var re goredis.Error
	if errors.As(err, &re) && !errors.Is(err, goredis.Nil) {
		// The server answered: whatever the clock says, this is its verdict.
		return replyError(re.Error(), err)
	}
	if errors.Is(err, context.DeadlineExceeded) || deadlinePassed(ctx) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	if errors.Is(err, context.Canceled) {
		return plugin.Wrap(plugin.Transient, nil, err)
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
}

// deadlinePassed reports whether ctx's deadline is reached. go-redis sets
// the socket deadline to the context's, so a read cut by it can return
// before ctx.Err() is set.
func deadlinePassed(ctx context.Context) bool {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	dl, ok := ctx.Deadline()
	return ok && !time.Now().Before(dl)
}

// replyError classifies a Redis error reply by its prefix.
func replyError(msg string, err error) error {
	prefix, _, _ := strings.Cut(msg, " ")
	if !isCode(prefix) {
		// Not a Redis error code (a proxy's free text): retry it.
		return plugin.Wrap(plugin.Transient, nil, err)
	}
	tag := "redis:" + prefix
	switch prefix {
	case "LOADING", "BUSY", "TRYAGAIN", "CLUSTERDOWN", "MASTERDOWN", "READONLY":
		return plugin.Wrap(plugin.Transient, []string{tag}, err)
	case "OOM":
		return &plugin.Error{Class: plugin.RateLimited, RetryAfter: oomRetryAfter, Tags: []string{tag}, Err: err}
	case "ERR":
		if strings.Contains(msg, "max number of clients reached") {
			// The server refused the connection; it is not the message.
			return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect, tag}, err)
		}
		return plugin.Wrap(plugin.Permanent, []string{tag}, err)
	case "WRONGTYPE", "NOPERM", "NOAUTH", "WRONGPASS":
		return plugin.Wrap(plugin.Permanent, []string{tag}, err)
	}
	return plugin.Wrap(plugin.Transient, []string{tag}, err)
}

// isCode reports whether s looks like a Redis error code: upper-case
// letters, digits and underscores.
func isCode(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

// hasCode reports whether err is a Redis error reply with this code, such
// as BUSYGROUP or NOGROUP.
func hasCode(err error, code string) bool {
	var re goredis.Error
	if !errors.As(err, &re) {
		return false
	}
	prefix, _, _ := strings.Cut(re.Error(), " ")
	return prefix == code
}
