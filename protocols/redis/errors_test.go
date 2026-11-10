package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/xavidop/hikyaku/plugin"
)

// replyErr is a Redis error reply, as go-redis returns one.
type replyErr string

func (e replyErr) Error() string { return string(e) }
func (replyErr) RedisError()     {}

var _ goredis.Error = replyErr("")

func TestClassify(t *testing.T) {
	ctx := context.Background()
	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	for name, tc := range map[string]struct {
		ctx   context.Context
		err   error
		class plugin.Class
		tags  []string
	}{
		"refused":         {ctx, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, plugin.Transient, []string{"connect"}},
		"eof":             {ctx, io.EOF, plugin.Transient, []string{"connect"}},
		"pool timeout":    {ctx, goredis.ErrPoolTimeout, plugin.Transient, []string{"connect"}},
		"closed client":   {ctx, goredis.ErrClosed, plugin.Transient, []string{"connect"}},
		"io timeout":      {ctx, &net.OpError{Op: "read", Err: timeoutErr{}}, plugin.Transient, []string{"connect"}},
		"deadline":        {ctx, context.DeadlineExceeded, plugin.Timeout, []string{"timeout"}},
		"deadline passed": {expired, &net.OpError{Op: "read", Err: timeoutErr{}}, plugin.Timeout, []string{"timeout"}},
		"canceled":        {ctx, context.Canceled, plugin.Transient, nil},
		"LOADING":         {ctx, replyErr("LOADING Redis is loading the dataset in memory"), plugin.Transient, []string{"redis:LOADING"}},
		"BUSY":            {ctx, replyErr("BUSY Redis is busy running a script"), plugin.Transient, []string{"redis:BUSY"}},
		"TRYAGAIN":        {ctx, replyErr("TRYAGAIN Multiple keys request during rehashing of slot"), plugin.Transient, []string{"redis:TRYAGAIN"}},
		"CLUSTERDOWN":     {ctx, replyErr("CLUSTERDOWN The cluster is down"), plugin.Transient, []string{"redis:CLUSTERDOWN"}},
		"MASTERDOWN":      {ctx, replyErr("MASTERDOWN Link with MASTER is down"), plugin.Transient, []string{"redis:MASTERDOWN"}},
		"READONLY":        {ctx, replyErr("READONLY You can't write against a read only replica."), plugin.Transient, []string{"redis:READONLY"}},
		"OOM":             {ctx, replyErr("OOM command not allowed when used memory > 'maxmemory'."), plugin.RateLimited, []string{"redis:OOM", "429"}},
		"WRONGTYPE":       {ctx, replyErr("WRONGTYPE Operation against a key holding the wrong kind of value"), plugin.Permanent, []string{"redis:WRONGTYPE"}},
		"NOPERM":          {ctx, replyErr("NOPERM User relay has no permissions to run the 'xadd' command"), plugin.Permanent, []string{"redis:NOPERM"}},
		"NOAUTH":          {ctx, replyErr("NOAUTH Authentication required."), plugin.Permanent, []string{"redis:NOAUTH"}},
		"WRONGPASS":       {ctx, replyErr("WRONGPASS invalid username-password pair or user is disabled."), plugin.Permanent, []string{"redis:WRONGPASS"}},
		"ERR":             {ctx, replyErr("ERR wrong number of arguments for 'xadd' command"), plugin.Permanent, []string{"redis:ERR"}},
		"max clients":     {ctx, replyErr("ERR max number of clients reached"), plugin.Transient, []string{"connect", "redis:ERR"}},
		"unknown code":    {ctx, replyErr("MISCONF Redis is configured to save RDB snapshots"), plugin.Transient, []string{"redis:MISCONF"}},
		"free text":       {ctx, replyErr("proxy said no"), plugin.Transient, nil},
		"reply past deadline": {expired, replyErr("WRONGTYPE Operation against a key holding the wrong kind of value"),
			plugin.Permanent, []string{"redis:WRONGTYPE"}},
		"wrapped reply": {ctx, fmt.Errorf("XADD s: %w", replyErr("NOPERM no")), plugin.Permanent, []string{"redis:NOPERM"}},
	} {
		t.Run(name, func(t *testing.T) {
			err := classify(tc.ctx, tc.err)
			requirePluginError(t, err)
			class, tags := classOf(err)
			assert.Equal(t, tc.class, class)
			assert.Equal(t, tc.tags, tags)
			assert.ErrorIs(t, err, tc.err, "the cause stays reachable")
		})
	}
	assert.NoError(t, classify(ctx, nil))
	pe := &plugin.Error{Class: plugin.Permanent}
	assert.Same(t, pe, classify(ctx, pe).(*plugin.Error), "already classified")
	_, retryAfter := plugin.Classify(classify(ctx, replyErr("OOM full")))
	assert.Equal(t, time.Second, retryAfter, "OOM asks for a second")
}

func TestHasCode(t *testing.T) {
	assert.True(t, hasCode(fmt.Errorf("x: %w", replyErr("BUSYGROUP Consumer Group name already exists")), "BUSYGROUP"))
	assert.False(t, hasCode(replyErr("BUSY script"), "BUSYGROUP"))
	assert.False(t, hasCode(errors.New("NOGROUP plain error"), "NOGROUP"), "only Redis replies")
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
