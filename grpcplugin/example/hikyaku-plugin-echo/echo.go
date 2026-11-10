package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// EchoOptions are the settings of an echo destination, under options: in
// the spec. The yaml tags name the keys; the jsonschema tags become the
// JSON Schema hikyaku validates options: blocks with.
type EchoOptions struct {
	File      string `yaml:"file" jsonschema:"required" jsonschema_description:"File each message body is appended to, one per line."`
	TestHooks bool   `yaml:"test_hooks,omitempty" jsonschema_description:"Honour the X-Echo-* headers: failures, sleeps and reports on demand, for tests."`
}

// Test hook headers (with test_hooks: true).
const (
	// HeaderFail fails the delivery with a class: transient,
	// rate_limited, permanent or timeout.
	HeaderFail = "X-Echo-Fail"
	// HeaderRetryAfter is the failure's retry-after hint (a duration).
	HeaderRetryAfter = "X-Echo-Retry-After"
	// HeaderTags are the failure's tags, comma-separated.
	HeaderTags = "X-Echo-Tags"
	// HeaderSleep sleeps (a duration) after writing, or until the
	// delivery's deadline.
	HeaderSleep = "X-Echo-Sleep"
	// HeaderEnv names an environment variable to report in the reply
	// (X-Echo-Env-Value, X-Echo-Env-Set).
	HeaderEnv = "X-Echo-Env"
)

// Echo appends each body to a file and replies with it.
type Echo struct {
	name  string
	path  string
	hooks bool
	log   *slog.Logger

	mu sync.Mutex
	f  *os.File
}

func newEcho(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
	var o EchoOptions
	if err := spec.Decode(&o); err != nil {
		return nil, err
	}
	if o.File == "" {
		return nil, fmt.Errorf("echo %s: options.file is required", spec.Name)
	}
	f, err := os.OpenFile(o.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("echo %s: %w", spec.Name, err)
	}
	log := spec.Env.Log()
	log.Info("echo opened", "file", o.File)
	return &Echo{name: spec.Name, path: o.File, hooks: o.TestHooks, log: log, f: f}, nil
}

// Name is the destination name.
func (e *Echo) Name() string { return e.name }

// Deliver appends the body and replies with it.
func (e *Echo) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if e.hooks {
		if err := failOnDemand(env); err != nil {
			return nil, err
		}
	}
	if err := e.write(env.Body); err != nil {
		return nil, err
	}
	res := &plugin.Result{
		Status:      200,
		Headers:     map[string][]string{"X-Echo-Id": {env.ID}},
		Body:        env.Body,
		ContentType: env.ContentType,
		Endpoint:    e.path,
	}
	if e.hooks {
		if err := sleepOnDemand(ctx, env); err != nil {
			return nil, err
		}
		report(ctx, env, res)
	}
	return res, nil
}

// DeliverBatch appends every body in one write (plugin.BatchSink).
func (e *Echo) DeliverBatch(_ context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	var b bytes.Buffer
	for _, env := range envs {
		b.Write(env.Body)
		b.WriteByte('\n')
	}
	if err := e.writeRaw(b.Bytes()); err != nil {
		return nil, err
	}
	return &plugin.Result{Status: 200, Headers: map[string][]string{"X-Echo-Batch": {strconv.Itoa(len(envs))}}, Endpoint: e.path}, nil
}

func (e *Echo) write(body []byte) error {
	return e.writeRaw(append(append([]byte(nil), body...), '\n'))
}

func (e *Echo) writeRaw(b []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.f == nil {
		return plugin.Permanentf("echo %s is closed", e.name)
	}
	if _, err := e.f.Write(b); err != nil {
		return plugin.Wrap(plugin.Transient, nil, err)
	}
	return nil
}

// Health checks the file is still there.
func (e *Echo) Health(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.f == nil {
		return errors.New("closed")
	}
	_, err := e.f.Stat()
	return err
}

// Close closes the file.
func (e *Echo) Close(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.f == nil {
		return nil
	}
	err := e.f.Close()
	e.f = nil
	e.log.Info("echo closed", "file", e.path)
	return err
}

// failOnDemand returns the failure X-Echo-Fail asks for.
func failOnDemand(env *envelope.Envelope) error {
	kind := env.Header(HeaderFail)
	if kind == "" {
		return nil
	}
	pe := &plugin.Error{Err: fmt.Errorf("echo: failing as asked (%s)", kind)}
	switch kind {
	case "transient":
		pe.Class = plugin.Transient
	case "rate_limited":
		pe.Class = plugin.RateLimited
	case "permanent":
		pe.Class = plugin.Permanent
	case "timeout":
		pe.Class = plugin.Timeout
	default:
		return plugin.Permanentf("echo: %s: unknown class %q", HeaderFail, kind)
	}
	if v := env.Header(HeaderRetryAfter); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return plugin.Permanentf("echo: %s: %v", HeaderRetryAfter, err)
		}
		pe.RetryAfter = d
	}
	if v := env.Header(HeaderTags); v != "" {
		pe.Tags = strings.Split(v, ",")
	}
	return pe
}

// sleepOnDemand sleeps for X-Echo-Sleep, or until the deadline.
func sleepOnDemand(ctx context.Context, env *envelope.Envelope) error {
	v := env.Header(HeaderSleep)
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return plugin.Permanentf("echo: %s: %v", HeaderSleep, err)
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err() // classified as a timeout when the deadline passed
	}
}

// report adds what the delivery saw to the reply: the envelope in its wire
// form, the time left before the deadline, the process, and an
// environment variable.
func report(ctx context.Context, env *envelope.Envelope, res *plugin.Result) {
	if b, err := env.MarshalJSON(); err == nil {
		res.Headers["X-Echo-Envelope"] = []string{string(b)}
	}
	if dl, ok := ctx.Deadline(); ok {
		res.Headers["X-Echo-Deadline-Ms"] = []string{strconv.FormatInt(time.Until(dl).Milliseconds(), 10)}
	}
	res.Headers["X-Echo-Pid"] = []string{strconv.Itoa(os.Getpid())}
	if name := env.Header(HeaderEnv); name != "" {
		v, ok := os.LookupEnv(name)
		res.Headers["X-Echo-Env-Value"] = []string{v}
		res.Headers["X-Echo-Env-Set"] = []string{strconv.FormatBool(ok)}
	}
}
