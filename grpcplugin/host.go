package grpcplugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	goplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// Timeouts of the host's own calls.
const (
	startTimeout    = 30 * time.Second
	describeTimeout = 10 * time.Second
	settleTimeout   = 30 * time.Second
	closeTimeout    = 10 * time.Second
)

// Relaunch backoff: the first relaunch after a crash is immediate; while
// relaunches keep failing (or the relaunched process keeps dying before a
// call succeeds) the wait doubles from relaunchBase up to relaunchMax.
const (
	relaunchBase = 250 * time.Millisecond
	relaunchMax  = 30 * time.Second
)

// minimalEnv is what a plugin inherits from hikyaku's environment; the
// rest of its environment is the spec's env: block. Secrets in hikyaku's
// environment never reach a plugin unless the spec passes them.
var minimalEnv = []string{
	"PATH", "HOME", "USER", "LOGNAME", "TMPDIR", "TZ",
	"LANG", "LC_ALL", "LC_CTYPE", "LC_MESSAGES",
	"SSL_CERT_FILE", "SSL_CERT_DIR",
	// Windows
	"SystemRoot", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "USERPROFILE",
	"APPDATA", "LOCALAPPDATA", "ComSpec", "PATHEXT",
}

// process is one running plugin process.
type process interface {
	client() pluginpb.PluginClient
	exited() bool
	kill()
	pid() int
}

// goProcess is a process go-plugin started.
type goProcess struct {
	c   *goplugin.Client
	rpc pluginpb.PluginClient
}

func (g *goProcess) client() pluginpb.PluginClient { return g.rpc }
func (g *goProcess) exited() bool                  { return g.c.Exited() }
func (g *goProcess) kill()                         { g.c.Kill() }

func (g *goProcess) pid() int {
	n, _ := strconv.Atoi(g.c.ID())
	return n
}

// Plugin is a launched plugin binary: its process, relaunched when it
// dies, and the kinds it serves.
type Plugin struct {
	name   string
	log    *slog.Logger
	launch func(ctx context.Context) (process, error)

	kinds      []Kind
	validators map[kindKey]*optionsValidator

	// Relaunch backoff, and its clock and jitter source.
	base, maxWait time.Duration
	now           func() time.Time
	rnd           func() float64

	mu       sync.Mutex
	proc     process // nil while the plugin is down
	gen      uint64  // incremented by every launch
	restarts int     // relaunches since a call last worked
	retryAt  time.Time
	lastErr  error
	closed   bool
	remotes  map[*remote]struct{}
	killing  sync.WaitGroup
}

type kindKey struct {
	role Role
	name string
}

// Launch starts a plugin binary and asks what it serves. cfg.Command is a
// path, relative to baseDir (the spec file's directory) unless absolute,
// or a bare name looked up on PATH. When cfg.SHA256 is set, a binary that
// does not match is refused, now and on every relaunch. The process
// starts with Args, and with Env on top of a minimal environment (PATH,
// HOME, TMPDIR, the locale, ...). Its stderr logs are forwarded to log.
func Launch(ctx context.Context, cfg config.PluginBinary, baseDir string, log *slog.Logger) (*Plugin, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if cfg.Name == "" {
		return nil, errors.New("plugin: name is required")
	}
	path, err := resolveCommand(cfg.Command, baseDir)
	if err != nil {
		return nil, fmt.Errorf("plugin %s: %w", cfg.Name, err)
	}
	var sum []byte
	if cfg.SHA256 != "" {
		sum, err = hex.DecodeString(strings.TrimSpace(cfg.SHA256))
		if err != nil || len(sum) != sha256.Size {
			return nil, fmt.Errorf("plugin %s: sha256: want %d hex digits", cfg.Name, 2*sha256.Size)
		}
	}
	l := log.With("plugin", cfg.Name)
	env, err := pluginEnv(cfg.Env, enabledLevel(l))
	if err != nil {
		return nil, fmt.Errorf("plugin %s: %w", cfg.Name, err)
	}
	return newPlugin(ctx, cfg.Name, l, func(ctx context.Context) (process, error) {
		proc, err := startProcess(ctx, path, cfg.Args, env, sum, l)
		if err != nil {
			if errors.Is(err, goplugin.ErrChecksumsDoNotMatch) {
				return nil, fmt.Errorf("%s does not match its sha256 %s", path, cfg.SHA256)
			}
			return nil, fmt.Errorf("start %s: %w", path, err)
		}
		return proc, nil
	})
}

// newPlugin launches a plugin with launch and describes it.
func newPlugin(ctx context.Context, name string, log *slog.Logger, launch func(context.Context) (process, error)) (*Plugin, error) {
	p := &Plugin{
		name:    name,
		log:     log,
		launch:  launch,
		base:    relaunchBase,
		maxWait: relaunchMax,
		now:     time.Now,
		rnd:     rand.Float64,
		remotes: map[*remote]struct{}{},
	}
	proc, err := launch(ctx)
	if err != nil {
		return nil, fmt.Errorf("plugin %s: %w", name, err)
	}
	p.proc, p.gen = proc, 1
	if err := p.describe(ctx, proc.client()); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("plugin %s: %w", name, err)
	}
	log.Debug("plugin started", "pid", proc.pid(), "kinds", len(p.kinds))
	return p, nil
}

// describe asks the plugin for its kinds and compiles their schemas.
func (p *Plugin) describe(ctx context.Context, c pluginpb.PluginClient) error {
	ctx, cancel := context.WithTimeout(ctx, describeTimeout)
	defer cancel()
	resp, err := c.Describe(ctx, &pluginpb.DescribeRequest{})
	if err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	p.validators = map[kindKey]*optionsValidator{}
	for _, k := range resp.GetKinds() {
		var role Role
		switch k.GetRole() {
		case pluginpb.Role_ROLE_SOURCE:
			role = RoleSource
		case pluginpb.Role_ROLE_SINK:
			role = RoleSink
		default:
			return fmt.Errorf("describe: kind %q has no role", k.GetName())
		}
		if err := checkKindName(k.GetName()); err != nil {
			return fmt.Errorf("describe: %w", err)
		}
		key := kindKey{role, k.GetName()}
		if _, dup := p.validators[key]; dup {
			return fmt.Errorf("describe: %s type %q is declared twice", role, k.GetName())
		}
		kind := Kind{Name: k.GetName(), Role: role, Description: k.GetDescription()}
		var v *optionsValidator
		if s := strings.TrimSpace(k.GetOptionsSchemaJson()); s != "" {
			kind.Schema = json.RawMessage(s)
			url := config.SchemaBaseURL + "plugins/" + p.name + "/" + k.GetName() + ".json"
			if v, err = compileOptionsSchema(url, kind.Schema); err != nil {
				return fmt.Errorf("%s type %q: %w", role, k.GetName(), err)
			}
		}
		p.validators[key] = v
		p.kinds = append(p.kinds, kind)
	}
	if len(p.kinds) == 0 {
		return errors.New("describe: the plugin serves no kinds")
	}
	return nil
}

// checkKindName accepts the names a spec's type: can hold.
func checkKindName(name string) error {
	if name == "" {
		return errors.New("a kind has no name")
	}
	for _, r := range name {
		if !kindRune(r) {
			return fmt.Errorf("kind %q: names hold letters, digits, '-', '_' and '.'", name)
		}
	}
	return nil
}

func kindRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return r == '-' || r == '_' || r == '.'
}

// Name is the plugin's name in the spec.
func (p *Plugin) Name() string { return p.name }

// Kinds are the source and destination types the plugin serves.
func (p *Plugin) Kinds() []Kind { return slices.Clone(p.kinds) }

// pid is the running process's ID, 0 while the plugin is down.
func (p *Plugin) pid() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.proc == nil {
		return 0
	}
	return p.proc.pid()
}

// Close releases what is still open in the plugin (so it can flush) and
// stops its process: gracefully, then killed after two seconds. Calls made
// after Close fail.
func (p *Plugin) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	remotes := slices.Collect(maps.Keys(p.remotes))
	p.mu.Unlock()

	var errs []error
	if len(remotes) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		for _, r := range remotes {
			if err := r.shut(ctx); err != nil {
				errs = append(errs, fmt.Errorf("%s %s: %w", r.role, r.name, err))
			}
		}
		cancel()
	}

	p.mu.Lock()
	proc := p.proc
	p.proc = nil
	p.mu.Unlock()
	if proc != nil {
		proc.kill()
	}
	p.killing.Wait()
	p.log.Debug("plugin stopped")
	return errors.Join(errs...)
}

// conn returns the client of the running process and its generation. When
// the process died, it relaunches it first: at once after a crash, with
// backoff while relaunching fails.
func (p *Plugin) conn(ctx context.Context) (pluginpb.PluginClient, uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, 0, &plugin.Error{Class: plugin.Transient, Tags: []string{plugin.TagConnect}, Err: fmt.Errorf("plugin %s is closed", p.name)}
	}
	if p.proc != nil {
		if !p.proc.exited() {
			return p.proc.client(), p.gen, nil
		}
		p.log.Warn("plugin process exited")
		p.lastErr = errors.New("the plugin process exited")
		p.drop()
	}
	now := p.now()
	if now.Before(p.retryAt) {
		wait := p.retryAt.Sub(now)
		return nil, 0, &plugin.Error{Class: plugin.Transient, RetryAfter: wait, Tags: []string{plugin.TagConnect},
			Err: fmt.Errorf("plugin %s is down, relaunching in %s: %v", p.name, wait.Round(time.Millisecond), p.lastErr)}
	}
	if p.restarts > 0 {
		p.log.Info("relaunching plugin", "attempt", p.restarts+1)
	}
	proc, err := p.launch(ctx)
	p.restarts++
	p.retryAt = p.now().Add(backoff.Delay(p.base, p.maxWait, "equal", p.restarts, p.rnd))
	if err != nil {
		p.lastErr = err
		p.log.Error("plugin relaunch failed", "error", err)
		return nil, 0, &plugin.Error{Class: plugin.Transient, Tags: []string{plugin.TagConnect}, Err: fmt.Errorf("plugin %s: relaunch: %w", p.name, err)}
	}
	p.proc = proc
	p.gen++
	p.log.Info("plugin relaunched", "pid", proc.pid())
	return proc.client(), p.gen, nil
}

// current is the running process's client and generation, without
// relaunching anything.
func (p *Plugin) current() (pluginpb.PluginClient, uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.proc == nil || p.proc.exited() {
		return nil, 0, false
	}
	return p.proc.client(), p.gen, true
}

// healthy records that a call to generation gen worked: the next crash is
// relaunched at once again.
func (p *Plugin) healthy(gen uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if gen == p.gen && p.restarts > 0 {
		p.restarts = 0
		p.retryAt = time.Time{}
	}
}

// lost records that generation gen's connection broke: the next call
// relaunches the process.
func (p *Plugin) lost(gen uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if gen != p.gen || p.proc == nil {
		return
	}
	p.log.Warn("plugin connection lost", "error", err)
	p.lastErr = err
	p.drop()
}

// drop forgets the current process and stops it in the background (it is
// usually dead already). p.mu is held.
func (p *Plugin) drop() {
	proc := p.proc
	p.proc = nil
	p.killing.Add(1)
	go func() {
		defer p.killing.Done()
		proc.kill()
	}()
}

// exitedNow reports whether generation gen's process is known to be gone.
func (p *Plugin) exitedNow(gen uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return gen == p.gen && (p.proc == nil || p.proc.exited())
}

// callError classifies a failed call: a lost connection is transient with
// the tag connect (and the next call relaunches the plugin), the host's
// deadline is a timeout, a message the channel cannot carry is permanent.
func (p *Plugin) callError(ctx context.Context, gen uint64, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return plugin.Wrap(plugin.Transient, nil, fmt.Errorf("plugin %s: %w", p.name, err))
	}
	msg := fmt.Errorf("plugin %s: %s", p.name, st.Message())
	switch st.Code() {
	case codes.DeadlineExceeded:
		return &plugin.Error{Class: plugin.Timeout, Tags: []string{plugin.TagTimeout}, Err: msg}
	case codes.Canceled:
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return &plugin.Error{Class: plugin.Timeout, Tags: []string{plugin.TagTimeout}, Err: msg}
		}
		return plugin.Wrap(plugin.Transient, nil, msg)
	case codes.Unavailable:
		p.lost(gen, err)
		return &plugin.Error{Class: plugin.Transient, Tags: []string{plugin.TagConnect}, Err: fmt.Errorf("plugin %s: connection lost: %s", p.name, st.Message())}
	case codes.ResourceExhausted, codes.Unimplemented, codes.InvalidArgument:
		return plugin.Wrap(plugin.Permanent, nil, msg)
	}
	if p.exitedNow(gen) {
		p.lost(gen, err)
		return &plugin.Error{Class: plugin.Transient, Tags: []string{plugin.TagConnect}, Err: fmt.Errorf("plugin %s: the process exited: %s", p.name, st.Message())}
	}
	return plugin.Wrap(plugin.Transient, nil, msg)
}

func (p *Plugin) track(r *remote) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.remotes[r] = struct{}{}
	return true
}

func (p *Plugin) forget(r *remote) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.remotes, r)
}

// resolveCommand finds the binary: a path (relative to baseDir) or a name
// on PATH.
func resolveCommand(command, baseDir string) (string, error) {
	if command == "" {
		return "", errors.New("command is required")
	}
	path := command
	if strings.ContainsRune(command, '/') || strings.ContainsRune(command, filepath.Separator) {
		if !filepath.IsAbs(path) {
			path = filepath.Join(baseDir, path)
		}
	} else {
		found, err := exec.LookPath(command)
		if err != nil {
			return "", fmt.Errorf("command %q is not on PATH (write ./%s for a binary next to the spec)", command, command)
		}
		path = found
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return "", fmt.Errorf("command %s: %w", abs, err)
	}
	if fi.IsDir() {
		return "", fmt.Errorf("command %s is a directory", abs)
	}
	if runtime.GOOS != "windows" && fi.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("command %s is not executable", abs)
	}
	return abs, nil
}

// pluginEnv is a plugin's environment: the minimal set inherited from
// hikyaku, the host's log level, then the spec's env: block.
func pluginEnv(extra map[string]string, level slog.Level) ([]string, error) {
	var env []string
	for _, k := range minimalEnv {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	env = append(env, logLevelEnv+"="+levelName(level))
	for _, k := range slices.Sorted(maps.Keys(extra)) {
		if k == "" || strings.ContainsAny(k, "=\x00") {
			return nil, fmt.Errorf("env: invalid variable name %q", k)
		}
		if strings.ContainsRune(extra[k], 0) {
			return nil, fmt.Errorf("env: %s holds a NUL byte", k)
		}
		env = append(env, k+"="+extra[k])
	}
	return env, nil
}

// startProcess starts the binary through go-plugin and dispenses the
// protocol client. ctx bounds the start; the start itself times out after
// startTimeout.
func startProcess(ctx context.Context, path string, args, env []string, sum []byte, log *slog.Logger) (process, error) {
	cmd := exec.Command(path, args...)
	cmd.Env = env
	cfg := &goplugin.ClientConfig{
		HandshakeConfig:  Handshake,
		Plugins:          pluginSet(nil),
		Cmd:              cmd,
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
		AutoMTLS:         true,
		SkipHostEnv:      true,
		Logger:           newHCLogAdapter(log),
		SyncStdout:       stdioWriter(log, "stdout"),
		SyncStderr:       stdioWriter(log, "stderr"),
		StartTimeout:     startTimeout,
		GRPCDialOptions: []grpc.DialOption{grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes))},
	}
	if sum != nil {
		// A fresh hash per launch: SecureConfig never resets it.
		cfg.SecureConfig = &goplugin.SecureConfig{Checksum: sum, Hash: sha256.New()}
	}
	c := goplugin.NewClient(cfg)
	type started struct {
		rpc pluginpb.PluginClient
		err error
	}
	done := make(chan started, 1)
	go func() {
		cp, err := c.Client()
		if err != nil {
			done <- started{err: err}
			return
		}
		raw, err := cp.Dispense(pluginName)
		if err != nil {
			done <- started{err: err}
			return
		}
		rpc, ok := raw.(pluginpb.PluginClient)
		if !ok {
			done <- started{err: fmt.Errorf("unexpected client %T", raw)}
			return
		}
		done <- started{rpc: rpc}
	}()
	select {
	case s := <-done:
		if s.err != nil {
			c.Kill()
			return nil, s.err
		}
		return &goProcess{c: c, rpc: s.rpc}, nil
	case <-ctx.Done():
		// Kill waits for the start to finish, so do it in the background.
		go func() {
			<-done
			c.Kill()
		}()
		return nil, ctx.Err()
	}
}
