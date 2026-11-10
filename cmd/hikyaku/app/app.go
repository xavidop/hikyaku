// Package app is the hikyaku binary as a library: Main runs the CLI with
// every built-in protocol, the AI router, the Redis store and fault
// injection wired in.
//
// A custom build adds its own destinations (or sources) by registering
// them before Main runs; the engine gives a registered sink the same
// retries, breakers, budgets, groups, fallbacks and DLQ as a built-in one:
//
//	func main() {
//		plugin.RegisterSink("chat", chat.New, plugin.WithOptions(chat.Options{}))
//		os.Exit(app.Main(os.Args[1:]))
//	}
//
// Its settings live in the destination's options: block; the plugin reads
// them with spec.Decode, and hikyaku validate and hikyaku schema learn them
// from WithOptions.
package app

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/cli"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/protocols/fault"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
	redisstore "github.com/xavidop/hikyaku/stores/redis"
)

// The built-in protocols register when the package loads, so a custom
// build's main can replace one by registering the same kind before Main.
func init() { registerBuiltins() }

// Option configures Main.
type Option func(*options)

type options struct {
	version, commit, date string
	in                    io.Reader
	out, err              io.Writer
	lookupEnv             func(string) (string, bool)
	ctx                   context.Context
}

// WithBuild stamps the version information (main.go passes the values
// the release build sets).
func WithBuild(version, commit, date string) Option {
	return func(o *options) { o.version, o.commit, o.date = version, commit, date }
}

// WithStreams replaces the standard streams (tests).
func WithStreams(in io.Reader, out, err io.Writer) Option {
	return func(o *options) { o.in, o.out, o.err = in, out, err }
}

// WithEnv replaces the environment lookup of {{env.X}} references and the
// CLI's own variables (tests). Secrets declared as env:NAME are resolved
// by mamori from the process environment.
func WithEnv(lookup func(string) (string, bool)) Option {
	return func(o *options) { o.lookupEnv = lookup }
}

// WithContext runs the CLI under ctx: cancelling it stops a running relay
// like SIGTERM does (tests, and programs that embed the CLI).
func WithContext(ctx context.Context) Option {
	return func(o *options) { o.ctx = ctx }
}

// Main runs the CLI with args (without the program name) and returns the
// process exit code: 0 ok, 1 a failed check, 2 a config or usage error,
// 3 an infrastructure error.
func Main(args []string, opts ...Option) int {
	o := options{in: os.Stdin, out: os.Stdout, err: os.Stderr, lookupEnv: os.LookupEnv, ctx: context.Background()}
	for _, opt := range opts {
		opt(&o)
	}
	// Libraries log through the default logger (Genkit's init line, for
	// one): warnings and up, on stderr. hikyaku's own logs have their own.
	slog.SetDefault(slog.New(slog.NewTextHandler(o.err, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, stop := signal.NotifyContext(o.ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	root := cli.NewRoot(
		cli.BuildInfo{Version: o.version, Commit: o.commit, Date: o.date},
		cli.Streams{In: o.in, Out: o.out, Err: o.err},
		Deps(o.lookupEnv),
	)
	return cli.Execute(ctx, root, args)
}

// Deps is the CLI's wiring of the heavy modules.
func Deps(lookupEnv func(string) (string, bool)) cli.Deps {
	return cli.Deps{
		LookupEnv: lookupEnv,
		NewAI: func(ctx context.Context, spec *config.Spec, o cli.AIOptions) (router.AIFactory, error) {
			return llm.New(ctx, spec, llm.Options{Credentials: o.Credentials, Logger: o.Logger, Metrics: o.Metrics})
		},
		Plugins: pluginLoader,
		StoreFactories: map[string]engine.StoreFactory{
			"redis": func(ctx context.Context, spec config.Store, relay string, log *slog.Logger) (store.Store, error) {
				return redisstore.New(ctx, spec, relay, log)
			},
		},
		WrapFault:     func(s plugin.Sink, f config.Fault) (plugin.Sink, error) { return fault.Wrap(s, f) },
		GenkitVersion: llm.GenkitVersion,
	}
}

// pluginLoader starts out-of-process plugins; the full build sets it
// (wiring_full.go), and hikyaku-lite leaves plugins: unsupported.
var pluginLoader func(ctx context.Context, bins []config.PluginBinary, baseDir string, log *slog.Logger) (cli.PluginSet, error)
