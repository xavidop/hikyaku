// Package cli is the hikyaku command line: run, validate, lint, init,
// schema, doctor, send, route, ai test, chaos, dev, tail, dlq, history and
// version. It lives in the root module and gets the heavy pieces (the AI
// router, the Redis store, fault injection) from cmd/hikyaku through Deps.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/xavidop/mamori"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/redact"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
)

// BuildInfo is stamped by the release build.
type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

// Streams are the process's standard streams.
type Streams struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

// AIOptions configure the AI factory.
type AIOptions struct {
	// Credentials returns a declared secret by name.
	Credentials func(name string) (string, bool)
	Logger      *slog.Logger
	Metrics     *metrics.Metrics
}

// Deps are what cmd/hikyaku wires in.
type Deps struct {
	LookupEnv func(name string) (string, bool)
	Now       func() time.Time
	// NewAI builds the AI router factory (the llm module). Nil leaves AI
	// routes unavailable.
	NewAI func(ctx context.Context, spec *config.Spec, o AIOptions) (router.AIFactory, error)
	// StoreFactories open stores by type ("redis"); memory is built in.
	StoreFactories map[string]engine.StoreFactory
	// WrapFault puts injected faults in front of a sink (hikyaku chaos).
	WrapFault func(s plugin.Sink, f config.Fault) (plugin.Sink, error)
	// GenkitVersion reports the linked Genkit version.
	GenkitVersion func() string
	// Providers are extra mamori secret providers (vault, aws, gcp, ...).
	Providers []mamori.Provider
	// Plugins starts a spec's out-of-process plugins (plugins:, the
	// grpcplugin module). Nil leaves plugins: unsupported in this build.
	Plugins func(ctx context.Context, bins []config.PluginBinary, baseDir string, log *slog.Logger) (PluginSet, error)
}

// PluginSet is a spec's out-of-process plugins, running: what grpcplugin's
// LoadAll returns.
type PluginSet interface {
	// Register makes the plugins' kinds source and destination types.
	Register()
	// Extensions describes those types for validate and schema.
	Extensions() config.Extensions
	// Close stops the plugin processes.
	Close() error
}

type app struct {
	info     BuildInfo
	streams  Streams
	deps     Deps
	redactor *redact.Redactor

	files     []string
	vars      []string
	envFile   string
	colorMode string
	verbose   bool

	// wrapSink decorates destination sinks of the running relay (chaos).
	wrapSink func(dest string, s plugin.Sink) plugin.Sink

	// plugins are the out-of-process plugin sets started by load; the
	// newest serves the current spec. Older ones close once nothing uses
	// them (after a reload), and every one when the command ends.
	pluginsMu   sync.Mutex
	plugins     []PluginSet
	lastPlugins PluginSet
}

func (a *app) style(w io.Writer) Style {
	return NewStyle(ColorEnabled(a.colorMode, w, a.deps.LookupEnv))
}

var apps sync.Map

// NewRoot builds the command tree.
func NewRoot(info BuildInfo, streams Streams, deps Deps) *cobra.Command {
	if streams.Out == nil {
		streams.Out = io.Discard
	}
	if streams.Err == nil {
		streams.Err = io.Discard
	}
	if deps.LookupEnv == nil {
		deps.LookupEnv = func(string) (string, bool) { return "", false }
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if info.Version == "" {
		info.Version = "dev"
	}
	a := &app{info: info, streams: streams, deps: deps, redactor: redact.New()}
	root := &cobra.Command{
		Use:   "hikyaku",
		Short: "Read from endpoints, deliver to endpoints, route with AI.",
		Long: "hikyaku: Read from endpoints, deliver to endpoints, route with AI.\n\n" +
			"hikyaku (飛脚, the Edo-period relay couriers) reads from one or more endpoints and\n" +
			"delivers to one or more endpoints over HTTP, WebSockets, SSE, files and more,\n" +
			"configured by a YAML spec. Its AI router picks destinations with System One decision\n" +
			"models on Genkit Go, and retries and fallbacks are built in at every layer.",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) > 0 {
				return usageError(fmt.Errorf("unknown command %q for \"hikyaku\"", args[0]))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	if streams.In != nil {
		root.SetIn(streams.In)
	}
	root.SetOut(streams.Out)
	root.SetErr(streams.Err)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageError(err) })
	registerHelpFuncs()
	root.SetUsageTemplate(usageTemplate)
	root.SetHelpTemplate(helpTemplate)
	pf := root.PersistentFlags()
	pf.StringArrayVarP(&a.files, "file", "f", []string{"hikyaku.yaml"}, "spec file or directory (repeatable; files are merged)")
	pf.StringArrayVar(&a.vars, "var", nil, "KEY=VAL for ${VAR} in secret refs (repeatable)")
	pf.StringVar(&a.envFile, "env-file", "", "dotenv file read when an env: secret or {{env.X}} is not set")
	pf.StringVar(&a.colorMode, "color", "auto", "colour the output: auto (a terminal, unless NO_COLOR), always or never")
	pf.BoolVarP(&a.verbose, "verbose", "v", false, "debug logging to stderr")

	root.AddCommand(
		a.newRunCmd(),
		a.newDevCmd(),
		a.newValidateCmd(),
		a.newLintCmd(),
		a.newInitCmd(),
		a.newSchemaCmd(),
		a.newDoctorCmd(),
		a.newSendCmd(),
		a.newRouteCmd(),
		a.newAICmd(),
		a.newChaosCmd(),
		a.newTailCmd(),
		a.newDLQCmd(),
		a.newHistoryCmd(),
		newVersionCmd(a),
	)
	apps.Store(root, a)
	return root
}

// logger is the CLI's logger: stderr, through the secret redactor, at the
// spec's level (debug with -v), text on a terminal and json otherwise unless
// the spec says.
func (a *app) logger(spec *config.Spec) *slog.Logger {
	level := slog.LevelInfo
	format := ""
	if spec != nil {
		switch spec.Observability.Logs.Level {
		case "debug":
			level = slog.LevelDebug
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
		format = spec.Observability.Logs.Format
	}
	if a.verbose {
		level = slog.LevelDebug
	}
	if format == "" {
		format = "json"
		if ColorEnabled("auto", a.streams.Err, a.deps.LookupEnv) {
			format = "text"
		}
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if format == "text" {
		h = slog.NewTextHandler(a.streams.Err, opts)
	} else {
		h = slog.NewJSONHandler(a.streams.Err, opts)
	}
	return slog.New(a.redactor.Handler(h))
}

// quietLogger logs errors only (everything with -v): one-shot commands
// print their own report, and expected detours such as a model fallback
// are in it.
func (a *app) quietLogger() *slog.Logger {
	level := slog.LevelError
	if a.verbose {
		level = slog.LevelDebug
	}
	return slog.New(a.redactor.Handler(slog.NewTextHandler(a.streams.Err, &slog.HandlerOptions{Level: level})))
}

// Execute runs the command tree and returns the process exit code.
func Execute(ctx context.Context, root *cobra.Command, args []string) int {
	if args == nil {
		args = []string{} // nil would make cobra fall back to os.Args
	}
	root.SetArgs(args)
	err := root.ExecuteContext(ctx)
	if v, ok := apps.Load(root); ok {
		v.(*app).closePluginsExcept(nil)
	}
	if err == nil {
		return ExitOK
	}
	var ee *exitError
	if errors.As(err, &ee) && ee.silent {
		return ee.code
	}
	msg := err.Error()
	label := "error:"
	if v, ok := apps.Load(root); ok {
		a := v.(*app)
		msg = a.redactor.Redact(msg)
		label = a.style(root.ErrOrStderr()).Fail(label)
	}
	_, _ = fmt.Fprintf(root.ErrOrStderr(), "%s %s\n", label, msg)
	return ExitCode(err)
}

// stdin is the input stream (for --data @-).
func (a *app) stdin() io.Reader {
	if a.streams.In != nil {
		return a.streams.In
	}
	return os.Stdin
}
