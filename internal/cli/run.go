package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/admin"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
)

func (a *app) newRunCmd() *cobra.Command {
	var watch bool
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the relay (local dev and the container entrypoint)",
		Long: "Run the relay: start the sources, route and deliver until interrupted.\n\n" +
			"SIGHUP, POST /reload on the admin API or --watch reload the spec: the new graph is\n" +
			"built and validated first, then swapped in. SIGINT or SIGTERM drain in-flight\n" +
			"messages for server.shutdown_grace and exit.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return a.run(cmd.Context(), watch, nil) },
	}
	cmd.Flags().BoolVar(&watch, "watch", false, "reload when a spec file changes")
	return cmd
}

func (a *app) newDevCmd() *cobra.Command {
	var watch bool
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Run with the Genkit reflection server: genkit start -- hikyaku dev",
		Long: "Run the relay in development mode: GENKIT_ENV=dev starts Genkit's reflection server,\n" +
			"so `genkit start -- hikyaku dev` shows every route/<name> flow, trace and model call\n" +
			"in the Genkit Developer UI. Reloads on spec changes like run --watch.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := os.Setenv("GENKIT_ENV", "dev"); err != nil {
				return err
			}
			return a.run(cmd.Context(), !cmd.Flags().Changed("watch") || watch, nil)
		},
	}
	cmd.Flags().BoolVar(&watch, "watch", true, "reload when a spec file changes")
	return cmd
}

// relay supervises the running engine: build, swap on reload, stop.
type relay struct {
	a      *app
	log    *slog.Logger
	m      *metrics.Metrics
	hub    *admin.Hub // shared by every admin server, so a tail follows a move
	stores map[string]store.Store

	probe modelProbe

	// reloading lets one reload run at a time (SIGHUP, --watch and POST
	// /reload); stopping refuses reloads once run shuts down.
	reloading sync.Mutex
	stopping  bool
	// retiring are the admin servers a reload replaced, shutting down.
	retiring sync.WaitGroup

	mu      sync.Mutex
	loaded  *config.Loaded
	current *engine.Engine
	cancel  context.CancelFunc
	done    chan error
	ai      router.AIFactory   // the current engine's (nil without AI)
	aiStop  context.CancelFunc // ends ai's Genkit instance
	adm     *admin.Server      // nil with server.admin.disabled
	runErr  chan error
}

// start builds and starts an engine for a spec.
func (r *relay) start(ctx context.Context, l *config.Loaded, ai router.AIFactory) (*engine.Engine, context.CancelFunc, chan error, error) {
	e, err := engine.New(ctx, l.Spec, engine.Options{
		Logger: r.log, Metrics: r.m, AI: ai, StoreFactories: r.a.deps.StoreFactories,
		Stores: r.stores, Tap: func(ev engine.Event) { r.hub.Publish(ev) }, Now: r.a.deps.Now,
		WrapSink: r.a.wrapSink,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	ectx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan error, 1)
	go func() { done <- e.Run(ectx) }()
	return e, cancel, done, nil
}

// newAI builds the AI factory of a spec (nil without AI routes or model
// destinations). Its Genkit instance lives in a context of its own: stop
// ends it, and the Genkit reflection server of the Dev UI with it, once no
// engine routes with the factory.
func (r *relay) newAI(ctx context.Context, l *config.Loaded) (router.AIFactory, context.CancelFunc, error) {
	actx, stop := context.WithCancel(context.WithoutCancel(ctx))
	ai, err := r.a.newAI(actx, l, r.m)
	if err != nil || ai == nil {
		stop()
		return nil, nil, err
	}
	return ai, stop, nil
}

func (r *relay) engine() *engine.Engine {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current
}

// adminOptions are the admin API's options for a spec.
func (r *relay) adminOptions(l *config.Loaded) admin.Options {
	srv := l.Spec.Server
	return admin.Options{
		Listen: srv.Admin.Listen, Token: srv.Admin.Token, Metrics: r.m, Logger: r.log,
		NoMetricsEndpoint: !config.Bool(l.Spec.Observability.Metrics.Prometheus, true),
		Engine:            r.engine, Reload: r.reload, Version: r.a.info.Version, Hub: r.hub,
		SpecYAML:            r.specYAML,
		Redact:              r.a.redactor.RedactBytes,
		ReadyOnDestinations: config.Bool(srv.Readiness.Destinations, true),
		ReadyOnModels:       srv.Readiness.Models,
		ModelHealth:         r.modelHealth,
	}
}

func (r *relay) specYAML() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.a.redactedSpec(r.loaded.Spec)
}

// swap is a reload in progress: the new spec, and what serves it next to
// what serves the running one. The new pieces are in use only once
// commit swaps them in; abandon drops them when the reload fails.
type swap struct {
	l *config.Loaded
	// plugins are the new spec's out-of-process plugins (prevPlugins when
	// it starts none of its own).
	plugins, prevPlugins PluginSet
	// ai is the new engine's AI factory: the running one when it serves the
	// new spec too, a new one (aiNew) when genkit:, the model providers or
	// the secrets changed, nil when the new spec has no AI.
	ai, prevAI         router.AIFactory
	aiStop, prevAIStop context.CancelFunc
	aiNew              bool
	// adm serves the admin API of the new spec: the running server when
	// server.admin.listen stays (Configure gives it the new options), a new
	// one bound to the new address (admNew), nil when disabled.
	adm, prevAdm *admin.Server
	admNew       bool
}

// reload re-reads the spec files and swaps in a new engine once it is
// built; on any error the running relay is left as it was.
//
// The swap: load and validate the spec; build an AI factory when the
// new spec needs another one (a new Genkit instance next to the running
// one); bind the admin API's new address when it moves; start the new
// engine; swap the engine, the AI factory and the admin server in; stop
// the old engine. The old admin server shuts down after answering what it
// is serving (the POST /reload that asked for this one included); the old
// AI factory and plugins go once the old engine has stopped.
func (r *relay) reload(ctx context.Context) error {
	r.reloading.Lock()
	defer r.reloading.Unlock()
	if r.stopping {
		return errors.New("the relay is shutting down")
	}
	n := &swap{prevPlugins: r.a.currentPlugins()}
	l, err := r.a.load(ctx, true)
	if err != nil {
		r.m.Reloads.WithLabelValues("invalid").Inc()
		return err
	}
	// The new spec's plugins replace the old ones once the old engine is
	// gone; a reload that fails from here on keeps the old ones.
	n.l, n.plugins = l, r.a.currentPlugins()
	r.mu.Lock()
	old := r.loaded
	n.prevAI, n.prevAIStop, n.prevAdm = r.ai, r.aiStop, r.adm
	r.mu.Unlock()
	if err := r.prepare(ctx, old, n); err != nil {
		r.abandon(n)
		r.m.Reloads.WithLabelValues(reloadOutcome(err)).Inc()
		return err
	}
	if sameDiskBuffer(old.Spec, l.Spec) {
		return r.handOver(ctx, old, n)
	}
	if old.Spec.Buffer.Type == "disk" {
		r.log.Warn("the disk buffer changed: the old one keeps its messages until a relay opens it again", "path", old.Spec.Buffer.Path)
	}
	e, cancel, done, err := r.start(ctx, l, n.ai)
	if err != nil {
		r.abandon(n)
		r.m.Reloads.WithLabelValues("failed").Inc()
		return err
	}
	// The new sources took over shared listeners (same address and path);
	// give them a moment before the old engine lets go of its own.
	time.Sleep(100 * time.Millisecond)
	prevCancel, prevDone := r.commit(n, e, cancel, done)
	prevCancel()
	go func() {
		if err := wait(prevDone); err != nil {
			r.log.Warn("the previous engine stopped with an error", "err", err)
		}
		r.retire(n)
	}()
	go r.watchDone(done)
	r.m.Reloads.WithLabelValues("ok").Inc()
	r.log.Info("spec reloaded", "files", l.Files)
	return nil
}

// handOver reloads a relay whose old and new specs share a disk buffer.
// Only one engine can hold the buffer file, so the swap is sequential: the
// old engine stops its sources, finishes its in-flight messages (within
// server.shutdown_grace) and closes the buffer, then the new one opens it
// and delivers what is left. Listen sources refuse connections meanwhile.
// If the new engine cannot be built, the old spec is started again, with
// its own AI factory and admin server, which the reload had not replaced.
func (r *relay) handOver(ctx context.Context, old *config.Loaded, n *swap) error {
	r.mu.Lock()
	prevCancel, prevDone := r.cancel, r.done
	r.done = nil // the old engine's stop is not a failure
	r.mu.Unlock()
	prevCancel()
	if err := wait(prevDone); err != nil {
		r.log.Warn("the previous engine stopped with an error", "err", err)
	}
	e, cancel, done, err := r.start(ctx, n.l, n.ai)
	if err != nil {
		r.abandon(n)
		r.m.Reloads.WithLabelValues("failed").Inc()
		e2, cancel2, done2, err2 := r.start(ctx, old, n.prevAI)
		if err2 != nil {
			select {
			case r.runErr <- fmt.Errorf("reload failed (%v), and the previous spec did not start again: %w", err, err2):
			default:
			}
			return err
		}
		r.mu.Lock()
		r.current, r.cancel, r.done = e2, cancel2, done2
		r.mu.Unlock()
		go r.watchDone(done2)
		return err
	}
	r.commit(n, e, cancel, done)
	r.retire(n)
	go r.watchDone(done)
	r.m.Reloads.WithLabelValues("ok").Inc()
	r.log.Info("spec reloaded (disk buffer handed over)", "files", n.l.Files)
	return nil
}

// prepare builds what the new spec cannot share with the running relay:
// an AI factory when the running one does not serve it (sameAI), and an
// admin server when server.admin moves, bound now, so an address that is
// taken fails the reload while the running server stays.
func (r *relay) prepare(ctx context.Context, old *config.Loaded, n *swap) error {
	n.ai, n.aiStop = n.prevAI, n.prevAIStop
	switch {
	case !needsAI(n.l.Spec):
		n.ai, n.aiStop = nil, nil
	case n.prevAI == nil || !sameAI(old, n.l):
		ai, stop, err := r.newAI(ctx, n.l)
		if err != nil {
			return fmt.Errorf("AI router: %w", err)
		}
		n.ai, n.aiStop, n.aiNew = ai, stop, true
	}
	n.adm = n.prevAdm
	switch a := n.l.Spec.Server.Admin; {
	case a.Disabled:
		n.adm = nil
	case n.prevAdm == nil || a.Listen != old.Spec.Server.Admin.Listen:
		s := admin.New(r.adminOptions(n.l))
		if err := s.Listen(); err != nil {
			return err
		}
		n.adm, n.admNew = s, true
	}
	return nil
}

// abandon drops what prepare built for a reload that failed.
func (r *relay) abandon(n *swap) {
	if n.plugins != nil && n.plugins != n.prevPlugins {
		r.a.closePluginsExcept(n.prevPlugins)
	}
	if n.aiNew {
		n.aiStop()
	}
	if n.admNew {
		_ = n.adm.Shutdown(context.Background())
	}
}

// commit swaps the new spec in: its engine, its AI factory and its admin
// server. It returns the previous engine's cancel and done.
func (r *relay) commit(n *swap, e *engine.Engine, cancel context.CancelFunc, done chan error) (context.CancelFunc, chan error) {
	r.mu.Lock()
	prevCancel, prevDone := r.cancel, r.done
	r.loaded, r.current, r.cancel, r.done = n.l, e, cancel, done
	r.ai, r.aiStop, r.adm = n.ai, n.aiStop, n.adm
	r.mu.Unlock()
	switch {
	case n.admNew:
		n.adm.Serve()
		if n.prevAdm != nil {
			r.log.Info("admin API moved", "from", n.prevAdm.Addr(), "to", n.adm.Addr())
			r.retireAdmin(n.prevAdm)
		} else {
			r.log.Info("admin API listening", "addr", n.adm.Addr())
		}
	case n.adm != nil:
		n.adm.Configure(r.adminOptions(n.l))
	case n.prevAdm != nil:
		r.log.Info("admin API stopped (server.admin.disabled)")
		r.retireAdmin(n.prevAdm)
	}
	return prevCancel, prevDone
}

// retire lets go of what only the old engine used, once it has stopped:
// the plugin sets of older specs, and the old AI factory (with its Genkit
// instance) when the new spec got another one.
func (r *relay) retire(n *swap) {
	if n.plugins != n.prevPlugins {
		r.a.closePluginsExcept(n.plugins)
	}
	if (n.aiNew || n.ai == nil) && n.prevAIStop != nil {
		n.prevAIStop()
	}
}

// retireAdmin shuts a replaced admin server down in the background: what
// it is serving gets its answer first, the POST /reload that replaced it
// included, and its tail streams end.
func (r *relay) retireAdmin(s *admin.Server) {
	r.retiring.Add(1)
	go func() {
		defer r.retiring.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			r.log.Warn("the previous admin API did not stop cleanly", "addr", s.Addr(), "err", err)
		}
	}()
}

// reloadOutcome is the hikyaku_reloads_total outcome of a reload that
// failed before its engine started: needs_restart when only a new process
// can load the change (see llm's reflection port check), failed otherwise.
func reloadOutcome(err error) string {
	var nr interface{ NeedsRestart() bool }
	if errors.As(err, &nr) && nr.NeedsRestart() {
		return "needs_restart"
	}
	return "failed"
}

// sameDiskBuffer reports whether two specs use the same disk buffer file.
func sameDiskBuffer(a, b *config.Spec) bool {
	return a.Buffer.Type == "disk" && b.Buffer.Type == "disk" &&
		filepath.Clean(a.Buffer.Path) == filepath.Clean(b.Buffer.Path)
}

// wait waits for an engine to stop. done is nil when a hand-over failed
// and the previous spec did not start again: there is nothing to wait for.
func wait(done chan error) error {
	if done == nil {
		return nil
	}
	return <-done
}

// watchDone reports a running engine that stopped by itself (a source
// failed, or every source ran out).
func (r *relay) watchDone(done chan error) {
	err, ok := <-done
	if ok {
		done <- err // keep it for whoever waits next (a reload)
	}
	r.mu.Lock()
	current := r.done == done
	r.mu.Unlock()
	if !current {
		return
	}
	select {
	case r.runErr <- err:
	default:
	}
}

// sameAI reports whether the AI factory built for one spec serves another:
// the Genkit plugins come from genkit:, the model providers and their
// keys, so the same genkit: section, providers and secret values make the
// same plugins.
func sameAI(a, b *config.Loaded) bool {
	return reflect.DeepEqual(a.Spec.Genkit, b.Spec.Genkit) &&
		maps.Equal(providers(a.Spec), providers(b.Spec)) &&
		maps.Equal(a.SecretValues, b.SecretValues)
}

// providers are the model providers a spec names: the prefixes of models:,
// of the chain of every AI router (fallback routers too) and of the model
// destinations.
func providers(spec *config.Spec) map[string]bool {
	out := map[string]bool{}
	add := func(models ...string) {
		for _, m := range models {
			if m != "" {
				p, _, _ := strings.Cut(m, "/")
				out[p] = true
			}
		}
	}
	add(spec.Models.Default, spec.Models.Decision)
	add(spec.Models.Fallback.Default...)
	add(spec.Models.Fallback.Decision...)
	add(modelDestinations(spec)...)
	var walk func(rs *config.RouterSpec)
	walk = func(rs *config.RouterSpec) {
		if rs == nil {
			return
		}
		if rs.Type == "ai" {
			add(spec.ModelChain(*rs)...)
		}
		for i := range rs.Fallback {
			walk(&rs.Fallback[i])
		}
	}
	for _, r := range spec.Routes {
		walk(r.Router)
	}
	return out
}

// run is `hikyaku run`: ready is called once the relay serves (tests).
func (a *app) run(ctx context.Context, watch bool, ready func(adminAddr string)) error {
	l, err := a.load(ctx, true)
	if err != nil {
		return err
	}
	log := a.logger(l.Spec)
	m := metrics.New()
	obs := l.Spec.Observability
	shutdownTracing, err := metrics.SetupTracing(ctx, metrics.TracingOptions{
		Endpoint: obs.OTel.Endpoint, Insecure: obs.OTel.Insecure, Headers: obs.OTel.Headers,
		ServiceName: obs.OTel.ServiceName, SampleRatio: ptrOr(obs.OTel.SampleRatio, 1), Version: a.info.Version,
	})
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracing(context.Background()) }()
	r := &relay{a: a, log: log, m: m, hub: admin.NewHub(), stores: map[string]store.Store{}, loaded: l, runErr: make(chan error, 1)}
	ai, aiStop, err := r.newAI(ctx, l)
	if err != nil {
		return fmt.Errorf("AI router: %w", err)
	}
	r.ai, r.aiStop = ai, aiStop
	defer func() {
		r.mu.Lock()
		stop := r.aiStop
		r.mu.Unlock()
		if stop != nil {
			stop()
		}
	}()
	e, cancel, done, err := r.start(ctx, l, ai)
	if err != nil {
		return err
	}
	// Keep the stores across reloads: a reload must not reconnect Redis.
	for k, v := range e.Stores() {
		if k != "" {
			r.stores[k] = v
		}
	}
	r.stores[""] = e.Stores()[""]
	r.current, r.cancel, r.done = e, cancel, done
	go r.watchDone(done)

	srv := l.Spec.Server
	addr := ""
	if !srv.Admin.Disabled {
		adm := admin.New(r.adminOptions(l))
		if err := adm.Listen(); err != nil {
			cancel()
			if derr := <-done; derr != nil {
				log.Warn("relay stopped with an error", "err", derr)
			}
			return err
		}
		// Known to the relay before it answers, so a POST /reload sees it.
		r.mu.Lock()
		r.adm = adm
		r.mu.Unlock()
		adm.Serve()
		addr = adm.Addr()
		log.Info("admin API listening", "addr", addr)
	}
	if ready != nil {
		ready(addr)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigs)
	var tick <-chan time.Time
	stamps := map[string]time.Time{}
	if watch {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		tick = t.C
		stamps = mtimes(l.Files)
	}
	var runErr error
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case s := <-sigs:
			if s == syscall.SIGHUP {
				if err := r.reload(ctx); err != nil {
					log.Error("reload failed; still running the previous spec", "err", err)
				}
				continue
			}
			log.Info("shutting down", "signal", s.String(), "grace", srv.ShutdownGrace.String())
			break loop
		case <-tick:
			now := mtimes(l.Files)
			if !reflect.DeepEqual(now, stamps) {
				stamps = now
				if err := r.reload(ctx); err != nil {
					log.Error("reload failed; still running the previous spec", "err", err)
				}
			}
		case err := <-r.runErr:
			runErr = err
			break loop
		}
	}
	// A reload in flight (POST /reload) finishes first; later ones are
	// refused.
	r.reloading.Lock()
	r.stopping = true
	r.reloading.Unlock()
	r.mu.Lock()
	cancel, done, adm := r.cancel, r.done, r.adm
	r.mu.Unlock()
	cancel()
	if err := wait(done); err != nil && runErr == nil {
		runErr = err
	}
	if adm != nil {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		_ = adm.Shutdown(sctx)
	}
	r.retiring.Wait()
	for name, st := range r.stores {
		if name != "" {
			_ = st.Close()
		}
	}
	return runErr
}

func mtimes(files []string) map[string]time.Time {
	out := map[string]time.Time{}
	for _, f := range files {
		if fi, err := os.Stat(f); err == nil {
			out[f] = fi.ModTime()
		}
	}
	return out
}

func ptrOr(p *float64, def float64) float64 {
	if p == nil {
		return def
	}
	return *p
}

// modelProbeTTL is how long a readiness.models probe is fresh: a probe
// calls every model, so a kubelet asking every few seconds would spend.
const modelProbeTTL = 30 * time.Second

// modelProbe is the last readiness.models probe.
type modelProbe struct {
	mu      sync.Mutex
	at      time.Time      // zero until the first probe
	of      *engine.Engine // the engine whose spec was probed
	err     error
	running bool // a background refresh is in flight
}

// modelHealth backs readiness.models: the relay is not ready while every
// model of an AI route's chain fails (one that answers routes the
// messages). The first call waits for the first probe (so do concurrent
// ones), and so does the first call after a reload. After that /readyz
// answers at once from the last probe, and a probe older than
// modelProbeTTL is refreshed in the background, so a slow model never
// makes a kubelet probe time out.
func (r *relay) modelHealth(ctx context.Context) error {
	r.mu.Lock()
	e, ai := r.current, r.ai
	r.mu.Unlock()
	if ai == nil {
		return nil
	}
	r.probe.mu.Lock()
	defer r.probe.mu.Unlock()
	if r.probe.at.IsZero() || r.probe.of != e {
		r.probe.err = r.probeModels(ctx, e, ai)
		r.probe.at, r.probe.of = r.a.deps.Now(), e
		return r.probe.err
	}
	if r.a.deps.Now().Sub(r.probe.at) >= modelProbeTTL && !r.probe.running {
		r.probe.running = true
		go func() {
			err := r.probeModels(context.WithoutCancel(ctx), e, ai)
			r.probe.mu.Lock()
			defer r.probe.mu.Unlock()
			r.probe.running = false
			if r.probe.of == e { // not a probe of the spec a reload replaced
				r.probe.err, r.probe.at = err, r.a.deps.Now()
			}
		}()
	}
	return r.probe.err
}

// probeModels calls the models of every AI route's chain of an engine's
// spec, with the engine's AI factory, and reports the routes whose every
// model failed.
func (r *relay) probeModels(ctx context.Context, e *engine.Engine, ai router.AIFactory) error {
	// A caller that hangs up must not leave a canceled probe behind.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	spec := e.Spec()
	chains := map[string][]string{}
	var all []string
	for _, rt := range spec.Routes {
		if rs, ok := rt.AIRouter(); ok {
			chains[rt.Name] = spec.ModelChain(*rs)
			all = append(all, chains[rt.Name]...)
		}
	}
	res := ai.Health(pctx, uniq(all))
	var down []string
	for _, name := range sortedKeys(chains) {
		var errs []string
		for _, m := range chains[name] {
			if res[m] == nil {
				errs = nil
				break
			}
			errs = append(errs, m+": "+res[m].Error())
		}
		if len(errs) > 0 {
			down = append(down, "route "+name+": every model failed ("+strings.Join(errs, "; ")+")")
		}
	}
	if len(down) > 0 {
		return errors.New(r.a.redactor.Redact(strings.Join(down, "; ")))
	}
	return nil
}

func modelsOf(spec *config.Spec) []string {
	var out []string
	for _, r := range spec.Routes {
		if rs, ok := r.AIRouter(); ok {
			out = append(out, spec.ModelChain(*rs)...)
		}
	}
	return out
}

// modelDestinations lists the models of the model destinations (the LLM
// proxy), group members included.
func modelDestinations(spec *config.Spec) []string {
	var out []string
	for _, d := range spec.Destinations {
		if d.Type == "model" && d.Model != "" {
			out = append(out, d.Model)
		}
		for _, m := range d.Members {
			if m.Type == "model" && m.Model != "" {
				out = append(out, m.Model)
			}
		}
	}
	return out
}
