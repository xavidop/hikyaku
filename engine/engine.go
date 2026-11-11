// Package engine runs a relay: it builds the graph of a spec (sources,
// routers, destinations with their groups and fallbacks, the DLQ), moves
// messages from sources through routes to destinations with the retry,
// breaker, budget and fallback policies of design §7, and acks each message
// once every required destination has it or it is safely dead-lettered.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// StoreFactory builds a store of one type (the Redis module provides
// "redis"; "memory" is built in).
type StoreFactory func(ctx context.Context, spec config.Store, relay string, logger *slog.Logger) (store.Store, error)

// Event is what the engine reports to a tap (the admin API's tail).
type Event struct {
	Type     string // received | done
	Envelope *envelope.Envelope
	Report   *Report // set on done
}

// Options configure an engine.
type Options struct {
	Logger  *slog.Logger
	Metrics *metrics.Metrics
	// AI builds AI routers; nil makes ai routes fail to build.
	AI router.AIFactory
	// StoreFactories by store type; memory is always available.
	StoreFactories map[string]StoreFactory
	// Stores are already-open stores by name, reused instead of opened
	// (hot reload keeps its stores).
	Stores map[string]store.Store
	// Routers replace the router of a route by name (library embedding
	// with compile-time routers).
	Routers map[string]plugin.Router
	// WrapSink decorates the sink of a destination or member (hikyaku chaos).
	WrapSink func(destination string, s plugin.Sink) plugin.Sink
	// NoSources builds the engine without its sources: for send, route and
	// dlq replay, which inject messages themselves.
	NoSources bool
	// Tap receives an event per received and finished message.
	Tap func(Event)
	// Now and Rand make time and jitter deterministic in tests.
	Now  func() time.Time
	Rand func() float64
}

// Engine is a built relay.
type Engine struct {
	spec *config.Spec
	opts Options
	log  *slog.Logger
	m    *metrics.Metrics
	now  func() time.Time
	rand func() float64

	sources  []plugin.Source
	routes   []*route
	bySource map[string][]*route
	dests    map[string]*destination
	order    []string // destination names in spec order
	dlq      *deadLetter
	stores   map[string]store.Store
	ownStore map[string]bool
	defStore store.Store
	recorder *store.Recorder

	buffer      chan *envelope.Envelope
	disk        *diskBuffer
	orderer     *orderer
	dedupers    map[string]*deduper // source name → its dedupe (the engine's)
	batchers    map[string]*batcher
	sem         chan struct{}
	stopping    chan struct{} // closed when Run shuts down: held messages stop waiting
	inflight    sync.WaitGroup
	probeClient *http.Client

	closeOnce sync.Once
}

// route is a built route.
type route struct {
	name       string
	from       []string
	filter     *transform.Expr
	transforms transform.Chain
	router     plugin.Router
}

// New builds an engine. It opens stores and builds sinks, routers and
// sources, but starts nothing: Run does.
func New(ctx context.Context, spec *config.Spec, o Options) (*Engine, error) {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Rand == nil {
		o.Rand = defaultRand
	}
	e := &Engine{
		spec:        spec,
		opts:        o,
		log:         o.Logger,
		m:           o.Metrics,
		now:         o.Now,
		rand:        o.Rand,
		bySource:    map[string][]*route{},
		dests:       map[string]*destination{},
		stores:      map[string]store.Store{},
		ownStore:    map[string]bool{},
		buffer:      make(chan *envelope.Envelope, intakeSize(spec.Buffer)),
		batchers:    map[string]*batcher{},
		sem:         make(chan struct{}, max(spec.Server.Workers, 1)),
		stopping:    make(chan struct{}),
		probeClient: &http.Client{Timeout: 5 * time.Second},
	}
	if err := e.buildStores(ctx); err != nil {
		e.closeStores()
		return nil, err
	}
	e.recorder = store.NewRecorder(store.RecorderOptions{OnError: func(op string, err error) {
		if e.m != nil {
			e.m.HistoryErrors.WithLabelValues(op).Inc()
		}
		e.log.Warn("history write failed", "op", op, "err", err)
	}})
	// Started now, not in Run: messages injected without running (send,
	// route, dlq replay) record history too.
	e.recorder.Start()
	steps := []func(context.Context) error{e.buildDestinations, e.buildDLQ, e.buildRoutes}
	if !o.NoSources {
		steps = append(steps, e.buildSources)
	}
	for _, step := range steps {
		if err := step(ctx); err != nil {
			e.Close(ctx)
			return nil, err
		}
	}
	ord, err := newOrderer(e)
	if err != nil {
		e.Close(ctx)
		return nil, err
	}
	e.orderer = ord
	if err := e.buildDedupe(); err != nil {
		e.Close(ctx)
		return nil, err
	}
	return e, nil
}

// intakeSize is the capacity of the channel sources send on: the whole
// memory buffer, or the disk buffer's small staging area (a message there
// is not queued yet: its source is not acked until the log has it).
func intakeSize(b config.Buffer) int {
	if b.Type == "disk" {
		return min(max(b.Size, 1), diskIntake)
	}
	return max(b.Size, 1)
}

// Spec is the spec the engine was built from.
func (e *Engine) Spec() *config.Spec { return e.spec }

func (e *Engine) buildStores(ctx context.Context) error {
	for _, s := range e.spec.Stores {
		if st, ok := e.opts.Stores[s.Name]; ok {
			e.stores[s.Name] = st
			continue
		}
		var st store.Store
		switch s.Type {
		case "memory":
			st = store.NewMemory(s.Name)
		default:
			f, ok := e.opts.StoreFactories[s.Type]
			if !ok {
				return fmt.Errorf("store %s: type %q is not available in this build", s.Name, s.Type)
			}
			var err error
			if st, err = f(ctx, s, e.spec.Name, e.log); err != nil {
				return fmt.Errorf("store %s: %w", s.Name, err)
			}
		}
		e.stores[s.Name] = st
		e.ownStore[s.Name] = true
	}
	if len(e.spec.Stores) > 0 {
		e.defStore = e.stores[e.spec.Stores[0].Name]
	} else if st, ok := e.opts.Stores[""]; ok {
		e.defStore = st
	} else {
		e.defStore = store.NewMemory("default")
	}
	return nil
}

// Store returns a declared store by name; "" is the default store.
func (e *Engine) Store(name string) (store.Store, error) {
	if name == "" {
		return e.defStore, nil
	}
	s, ok := e.stores[name]
	if !ok {
		return nil, fmt.Errorf("store %q is not declared", name)
	}
	return s, nil
}

// Stores returns every store, the default one under "".
func (e *Engine) Stores() map[string]store.Store {
	out := map[string]store.Store{"": e.defStore}
	for k, v := range e.stores {
		out[k] = v
	}
	return out
}

func (e *Engine) pluginEnv() plugin.Env {
	buffer := e.spec.Buffer.Type
	if buffer == "" {
		buffer = "memory"
	}
	return plugin.Env{Relay: e.spec.Name, Logger: e.log, Store: e.Store, Buffer: buffer,
		Egress:  plugin.Egress{Allow: e.spec.Egress.Allow, AllowPrivate: e.spec.Egress.AllowPrivate},
		Deduped: e.countDeduped, Refused: e.countRefused}
}

func (e *Engine) buildDestinations(ctx context.Context) error {
	for i := range e.spec.Destinations {
		ds := e.spec.Destinations[i]
		p := config.ResolvePolicy(e.spec.Defaults, ds.Policy)
		d := &destination{
			name:       ds.Name,
			policy:     newRetryPolicy(*p.Retry),
			timeout:    p.Timeout.Std(),
			deadline:   p.Deadline.Std(),
			required:   ds.IsRequired(),
			idempotent: ds.Idempotent,
			idemHeader: ds.IdempotencyHeaderName(),
			fallback:   ds.Fallback,
		}
		if ds.Batch != nil {
			e.batchers[ds.Name] = newBatcher(e, ds.Name, ds.Batch)
		}
		d.limiter = newLimiter(ds.RateLimit)
		d.delay = ds.DeliverAfter.Std()
		if ds.DeliverAt != "" {
			t, err := transform.CompileTemplate(fmt.Sprintf("destination %q.deliver_at", ds.Name), ds.DeliverAt)
			if err != nil {
				return err
			}
			d.deliverAt = t
		}
		chain, err := transform.Compile(fmt.Sprintf("destination %q.transforms", ds.Name), ds.Transforms)
		if err != nil {
			return err
		}
		d.transforms = chain
		var shared store.KV
		if p.RetryBudget.Store != "" {
			if shared, err = e.Store(p.RetryBudget.Store); err != nil {
				return fmt.Errorf("destination %s: retry_budget: %w", ds.Name, err)
			}
		}
		d.budget = newRetryBudget(*p.RetryBudget, ds.Name, shared, e.now)
		if ds.Type == "group" {
			for j := range ds.Members {
				m := ds.Members[j]
				name := m.Name
				if name == "" {
					name = fmt.Sprintf("%s[%d]", ds.Name, j)
				}
				ep, err := e.newEndpoint(ctx, ds.Name, name, m.Endpoint, *p.CircuitBreaker)
				if err != nil {
					return err
				}
				ep.weight = m.Weight
				d.endpoints = append(d.endpoints, ep)
			}
			d.group = newGroup(ds, d.endpoints)
		} else {
			ep, err := e.newEndpoint(ctx, ds.Name, ds.Name, ds.Endpoint, *p.CircuitBreaker)
			if err != nil {
				return err
			}
			d.endpoints = []*endpoint{ep}
		}
		e.dests[ds.Name] = d
		e.order = append(e.order, ds.Name)
	}
	return nil
}

func (e *Engine) newEndpoint(ctx context.Context, dest, name string, ep config.Endpoint, cb config.CircuitBreaker) (*endpoint, error) {
	spec := ep
	ps := plugin.Spec{Name: name, Kind: ep.Type, Options: ep.Options, Config: &spec, Env: e.pluginEnv()}
	var sink plugin.Sink
	var err error
	if ep.Type == "model" {
		ms, ok := e.opts.AI.(router.ModelSinks)
		if !ok {
			return nil, fmt.Errorf("destination %s: model destinations need the AI module (cmd/hikyaku links the llm module)", name)
		}
		sink, err = ms.NewModelSink(ctx, ps)
	} else {
		sink, err = plugin.NewSink(ctx, ps)
	}
	if err != nil {
		if _, ok := plugin.LookupSink(ep.Type); !ok && config.IsBuiltinSink(ep.Type) {
			return nil, config.NotInBuild("destination", name, ep.Type)
		}
		return nil, err
	}
	if e.opts.WrapSink != nil {
		sink = e.opts.WrapSink(dest, sink)
	}
	out := &endpoint{dest: dest, name: name, sink: sink, origin: originOf(ep)}
	out.breaker = newBreaker(cb, e.now, func(s breakerState) {
		if e.m != nil {
			e.m.BreakerState.WithLabelValues(name).Set(float64(s))
		}
		if s == stateOpen {
			e.log.Warn("circuit breaker open", "endpoint", name)
		} else {
			e.log.Info("circuit breaker "+s.String(), "endpoint", name)
		}
	})
	out.healthy.Store(true)
	return out, nil
}

func (e *Engine) buildRoutes(ctx context.Context) error {
	deps := router.Deps{Spec: e.spec, Relay: e.spec.Name, AI: e.opts.AI, Store: e.Store, Logger: e.log, Metrics: e.m}
	for _, rs := range e.spec.Routes {
		r := &route{name: rs.Name, from: rs.From}
		if rs.Filter != "" {
			x, err := transform.CompileCEL(rs.Filter)
			if err != nil {
				return fmt.Errorf("route %s.filter: %w", rs.Name, err)
			}
			r.filter = x
		}
		chain, err := transform.Compile(fmt.Sprintf("route %q.transforms", rs.Name), rs.Transforms)
		if err != nil {
			return err
		}
		r.transforms = chain
		if custom, ok := e.opts.Routers[rs.Name]; ok {
			r.router = custom
		} else if r.router, err = router.Build(ctx, rs, deps); err != nil {
			return err
		}
		e.routes = append(e.routes, r)
		for _, s := range rs.From {
			e.bySource[s] = append(e.bySource[s], r)
		}
	}
	return nil
}

func (e *Engine) buildSources(ctx context.Context) error {
	for i := range e.spec.Sources {
		src := e.spec.Sources[i]
		s, err := plugin.NewSource(ctx, plugin.Spec{Name: src.Name, Kind: src.Type, Options: src.Options, Config: &src, Env: e.pluginEnv()})
		if err != nil {
			if _, ok := plugin.LookupSource(src.Type); !ok && config.IsBuiltinSource(src.Type) {
				return config.NotInBuild("source", src.Name, src.Type)
			}
			return err
		}
		e.sources = append(e.sources, s)
	}
	return nil
}

// Sources are the built sources.
func (e *Engine) Sources() []plugin.Source { return e.sources }

// Run starts the sources and delivers until ctx ends, a source fails, or
// every source has run out (stdin at EOF) and the queue is empty. On the
// way out it stops the sources, finishes queued and in-flight messages
// within server.shutdown_grace, and closes everything. With a disk buffer,
// the messages still queued at shutdown stay on disk for the next start
// instead.
func (e *Engine) Run(ctx context.Context) error {
	var disk *diskBuffer
	if e.spec.Buffer.Type == "disk" {
		d, err := e.openDisk()
		if err != nil {
			e.Close(context.Background())
			return err
		}
		disk, e.disk = d, d
	}
	if err := e.startSinks(ctx); err != nil {
		e.Close(context.Background())
		return err
	}
	hctx, hcancel := context.WithCancel(context.Background())
	defer hcancel()
	for _, name := range e.order {
		if d := e.dests[name]; d.group != nil && d.group.hc != nil {
			go e.healthLoop(hctx, d)
		}
	}
	pctx, pcancel := context.WithCancel(context.WithoutCancel(ctx))
	defer pcancel()
	sctx, scancel := context.WithCancel(ctx)
	defer scancel()

	srcErr := make(chan error, len(e.sources))
	var srcWG sync.WaitGroup
	for _, s := range e.sources {
		srcWG.Add(1)
		go func(s plugin.Source) {
			defer srcWG.Done()
			if err := s.Start(sctx, e.buffer); err != nil && sctx.Err() == nil {
				srcErr <- fmt.Errorf("source %s: %w", s.Name(), err)
			}
		}(s)
	}
	sourcesDone := make(chan struct{})
	go func() { srcWG.Wait(); close(sourcesDone) }()
	if len(e.sources) > 0 {
		e.log.Info("relay running", "relay", e.spec.Name, "sources", len(e.sources), "routes", len(e.routes), "destinations", len(e.dests))
	}
	stopSources := func() {
		scancel()
		srcWG.Wait()
	}

	var runErr error
	if disk != nil {
		runErr = e.runDisk(ctx, pctx, disk, srcErr, sourcesDone, stopSources)
	} else {
		runErr = e.runMemory(ctx, pctx, srcErr, sourcesDone, stopSources)
	}
	if ctx.Err() != nil {
		// A shutdown: the messages a delay holds are given up now (delay.go),
		// not after the grace. A run whose sources ran out keeps waiting
		// for them, up to the grace.
		close(e.stopping)
	}
	done := make(chan struct{})
	go func() { e.inflight.Wait(); close(done) }()
	grace := e.spec.Server.ShutdownGrace.Std()
	if grace <= 0 {
		grace = config.DefaultShutdownGrace
	}
	select {
	case <-done:
	case <-time.After(grace):
		e.log.Warn("shutdown grace elapsed; abandoning in-flight messages", "grace", grace.String())
		pcancel()
		<-done
	}
	if disk != nil {
		if n := disk.nackPending(pctx); n > 0 {
			e.log.Info("buffer: messages stay on disk for the next start", "count", n, "path", disk.log.Path())
		}
		disk.closeDeletes()
	}
	e.Close(context.Background())
	return runErr
}

// runMemory is the memory buffer's loop: the intake channel is the queue.
func (e *Engine) runMemory(ctx, pctx context.Context, srcErr <-chan error, sourcesDone <-chan struct{}, stopSources func()) error {
	var runErr error
loop:
	for {
		select {
		case env := <-e.buffer:
			env.Queued()
			e.dispatch(pctx, env)
		case err := <-srcErr:
			runErr = err
			e.log.Error("source failed; stopping", "err", err)
			break loop
		case <-sourcesDone:
			break loop
		case <-ctx.Done():
			break loop
		}
	}
	stopSources()
	for drained := false; !drained; {
		select {
		case env := <-e.buffer:
			env.Queued()
			e.dispatch(pctx, env)
		default:
			drained = true
		}
	}
	return runErr
}

// runDisk is the disk buffer's loop (see disk.go).
func (e *Engine) runDisk(ctx, pctx context.Context, d *diskBuffer, srcErr <-chan error, sourcesDone <-chan struct{}, stopSources func()) error {
	stopWriting := make(chan struct{})
	written := make(chan struct{})
	go func() { d.write(pctx, e.buffer, stopWriting); close(written) }()
	dispatched := make(chan struct{})
	go func() { d.dispatch(pctx); close(dispatched) }()
	var runErr error
	finite := false
	select {
	case runErr = <-srcErr:
		e.log.Error("source failed; stopping", "err", runErr)
	case <-sourcesDone:
		finite = true
	case <-ctx.Done():
	}
	stopSources()
	close(stopWriting)
	<-written
	if finite && ctx.Err() == nil {
		d.mu.Lock()
		d.retry = false
		d.mu.Unlock()
		d.waitIdle(ctx)
	}
	d.stop()
	<-dispatched
	return runErr
}

func (e *Engine) startSinks(ctx context.Context) error {
	for _, name := range e.order {
		for _, ep := range e.dests[name].endpoints {
			if st, ok := ep.sink.(plugin.Starter); ok {
				if err := st.Start(ctx); err != nil {
					return fmt.Errorf("destination %s: %w", ep.name, err)
				}
			}
		}
	}
	return nil
}

// dispatch hands a message to a worker, waiting for a free one.
func (e *Engine) dispatch(ctx context.Context, env *envelope.Envelope) {
	e.dispatchDone(ctx, env, nil)
}

// dispatchDone is dispatch with a callback for the finished report. An
// ordered message waits behind the messages of its key.
func (e *Engine) dispatchDone(ctx context.Context, env *envelope.Envelope, done doneFunc) {
	e.queueDepth()
	if e.orderer != nil && e.orderer.submit(ctx, env, done) {
		return
	}
	e.sem <- struct{}{}
	e.inflight.Add(1)
	go func() {
		defer func() { <-e.sem; e.inflight.Done() }()
		e.run(withSlot(ctx), env, done, false)
	}()
}

// queueDepth updates the queue gauge: the intake channel, or the disk
// buffer's messages.
func (e *Engine) queueDepth() {
	if e.m == nil {
		return
	}
	if e.disk != nil {
		e.m.QueueDepth.Set(float64(e.disk.log.Len()))
		return
	}
	e.m.QueueDepth.Set(float64(len(e.buffer)))
}

// Close closes sinks, the history recorder and the stores the engine opened.
// Run calls it; call it yourself for an engine that never ran.
func (e *Engine) Close(ctx context.Context) {
	e.closeOnce.Do(func() {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		for _, d := range e.dests {
			for _, ep := range d.endpoints {
				if err := ep.sink.Close(cctx); err != nil {
					e.log.Warn("closing sink", "endpoint", ep.name, "err", err)
				}
			}
		}
		e.closeDLQ(cctx)
		for _, src := range e.sources {
			if c, ok := src.(plugin.Closer); ok {
				if err := c.Close(cctx); err != nil {
					e.log.Warn("closing source", "source", src.Name(), "err", err)
				}
			}
		}
		if e.recorder != nil {
			if err := e.recorder.Close(cctx); err != nil {
				e.log.Warn("history writes still queued at shutdown", "err", err)
			}
		}
		e.closeStores()
		if e.disk != nil {
			if err := e.disk.log.Close(); err != nil {
				e.log.Warn("closing the disk buffer", "err", err)
			}
		}
	})
}

func (e *Engine) closeStores() {
	for name, st := range e.stores {
		if e.ownStore[name] {
			_ = st.Close()
		}
	}
}

// Buffer is the queue sources send on (tests).
func (e *Engine) Buffer() chan<- *envelope.Envelope { return e.buffer }
