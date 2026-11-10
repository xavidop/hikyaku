package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a source; tests shorten them. Close spends at most
// commitTimeout + leaveTimeout: the relay's shutdown budget is shared.
const (
	defaultCommitEvery   = time.Second
	defaultCommitTimeout = 5 * time.Second
	defaultLeaveTimeout  = 3 * time.Second
)

// propagator reads a W3C traceparent header, as the http source does.
var propagator = propagation.TraceContext{}

// Source is a kafka source: one member of a consumer group.
type Source struct {
	name string
	opts sourceOptions
	log  *slog.Logger
	// kopts are the client options, without the rebalance callbacks.
	kopts []kgo.Opt

	commitEvery   time.Duration
	commitTimeout time.Duration
	leaveTimeout  time.Duration
	rnd           func() float64

	started atomic.Bool

	mu    sync.Mutex
	cl    *kgo.Client
	parts map[tp]*partState
	// inflight counts the records polled and not settled yet.
	inflight      int
	closing       bool
	commitFailing bool

	// room gets a token whenever an in-flight record settles.
	room chan struct{}
	// commitMu serializes commits: a periodic commit computed before a
	// revoke never lands after it.
	commitMu sync.Mutex

	stop      chan struct{} // closed by Close: stops the committer
	committer chan struct{} // closed once the committer returned; nil before Start
	closeOnce sync.Once
	closeErr  error
}

func newSource(_ context.Context, spec plugin.Spec) (*Source, error) {
	opts, err := parseSource(spec)
	if err != nil {
		return nil, err
	}
	log := spec.Env.Log()
	s := &Source{
		name: spec.Name, opts: opts, log: log,
		commitEvery: defaultCommitEvery, commitTimeout: defaultCommitTimeout, leaveTimeout: defaultLeaveTimeout, rnd: rand.Float64,
		parts: map[tp]*partState{}, room: make(chan struct{}, 1), stop: make(chan struct{}),
	}
	s.kopts = append(opts.client.kgoOpts(opts.brokers, log),
		kgo.ConsumerGroup(opts.group),
		kgo.ConsumeTopics(opts.topics...),
		kgo.ConsumeStartOffset(opts.startOffset()),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		// A poll and a rebalance never interleave: the records a poll
		// returns are tracked before their partitions can be revoked.
		kgo.BlockRebalanceOnPoll(),
	)
	return s, nil
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Health pings the brokers.
func (s *Source) Health(ctx context.Context) error {
	s.mu.Lock()
	cl, closing := s.cl, s.closing
	s.mu.Unlock()
	switch {
	case closing:
		return errClosed
	case cl == nil:
		return errNotStarted
	}
	return cl.Ping(ctx)
}

// Start joins the consumer group and hands records to the relay until ctx
// ends, then returns nil and stops fetching: Close commits and leaves the
// group once the relay has finished the records in flight. With
// reconnect.max_attempts set, Start fails once the brokers refused that
// many connections in a row for the credentials or the certificate, or that
// many polls in a row for the ACLs; by default it keeps retrying.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	pctx, giveUp := context.WithCancelCause(ctx)
	defer giveUp(nil)
	maxAttempts := s.opts.reconnect.maxAttempts
	cl, err := s.connect(newConnWatch(func(n int, err error) {
		if maxAttempts > 0 && n >= maxAttempts {
			giveUp(fmt.Errorf("giving up after %d refused connections in a row: %w", n, err))
			return
		}
		if n == 1 || n%10 == 0 {
			s.log.Warn("kafka brokers refused the connection; retrying", "source", s.name, "refused", n, "max_attempts", maxAttempts, "err", err)
		}
	}))
	if err != nil {
		return err
	}
	s.log.Info("kafka source consuming", "source", s.name, "group", s.opts.group, "topics", s.opts.topics, "brokers", s.opts.brokers)
	err = s.consume(pctx, cl, out)
	cl.PauseFetchTopics(s.opts.topics...)
	if cause := context.Cause(pctx); err == nil && ctx.Err() == nil && cause != nil {
		err = cause
	}
	return err
}

// connect creates the client, which joins the group, and starts the
// committer.
func (s *Source) connect(watch *connWatch) (*kgo.Client, error) {
	opts := append(slices.Clone(s.kopts),
		kgo.WithHooks(watch),
		kgo.OnPartitionsAssigned(s.onAssigned),
		kgo.OnPartitionsRevoked(s.onRevoked),
		kgo.OnPartitionsLost(s.onLost),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", s.name, err)
	}
	s.mu.Lock()
	closing := s.closing
	if !closing {
		s.cl = cl
		s.committer = make(chan struct{})
		go s.commitLoop(cl, s.committer)
	}
	s.mu.Unlock()
	if closing {
		cl.CloseAllowingRebalance()
		return nil, errClosed
	}
	return cl, nil
}

// polled is a record taken from a poll and the state that tracks it.
type polled struct {
	rec *kgo.Record
	ps  *partState
}

// consume polls and hands records to the relay. A poll takes at most the
// room left under prefetch, so while the relay applies backpressure the
// source stops polling, and franz-go stops fetching once it holds one
// fetch per broker.
//
// franz-go retries on its own and reports the errors it hit to the polls,
// at its own pace. An ACL refusal counts toward reconnect.max_attempts and
// is spaced by reconnect.backoff; anything else is logged.
func (s *Source) consume(ctx context.Context, cl *kgo.Client, out chan<- *envelope.Envelope) error {
	rc := s.opts.reconnect
	refusals := 0
	var errs errLog
	for {
		room, ok := s.waitRoom(ctx)
		if !ok {
			return nil
		}
		fetches := cl.PollRecords(ctx, room)
		batch := s.take(fetches)
		cl.AllowRebalance()
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil // what was polled and not handed over is redelivered
		}
		refused := s.fetchErrors(fetches, &errs)
		if len(batch) > 0 {
			refusals = 0
			errs.recovered()
			if !s.hand(ctx, batch, out) {
				return nil
			}
			continue
		}
		if refused == nil {
			continue
		}
		refusals++
		if rc.maxAttempts > 0 && refusals >= rc.maxAttempts {
			return fmt.Errorf("giving up after %d refused polls in a row: %w", refusals, refused)
		}
		if !backoff.Sleep(ctx, backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, refusals, s.rnd)) {
			return nil
		}
	}
}

// errLog rate-limits the consumer errors of a poll loop: the first of a run
// is logged at warn, then one every errLogEvery; the rest go to debug.
type errLog struct {
	last time.Time
}

const errLogEvery = 30 * time.Second

func (e *errLog) level() slog.Level {
	if now := time.Now(); now.Sub(e.last) >= errLogEvery {
		e.last = now
		return slog.LevelWarn
	}
	return slog.LevelDebug
}

// recovered ends a run of errors: the next one is logged at warn.
func (e *errLog) recovered() { e.last = time.Time{} }

// waitRoom waits until fewer than prefetch records are in flight and
// returns how many more may be polled; false once ctx ended.
func (s *Source) waitRoom(ctx context.Context) (int, bool) {
	for {
		s.mu.Lock()
		n := s.opts.prefetch - s.inflight
		s.mu.Unlock()
		if n > 0 {
			return n, true
		}
		select {
		case <-s.room:
		case <-ctx.Done():
			return 0, false
		}
	}
}

// take tracks the records of a poll in their partitions' watermarks.
func (s *Source) take(fetches kgo.Fetches) []polled {
	n := fetches.NumRecords()
	if n == 0 {
		return nil
	}
	batch := make([]polled, 0, n)
	var resets []string
	s.mu.Lock()
	fetches.EachRecord(func(r *kgo.Record) {
		k := tp{r.Topic, r.Partition}
		ps := s.parts[k]
		if ps == nil {
			ps = newPartState(k)
			s.parts[k] = ps
		}
		if ps.add(r.Offset, r.LeaderEpoch) == rewound {
			// The consumer went back (it reset after data loss): track
			// the partition anew, and ignore what the old state was
			// still waiting for.
			resets = append(resets, k.String()+"@"+strconv.FormatInt(r.Offset, 10))
			ps.dead = true
			ps = newPartState(k)
			s.parts[k] = ps
			ps.add(r.Offset, r.LeaderEpoch)
		}
		s.inflight++
		batch = append(batch, polled{rec: r, ps: ps})
	})
	s.mu.Unlock()
	for _, at := range resets {
		s.log.Warn("kafka partition rewound; tracking it from here", "source", s.name, "at", at)
	}
	return batch
}

// fetchErrors logs what a poll reported and returns the first refusal by
// the ACLs or the credentials, nil when there was none. A context end and
// the client closing are not errors; a data-loss reset is logged (franz-go
// resumes on its own).
func (s *Source) fetchErrors(fetches kgo.Fetches, errs *errLog) (refused error) {
	fetches.EachError(func(topic string, partition int32, err error) {
		var loss *kgo.ErrDataLoss
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, kgo.ErrClientClosed):
			return
		case errors.As(err, &loss):
			s.log.Warn("kafka consumer detected data loss and reset", "source", s.name, "partition", tp{topic, partition}.String(), "err", err)
			return
		case authFailure(err) && refused == nil:
			refused = err
		}
		attrs := []any{"source", s.name, "group", s.opts.group, "err", err}
		if topic != "" {
			attrs = append(attrs, "topic", topic)
		}
		s.log.Log(context.Background(), errs.level(), "kafka consumer error", attrs...)
	})
	return refused
}

// hand gives the polled records to the relay, in order. It returns false
// once ctx ended; the records not handed over stay unsettled, so their
// partitions resume from them.
func (s *Source) hand(ctx context.Context, batch []polled, out chan<- *envelope.Envelope) bool {
	for _, p := range batch {
		r, ps := p.rec, p.ps
		if s.dead(ps) {
			// Revoked since the poll: its new owner delivers it.
			s.settle(ps, r.Offset, false)
			continue
		}
		if int64(len(r.Value)) > s.opts.maxBytes {
			// Redelivering it would fail the same way, forever.
			s.log.Warn("kafka record over max_body_bytes skipped", "source", s.name, "partition", ps.tp.String(),
				"offset", r.Offset, "bytes", len(r.Value), "max_body_bytes", s.opts.maxBytes)
			s.settle(ps, r.Offset, true)
			continue
		}
		env := s.envelope(r)
		offset := r.Offset
		plugin.OnSettle(env, s.opts.ack, func(_ context.Context, final bool) { s.settle(ps, offset, final) })
		select {
		case out <- env:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func (s *Source) dead(ps *partState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ps.dead
}

// settle records a record's outcome in its partition's watermark. A
// partition revoked since then ignores it.
func (s *Source) settle(ps *partState, offset int64, final bool) {
	s.mu.Lock()
	s.inflight--
	held := !ps.dead && ps.settle(offset, final)
	s.mu.Unlock()
	select {
	case s.room <- struct{}{}:
	default:
	}
	if held {
		s.log.Warn("kafka record failed: its partition's commit is held until the next restart or rebalance, which resumes from it and delivers the records after it again",
			"source", s.name, "partition", ps.tp.String(), "offset", offset)
	}
}

// envelope turns a record into an envelope.
func (s *Source) envelope(r *kgo.Record) *envelope.Envelope {
	env := envelope.New(s.name, Kind, r.Value)
	for _, h := range r.Headers {
		env.Headers[h.Key] = append(env.Headers[h.Key], string(h.Value))
	}
	partition := strconv.Itoa(int(r.Partition))
	if key, ok := header(r.Headers, headerIdempotency); ok && key != "" && len(key) <= maxIdempotencyKey {
		env.ID = key
		env.Attributes["idempotency_key"] = key
	} else {
		env.ID = r.Topic + "-" + partition + "-" + strconv.FormatInt(r.Offset, 10)
	}
	env.Subject = r.Topic
	if ct, ok := header(r.Headers, headerContentType); ok && ct != "" {
		env.ContentType = ct
	} else {
		env.ContentType = detectContentType(r.Value)
	}
	env.Attributes["kafka.topic"] = r.Topic
	env.Attributes["kafka.partition"] = int64(r.Partition)
	env.Attributes["kafka.offset"] = r.Offset
	env.Attributes["kafka.key"] = string(r.Key)
	env.Attributes["kafka.timestamp"] = r.Timestamp.UTC().Format(time.RFC3339Nano)
	env.Attributes["ordering_key"] = r.Topic + "/" + partition
	if sc := trace.SpanContextFromContext(propagator.Extract(context.Background(), carrier(r.Headers))); sc.IsValid() {
		env.Trace = sc
	}
	return env
}

// carrier reads record headers for trace propagation.
type carrier []kgo.RecordHeader

func (c carrier) Get(key string) string {
	v, _ := header(c, key)
	return v
}

func (c carrier) Set(string, string) {}

func (c carrier) Keys() []string {
	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
}

func (s *Source) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	if parts := tps(assigned); len(parts) > 0 {
		s.log.Info("kafka partitions assigned", "source", s.name, "group", s.opts.group, "partitions", describe(parts))
	}
}

func (s *Source) onRevoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	s.release(ctx, cl, revoked, "revoked")
}

func (s *Source) onLost(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
	s.release(ctx, cl, lost, "lost")
}

// release commits the final prefix of the partitions this member no longer
// owns, then forgets them: later settles are ignored, and the next owner
// resumes from the commit.
func (s *Source) release(ctx context.Context, cl *kgo.Client, parts map[string][]int32, why string) {
	list := tps(parts)
	if len(list) == 0 {
		return
	}
	only := make(map[tp]bool, len(list))
	for _, k := range list {
		only[k] = true
	}
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if !closing { // Close committed and forgot every partition already
		_ = s.commit(ctx, cl, only, true, "partitions "+why)
	}
	s.log.Info("kafka partitions "+why, "source", s.name, "group", s.opts.group, "partitions", describe(list))
}

// commitLoop commits every commitEvery until Close.
func (s *Source) commitLoop(cl *kgo.Client, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(s.commitEvery)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			_ = s.commit(context.Background(), cl, nil, false, "periodic")
		}
	}
}

// commit commits the final prefix of the partitions in only (every
// partition when nil) that moved since their last commit. With drop it
// forgets those partitions in the same step, so no settle slips in between.
func (s *Source) commit(ctx context.Context, cl *kgo.Client, only map[tp]bool, drop bool, why string) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	offsets := map[string]map[int32]kgo.EpochOffset{}
	var states []*partState
	s.mu.Lock()
	for k, ps := range s.parts {
		if only != nil && !only[k] {
			continue
		}
		if eo, ok := ps.toCommit(); ok {
			if offsets[k.topic] == nil {
				offsets[k.topic] = map[int32]kgo.EpochOffset{}
			}
			offsets[k.topic][k.partition] = eo
			states = append(states, ps)
		}
		if drop {
			ps.dead = true
			delete(s.parts, k)
		}
	}
	s.mu.Unlock()
	if len(offsets) == 0 {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, s.commitTimeout)
	defer cancel()
	var err error
	cl.CommitOffsetsSync(cctx, offsets, func(_ *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, rerr error) {
		err = commitError(req, resp, rerr)
	})
	s.mu.Lock()
	if err == nil {
		for _, ps := range states {
			eo := offsets[ps.tp.topic][ps.tp.partition]
			ps.markCommitted(eo.Offset)
		}
	}
	wasFailing := s.commitFailing
	s.commitFailing = err != nil
	s.mu.Unlock()
	switch {
	case err != nil && !wasFailing:
		s.log.Warn("kafka offset commit failed; retrying with the next commit", "source", s.name, "group", s.opts.group, "why", why, "err", err)
	case err != nil:
		s.log.Debug("kafka offset commit failed again", "source", s.name, "group", s.opts.group, "why", why, "err", err)
	case wasFailing:
		s.log.Info("kafka offset commits recovered", "source", s.name, "group", s.opts.group)
	}
	return err
}

// commitError is the error of a commit: the request's, or the first
// partition's. A commit that fails is simply made again later.
func commitError(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) error {
	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	names := map[[16]byte]string{}
	if req != nil {
		for _, t := range req.Topics {
			names[t.TopicID] = t.Topic
		}
	}
	var errs []error
	for _, t := range resp.Topics {
		topic := t.Topic
		if topic == "" {
			topic = names[t.TopicID]
		}
		for _, p := range t.Partitions {
			if perr := kerr.ErrorForCode(p.ErrorCode); perr != nil {
				errs = append(errs, fmt.Errorf("%s: %w", tp{topic, p.Partition}, perr))
			}
		}
	}
	return errors.Join(errs...)
}

// Close commits the final prefix of every partition, leaves the group and
// closes the client. The relay calls it once the records in flight are
// finished.
func (s *Source) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		cl, committer := s.cl, s.committer
		s.mu.Unlock()
		close(s.stop)
		if cl == nil {
			return
		}
		<-committer
		s.closeErr = s.commit(ctx, cl, nil, true, "close")
		// Leaving lets the group hand the partitions over at once rather
		// than after the session timeout. A leave that runs out of time
		// gives up, and Close no longer waits for it.
		lctx, cancel := context.WithTimeout(ctx, s.leaveTimeout)
		if err := cl.LeaveGroupContext(lctx); err != nil {
			s.log.Debug("kafka source left the group uncleanly", "source", s.name, "group", s.opts.group, "err", err)
		}
		cancel()
		cl.CloseAllowingRebalance()
		s.log.Info("kafka source closed", "source", s.name, "group", s.opts.group)
	})
	return s.closeErr
}
