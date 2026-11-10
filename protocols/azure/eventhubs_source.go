package azure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

// Timings of an eventhubs source; tests shorten them.
const (
	// defaultFlushEvery is how often checkpoints are written.
	defaultFlushEvery = time.Second
	// defaultFlushTimeout bounds one round of checkpoint writes.
	defaultFlushTimeout = 10 * time.Second
	// defaultBatchWait is how long a receive gathers more events once the
	// first one came.
	defaultBatchWait = 20 * time.Millisecond
	// defaultRefresh is how often the partition list is read again.
	defaultRefresh = time.Minute
	// receiveBatch is the most events one receive returns.
	receiveBatch = 100
)

// ehSource is an eventhubs source: a partition reader per owned partition,
// with checkpoints and leases in a store.
type ehSource struct {
	name     string
	opts     ehSourceOptions
	consumer ehConsumer
	cps      *checkpoints
	leases   *leases
	log      *slog.Logger
	now      func() time.Time
	rnd      func() float64

	flushEvery time.Duration
	batchWait  time.Duration
	refresh    time.Duration
	leaseTick  time.Duration

	started atomic.Bool
	fatal   chan error

	mu        sync.Mutex
	status    error            // not started, stopped, or the partition list's failure
	problems  map[string]error // what fails now: the leases, a partition
	closed    bool
	entries   map[*cpEntry]struct{} // checkpoints being written
	flushStop chan struct{}
	flushDone chan struct{}
}

// readers are the partition readers of a running source.
type readers struct {
	all     sync.WaitGroup
	running map[string]*partitionReader
	lostIn  map[string]int64 // partition → the epoch another reader took it in
}

// cpEntry is a partition reader's checkpoint, written until its events are
// all settled.
type cpEntry struct {
	partition string
	cp        *cpSession
	wm        *watermark
	retired   atomic.Bool // the reader stopped
	released  atomic.Bool // the partition went to another reader
	failing   bool        // the last write failed (flusher only)
}

// partitionReader reads one owned partition.
type partitionReader struct {
	partition string
	cancel    context.CancelFunc
	done      chan struct{}
	lost      atomic.Bool // another reader took the partition
	released  atomic.Bool // the lease went elsewhere
	failed    atomic.Bool // gave up after reconnect.max_attempts
}

func newEHSource(_ context.Context, spec plugin.Spec) (*ehSource, error) {
	opts, err := parseEHSource(spec)
	if err != nil {
		return nil, err
	}
	retry := azeventhubs.RetryOptions{MaxRetries: 3, RetryDelay: opts.reconnect.backoff, MaxRetryDelay: opts.reconnect.maxBackoff}
	consumer, err := dialConsumer(opts.conn, opts.hub, opts.group, retry)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	s := buildEHSource(spec, opts, consumer)
	if _, ok := opts.store.(*store.Memory); ok {
		s.log.Warn("eventhubs checkpoints and leases are in the in-process memory store: a restart reads from start: again, and replicas do not share partitions; declare a redis store as state_store",
			"source", spec.Name)
	}
	return s, nil
}

// buildEHSource makes the source on a consumer (tests pass a fake).
func buildEHSource(spec plugin.Spec, opts ehSourceOptions, consumer ehConsumer) *ehSource {
	prefix := "eventhubs:" + opts.conn.namespace + ":" + opts.hub + ":" + opts.group
	s := &ehSource{
		name: spec.Name, opts: opts, consumer: consumer, log: spec.Env.Log(), now: time.Now, rnd: rand.Float64,
		flushEvery: defaultFlushEvery, batchWait: defaultBatchWait, refresh: defaultRefresh,
		leaseTick: defaultLeaseEpoch / 4,
		fatal:     make(chan error, 1), status: errNotStarted,
		problems: map[string]error{}, entries: map[*cpEntry]struct{}{},
	}
	s.cps = &checkpoints{kv: opts.store, prefix: prefix, now: func() time.Time { return s.now() }}
	s.leases = newLeases(opts.store, prefix, replicaID(), defaultLeaseEpoch, func() time.Time { return s.now() })
	return s
}

// Name is the source name.
func (s *ehSource) Name() string { return s.name }

func (s *ehSource) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

// problem records (or, with nil, clears) what fails now in one part of the
// source: "leases", or a partition.
func (s *ehSource) problem(part string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.problems, part)
		return
	}
	s.problems[part] = err
}

// Health is what fails now (the leases, a partition reader), else the
// event hub's properties: the connection, the credentials and the hub.
func (s *ehSource) Health(ctx context.Context) error {
	s.mu.Lock()
	status, closed := s.status, s.closed
	var problem error
	for _, part := range slices.Sorted(maps.Keys(s.problems)) {
		problem = s.problems[part]
		break
	}
	s.mu.Unlock()
	switch {
	case closed:
		return errStopped
	case status != nil:
		return status
	case problem != nil:
		return problem
	}
	if _, err := s.consumer.GetEventHubProperties(ctx, nil); err != nil {
		return fmt.Errorf("event hub %s: %w", s.opts.hub, err)
	}
	return nil
}

// Start reads the partitions this replica owns until ctx ends, then returns
// nil; the checkpoints of events still in flight are written as they settle,
// and on Close. It fails only after reconnect.max_attempts failures in a row.
func (s *ehSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	s.setStatus(nil)
	parts, err := s.readPartitions(ctx)
	if err != nil || ctx.Err() != nil {
		if ctx.Err() != nil {
			s.setStatus(errStopped)
			return nil
		}
		s.setStatus(err)
		return err
	}
	s.log.Info("eventhubs source reading", "source", s.name, "namespace", s.opts.conn.namespace, "event_hub", s.opts.hub,
		"consumer_group", s.opts.group, "partitions", len(parts), "replica", s.leases.id)
	s.startFlusher()

	rs := &readers{running: map[string]*partitionReader{}, lostIn: map[string]int64{}}
	defer func() {
		for _, r := range rs.running {
			r.cancel()
		}
		rs.all.Wait()
	}()
	refreshed := s.now()
	t := time.NewTicker(s.leaseTick)
	defer t.Stop()
	for {
		if s.now().Sub(refreshed) >= s.refresh {
			if ids, err := s.properties(ctx); err == nil {
				s.setPartitions(ids)
			}
			refreshed = s.now()
		}
		s.reconcile(ctx, rs, out)
		select {
		case <-ctx.Done():
			s.setStatus(errStopped)
			return nil
		case err := <-s.fatal:
			s.setStatus(err)
			return err
		case <-t.C:
		}
	}
}

// reconcile takes and renews leases, then starts a reader for each
// partition owned and stops the readers of the others. A partition another
// reader took is not read again in the same epoch. When the leases cannot
// be read, the readers go on.
func (s *ehSource) reconcile(ctx context.Context, rs *readers, out chan<- *envelope.Envelope) {
	owned, epoch, err := s.leases.tick(ctx)
	s.mu.Lock()
	_, failing := s.problems["leases"]
	s.mu.Unlock()
	if err != nil {
		if ctx.Err() == nil {
			s.problem("leases", fmt.Errorf("leases: %w", err))
			if !failing {
				s.log.Warn("eventhubs leases unavailable; the partitions read so far go on", "source", s.name, "err", err)
			}
		}
		return
	}
	if failing {
		s.problem("leases", nil)
		s.log.Info("eventhubs leases available again", "source", s.name)
	}
	for p, r := range rs.running {
		select {
		case <-r.done:
			delete(rs.running, p)
			if r.lost.Load() {
				rs.lostIn[p] = epoch
			}
			continue
		default:
		}
		if !owned[p] {
			s.log.Info("eventhubs partition released", "source", s.name, "partition", p)
			r.released.Store(true)
			r.cancel()
			delete(rs.running, p)
		}
	}
	for _, p := range s.leases.partitions {
		if !owned[p] || rs.running[p] != nil {
			continue
		}
		if e, ok := rs.lostIn[p]; ok && e >= epoch {
			continue
		}
		delete(rs.lostIn, p)
		rs.running[p] = s.startReader(ctx, &rs.all, p, out)
	}
}

// readPartitions reads the partition IDs, retrying with reconnect.*.
func (s *ehSource) readPartitions(ctx context.Context) ([]string, error) {
	for failures := 1; ; failures++ {
		ids, err := s.properties(ctx)
		if err == nil {
			s.problem("event hub", nil)
			s.setPartitions(ids)
			return ids, nil
		}
		if ctx.Err() != nil {
			return nil, nil
		}
		if !s.retryWait(ctx, "", "read the event hub", failures, err) {
			if ctx.Err() != nil {
				return nil, nil
			}
			return nil, fmt.Errorf("giving up after %d failures in a row: %w", failures, err)
		}
	}
}

func (s *ehSource) properties(ctx context.Context) ([]string, error) {
	props, err := s.consumer.GetEventHubProperties(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("event hub %s: %w", s.opts.hub, err)
	}
	if len(props.PartitionIDs) == 0 {
		return nil, fmt.Errorf("event hub %s has no partitions", s.opts.hub)
	}
	return props.PartitionIDs, nil
}

func (s *ehSource) setPartitions(ids []string) {
	s.leases.partitions = sortPartitions(ids)
}

// retryWait records a failure and waits before the next try. It returns
// false when ctx ended or reconnect.max_attempts is used up.
func (s *ehSource) retryWait(ctx context.Context, partition, what string, failures int, err error) bool {
	rc := s.opts.reconnect
	part := partition
	if part == "" {
		part = "event hub"
	}
	s.problem(part, fmt.Errorf("%s: %w", what, err))
	if rc.maxAttempts > 0 && failures >= rc.maxAttempts {
		return false
	}
	wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, failures, s.rnd)
	s.log.Warn("eventhubs: failed to "+what+"; retrying", "source", s.name, "event_hub", s.opts.hub,
		"partition", partition, "failures", failures, "wait", wait.String(), "err", err)
	return backoff.Sleep(ctx, wait)
}

func (s *ehSource) fail(err error) {
	select {
	case s.fatal <- err:
	default:
	}
}

func (s *ehSource) startReader(ctx context.Context, all *sync.WaitGroup, partition string, out chan<- *envelope.Envelope) *partitionReader {
	rctx, cancel := context.WithCancel(ctx)
	r := &partitionReader{partition: partition, cancel: cancel, done: make(chan struct{})}
	all.Go(func() {
		defer close(r.done)
		defer cancel()
		s.read(rctx, r, out)
	})
	return r
}

// read is a partition reader: it allocates a checkpoint generation (its
// owner level), decides where to start, and receives until ctx ends or
// another reader takes the partition.
func (s *ehSource) read(ctx context.Context, r *partitionReader, out chan<- *envelope.Envelope) {
	p := r.partition
	defer func() {
		if !r.failed.Load() {
			s.problem(p, nil)
		}
	}()
	cp, pos, base, ok := s.open(ctx, r)
	if !ok {
		return
	}
	s.problem(p, nil)
	en := &cpEntry{partition: p, cp: cp, wm: newWatermark(base)}
	s.mu.Lock()
	s.entries[en] = struct{}{}
	s.mu.Unlock()
	defer func() {
		if r.lost.Load() || r.released.Load() {
			en.released.Store(true)
		}
		en.retired.Store(true)
	}()
	s.log.Info("eventhubs partition owned", "source", s.name, "partition", p, "owner_level", cp.gen, "start", describe(pos))

	owner := cp.gen
	var pc ehPartition
	defer func() {
		if pc != nil {
			_ = pc.Close(context.Background())
		}
	}()
	last, received := int64(0), false
	failures := 0
	for {
		if pc == nil {
			start := pos
			if received {
				start = after(last)
			}
			var err error
			if pc, err = s.consumer.newPartitionClient(p, &azeventhubs.PartitionClientOptions{StartPosition: start, OwnerLevel: &owner}); err != nil {
				failures++
				if !s.retryWait(ctx, p, "open partition "+p, failures, err) {
					s.giveUp(ctx, r, failures, err)
					return
				}
				continue
			}
		}
		events, err := s.receive(ctx, pc)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if ownershipLost(err) {
				r.lost.Store(true)
				s.log.Info("eventhubs partition taken by another reader", "source", s.name, "partition", p)
				return
			}
			_ = pc.Close(context.Background())
			pc = nil
			failures++
			if !s.retryWait(ctx, p, "receive from partition "+p, failures, err) {
				s.giveUp(ctx, r, failures, err)
				return
			}
			continue
		}
		if failures > 0 {
			failures = 0
			s.problem(p, nil)
		}
		for _, ev := range events {
			last, received = ev.SequenceNumber, true
			if !s.handle(ctx, p, en.wm, ev, out) {
				return
			}
		}
	}
}

func (s *ehSource) giveUp(ctx context.Context, r *partitionReader, failures int, err error) {
	if ctx.Err() != nil {
		return
	}
	r.failed.Store(true)
	s.fail(fmt.Errorf("partition %s: giving up after %d failures in a row: %w", r.partition, failures, err))
}

// open allocates the reader's checkpoint generation and decides where it
// starts: after the newest checkpoint, else per start:. The start is
// written as the generation's base at once, so a restart before the first
// checkpoint resumes there (start: latest is pinned on the first read).
func (s *ehSource) open(ctx context.Context, r *partitionReader) (*cpSession, azeventhubs.StartPosition, int64, bool) {
	p := r.partition
	var (
		props azeventhubs.PartitionProperties
		cp    *cpSession
		err   error
	)
	for failures := 1; ; failures++ {
		if props, err = s.consumer.GetPartitionProperties(ctx, p, nil); err == nil {
			break
		}
		if !s.retryWait(ctx, p, "read partition "+p, failures, err) {
			s.giveUp(ctx, r, failures, err)
			return nil, azeventhubs.StartPosition{}, 0, false
		}
	}
	for failures := 1; ; failures++ {
		if cp, err = s.cps.alloc(ctx, p); err == nil {
			break
		}
		if !s.retryWait(ctx, p, "open the checkpoint of partition "+p, failures, err) {
			s.giveUp(ctx, r, failures, err)
			return nil, azeventhubs.StartPosition{}, 0, false
		}
	}
	var (
		v     int64
		found bool
	)
	for failures := 1; ; failures++ {
		if v, found, err = s.cps.latest(ctx, p, cp.gen); err == nil {
			break
		}
		if !s.retryWait(ctx, p, "read the checkpoint of partition "+p, failures, err) {
			s.giveUp(ctx, r, failures, err)
			return nil, azeventhubs.StartPosition{}, 0, false
		}
	}
	pos, base := s.startAt(p, v, found, props)
	if err := cp.begin(ctx, base); err != nil {
		s.log.Warn("eventhubs: could not write the start of partition's checkpoint; it is written with the next checkpoint",
			"source", s.name, "partition", p, "err", err)
	}
	return cp, pos, base, true
}

// startAt is where a new owner starts: after the checkpoint v when found
// (unless it is past the partition's last event: the hub was recreated),
// else at the earliest event, or after the last one (latest).
func (s *ehSource) startAt(p string, v int64, found bool, props azeventhubs.PartitionProperties) (azeventhubs.StartPosition, int64) {
	if found && !props.IsEmpty && v > props.LastEnqueuedSequenceNumber {
		s.log.Warn("eventhubs checkpoint is past the partition's last event (was the event hub recreated?); starting from start:",
			"source", s.name, "partition", p, "checkpoint", v, "last_sequence_number", props.LastEnqueuedSequenceNumber, "start", s.opts.start)
		found = false
	}
	switch {
	case found:
		return after(v), v
	case s.opts.start == startEarliest:
		return after(-1), -1
	}
	last := max(props.LastEnqueuedSequenceNumber, -1)
	return after(last), last
}

// after is the position after sequence number seq; the earliest event when
// seq is negative.
func after(seq int64) azeventhubs.StartPosition {
	if seq < 0 {
		earliest := true
		return azeventhubs.StartPosition{Earliest: &earliest}
	}
	return azeventhubs.StartPosition{SequenceNumber: &seq}
}

func describe(p azeventhubs.StartPosition) string {
	if p.SequenceNumber != nil {
		return "after sequence number " + strconv.FormatInt(*p.SequenceNumber, 10)
	}
	return "earliest"
}

// receive waits for an event, then gathers the ones that follow within
// batchWait.
func (s *ehSource) receive(ctx context.Context, pc ehPartition) ([]*azeventhubs.ReceivedEventData, error) {
	events, err := pc.ReceiveEvents(ctx, 1, nil)
	if err != nil || len(events) == 0 {
		return events, err
	}
	bctx, cancel := context.WithTimeout(ctx, s.batchWait)
	defer cancel()
	more, err := pc.ReceiveEvents(bctx, receiveBatch-1, nil)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		// The first event is kept; the next receive reports the failure.
		s.log.Debug("eventhubs receive", "source", s.name, "err", err)
	}
	return append(events, more...), nil
}

// handle turns one event into an envelope and hands it over. It returns
// false when ctx ended first.
func (s *ehSource) handle(ctx context.Context, p string, wm *watermark, ev *azeventhubs.ReceivedEventData, out chan<- *envelope.Envelope) bool {
	body := ev.Body
	if body == nil && ev.RawAMQPMessage != nil {
		b := ev.RawAMQPMessage.Body
		body = amqpBody(b.Data, b.Value, b.Sequence)
	}
	tracked := wm.add(ev.SequenceNumber)
	if int64(len(body)) > s.opts.maxBytes {
		s.log.Warn("eventhubs event over max_body_bytes; skipping it", "source", s.name, "partition", p,
			"sequence_number", ev.SequenceNumber, "bytes", len(body), "max_body_bytes", s.opts.maxBytes)
		wm.settle(tracked, true)
		return true
	}
	env := s.envelope(p, ev, body)
	seq := ev.SequenceNumber
	plugin.OnSettle(env, s.opts.ack, func(_ context.Context, final bool) {
		if wm.settle(tracked, final) {
			s.log.Warn("eventhubs event not delivered; the partition's checkpoint stays before it, so it is read again after a restart",
				"source", s.name, "partition", p, "sequence_number", seq, "id", env.ID)
		}
	})
	select {
	case out <- env:
		return true
	case <-ctx.Done():
		wm.settle(tracked, false)
		return false
	}
}

// envelope maps an event: ID from MessageId (else Idempotency-Key, else
// <partition>-<sequence number>), the properties as headers, the hub as
// subject, the content type, and the eventhubs.* attributes.
func (s *ehSource) envelope(p string, ev *azeventhubs.ReceivedEventData, body []byte) *envelope.Envelope {
	env := envelope.New(s.name, kindEventHubs, body)
	switch key := idempotencyFrom(ev.Properties); {
	case ev.MessageID != nil && *ev.MessageID != "":
		env.ID = *ev.MessageID
	case key != "":
		env.ID = key
		env.Attributes["idempotency_key"] = key
	default:
		env.ID = p + "-" + strconv.FormatInt(ev.SequenceNumber, 10)
	}
	env.Headers = headersFrom(ev.Properties)
	env.Subject = s.opts.hub
	env.ContentType = contentType(ev.ContentType, body)
	a := env.Attributes
	a["eventhubs.partition"] = p
	a["eventhubs.sequence_number"] = ev.SequenceNumber
	a["eventhubs.offset"] = ev.Offset
	a["ordering_key"] = p
	if t, ok := rfc3339(ev.EnqueuedTime); ok {
		a["eventhubs.enqueued_time"] = t
	}
	if ev.PartitionKey != nil && *ev.PartitionKey != "" {
		a["eventhubs.partition_key"] = *ev.PartitionKey
	}
	if ev.CorrelationID != nil {
		if c := stringify(ev.CorrelationID); c != "" {
			a["eventhubs.correlation_id"] = c
		}
	}
	return env
}

// startFlusher writes the checkpoints every flushEvery until Close.
func (s *ehSource) startFlusher() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flushStop != nil || s.closed {
		return
	}
	s.flushStop, s.flushDone = make(chan struct{}), make(chan struct{})
	go func(stop <-chan struct{}, done chan<- struct{}) {
		defer close(done)
		t := time.NewTicker(s.flushEvery)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
			}
			ctx, cancel := context.WithTimeout(context.Background(), defaultFlushTimeout)
			s.flush(ctx, false)
			cancel()
		}
	}(s.flushStop, s.flushDone)
}

// flush writes the checkpoints that moved. With final (Close), it writes
// each partition's newest checkpoint exactly. A released reader's entry is
// dropped once its events are all settled and written as far as they can
// be; the entries of readers stopped with the source wait for Close. It
// reports whether every write succeeded.
func (s *ehSource) flush(ctx context.Context, final bool) bool {
	s.mu.Lock()
	entries := make([]*cpEntry, 0, len(s.entries))
	newest := map[string]*cpEntry{}
	for en := range s.entries {
		entries = append(entries, en)
		if cur, ok := newest[en.partition]; !ok || en.cp.gen > cur.cp.gen {
			newest[en.partition] = en
		}
	}
	s.mu.Unlock()
	ok := true
	for _, en := range entries {
		retired := en.retired.Load()
		if retired {
			en.cp.fixed = true
		}
		idle := en.wm.idle() // before value: a later settle is caught next time
		v := en.wm.value()
		err := en.cp.commit(ctx, v, final && newest[en.partition] == en)
		switch {
		case err != nil && !en.failing:
			en.failing = true
			s.log.Warn("eventhubs checkpoint write failed; retrying", "source", s.name, "partition", en.partition, "err", err)
		case err == nil && en.failing:
			en.failing = false
			s.log.Info("eventhubs checkpoint write recovered", "source", s.name, "partition", en.partition)
		}
		ok = ok && err == nil
		if retired && idle && err == nil && en.released.Load() && (v-en.cp.committed <= 1 || en.cp.broken) {
			s.mu.Lock()
			delete(s.entries, en)
			s.mu.Unlock()
		}
	}
	return ok
}

// Close writes the last checkpoints and closes the connection. The engine
// calls it once the events in flight are settled.
func (s *ehSource) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	stop, done := s.flushStop, s.flushDone
	s.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
		for range 3 {
			if s.flush(ctx, true) || ctx.Err() != nil {
				break
			}
		}
	}
	return s.consumer.Close(ctx)
}
