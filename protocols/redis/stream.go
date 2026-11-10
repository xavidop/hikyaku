package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings and sizes of a stream source; tests shorten the timings.
const (
	// defaultBlock is how long one XREADGROUP waits for new entries.
	defaultBlock = 5 * time.Second
	// defaultCallTimeout bounds every other call: XGROUP, XACK, XCLAIM,
	// XAUTOCLAIM, XPENDING.
	defaultCallTimeout = 5 * time.Second
	// maxAckBatch is the most IDs one XACK carries.
	maxAckBatch = 512
	// maxClaimBatch is the most IDs one XCLAIM refresh carries.
	maxClaimBatch = 512
	// maxClaimRounds bounds the XAUTOCLAIM calls of one reclaim pass.
	maxClaimRounds = 100
)

// StreamSource reads a Redis stream as one consumer of a consumer group.
type StreamSource struct {
	name  string
	opts  sourceOptions
	log   *slog.Logger
	nodes []*node
	rnd   func() float64

	block       time.Duration
	callTimeout time.Duration
	tick        time.Duration // reclaim period: half of ack_wait

	life     context.Context // ends at Close
	stopLife context.CancelFunc
	freed    chan struct{} // a prefetch slot was freed
	ackWake  chan struct{} // an ack was queued, or Close wants the queue flushed
	ackDone  chan struct{} // closed once the ack loop returned
	reclaim  sync.WaitGroup
	// resumed: this consumer's own pending entries are read, so the reclaim
	// pass may claim idle entries without racing that read.
	resumed atomic.Bool

	mu         sync.Mutex
	cur        int // the node in use
	status     error
	inflight   map[string]struct{} // entries handed over and not settled, or settled and not yet acknowledged
	acks       []string            // entries waiting for XACK
	ackStopped bool
	started    bool
	closing    bool
}

// node is one server of url and failover_urls.
type node struct {
	shown  string
	cmd    *goredis.Client // group setup, acks, claims, health
	reader *goredis.Client // XREADGROUP; closed as soon as the source stops
}

func newStreamSource(spec plugin.Spec, opts sourceOptions) *StreamSource {
	s := &StreamSource{
		name: spec.Name, opts: opts, log: spec.Env.Log(), rnd: rand.Float64,
		block: defaultBlock, callTimeout: defaultCallTimeout, tick: max(opts.ackWait/2, time.Millisecond),
		freed: make(chan struct{}, 1), ackWake: make(chan struct{}, 1), ackDone: make(chan struct{}),
		inflight: map[string]struct{}{},
	}
	s.life, s.stopLife = context.WithCancel(context.Background())
	for _, sv := range opts.servers {
		s.nodes = append(s.nodes, &node{shown: sv.shown, cmd: sv.client(), reader: sv.client()})
	}
	return s
}

// Name is the source name.
func (s *StreamSource) Name() string { return s.name }

// Health is the read loop's error while it fails, else a PING.
func (s *StreamSource) Health(ctx context.Context) error {
	s.mu.Lock()
	st, n, closing := s.status, s.nodes[s.cur], s.closing
	s.mu.Unlock()
	switch {
	case closing:
		return errClosed
	case st != nil:
		return st
	}
	return n.cmd.Ping(ctx).Err()
}

// Start reads the stream until ctx ends: first the entries this consumer
// left pending, then new ones, while a background pass reclaims idle
// entries. It returns an error only when it gives up, after
// reconnect.max_attempts failed attempts in a row.
func (s *StreamSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	s.mu.Lock()
	if s.started || s.closing {
		s.mu.Unlock()
		return fmt.Errorf("source %s: already started or closed", s.name)
	}
	s.started = true
	s.mu.Unlock()
	go s.ackLoop()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopOnClose := context.AfterFunc(s.life, cancel)
	defer stopOnClose()
	// A blocked XREADGROUP only returns once its BLOCK runs out; closing the
	// readers cuts it short when the source stops.
	stopReaders := context.AfterFunc(ctx, s.closeReaders)
	defer stopReaders()

	s.reclaim.Add(1)
	go s.reclaimLoop(ctx, out)
	err := s.loop(ctx, out)
	cancel()
	s.reclaim.Wait()
	if err != nil {
		s.setStatus(err)
		return err
	}
	s.setStatus(errStopped)
	return nil
}

// loop reads until ctx ends or the reconnect policy gives up. Every
// failure re-runs the group setup, which recreates a group that a Redis
// restart lost.
func (s *StreamSource) loop(ctx context.Context, out chan<- *envelope.Envelope) error {
	r := retrier{rc: s.opts.reconnect, rnd: s.rnd}
	cursor := "0" // this consumer's pending entries come first; "" once read
	ready := false
	for ctx.Err() == nil {
		n := s.node()
		var err error
		switch {
		case !ready:
			if err = s.ensureGroup(ctx, n); err == nil {
				ready = true
				s.setStatus(nil)
				s.log.Info("redis source reading", "source", s.name, "url", n.shown,
					"stream", s.opts.stream, "group", s.opts.group, "consumer", s.opts.consumer)
				continue
			}
		case cursor != "":
			if cursor, err = s.readPending(ctx, out, n, cursor); err == nil && cursor == "" {
				s.resumed.Store(true)
			}
		default:
			err = s.readNew(ctx, out, n)
		}
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			r.success()
			s.setStatus(nil)
			continue
		}
		ready = false
		s.setStatus(err)
		if !r.failure() {
			return fmt.Errorf("source %s: giving up after %d failed attempts in a row: %w", s.name, r.failures, err)
		}
		if r.failures > 0 && len(s.nodes) > 1 {
			s.rotate()
		}
		wait := r.wait()
		s.log.Warn("redis source retrying", "source", s.name, "url", s.node().shown, "stream", s.opts.stream,
			"failures", r.failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			return nil
		}
	}
	return nil
}

// ensureGroup creates the consumer group, and the stream with it, unless
// the group exists.
func (s *StreamSource) ensureGroup(ctx context.Context, n *node) error {
	cctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()
	err := n.cmd.XGroupCreateMkStream(cctx, s.opts.stream, s.opts.group, s.opts.start).Err()
	if err != nil && !hasCode(err, "BUSYGROUP") {
		return fmt.Errorf("XGROUP CREATE %s %s: %w", s.opts.stream, s.opts.group, err)
	}
	return nil
}

// readPending reads the next batch of this consumer's own pending entries
// after cursor: those an earlier run read and never acknowledged. It
// returns the cursor to go on from, "" once there are none left.
func (s *StreamSource) readPending(ctx context.Context, out chan<- *envelope.Envelope, n *node, cursor string) (string, error) {
	free := s.waitSlots(ctx)
	if free == 0 {
		return cursor, nil
	}
	res, err := n.reader.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: s.opts.group, Consumer: s.opts.consumer,
		Streams: []string{s.opts.stream, cursor}, Count: int64(free), Block: -1,
	}).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return cursor, fmt.Errorf("XREADGROUP %s (pending entries): %w", s.opts.stream, err)
	}
	msgs := messagesOf(res, s.opts.stream)
	if len(msgs) == 0 {
		return "", nil
	}
	if cursor == "0" {
		s.log.Info("redis source resuming its pending entries", "source", s.name, "stream", s.opts.stream, "consumer", s.opts.consumer)
	}
	admitted := s.admit(msgs)
	counts := s.deliveries(ctx, n, admitted)
	if !s.handOver(ctx, out, admitted, func(id string) int64 { return counts[id] }) {
		return cursor, nil
	}
	return msgs[len(msgs)-1].ID, nil
}

// readNew waits up to block for entries never delivered to the group, and
// takes as many as there are free prefetch slots.
func (s *StreamSource) readNew(ctx context.Context, out chan<- *envelope.Envelope, n *node) error {
	free := s.waitSlots(ctx)
	if free == 0 {
		return nil
	}
	res, err := n.reader.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: s.opts.group, Consumer: s.opts.consumer,
		Streams: []string{s.opts.stream, ">"}, Count: int64(free), Block: s.block,
	}).Result()
	if errors.Is(err, goredis.Nil) {
		return nil // nothing new within block
	}
	if err != nil {
		return fmt.Errorf("XREADGROUP %s: %w", s.opts.stream, err)
	}
	s.handOver(ctx, out, s.admit(messagesOf(res, s.opts.stream)), func(string) int64 { return 1 })
	return nil
}

// messagesOf returns the entries of one stream in an XREADGROUP reply.
func messagesOf(res []goredis.XStream, stream string) []goredis.XMessage {
	for _, st := range res {
		if st.Stream == stream {
			return st.Messages
		}
	}
	return nil
}

// admit marks entries in flight before they are handed over, so the
// refresh keeps all of them fresh while the relay applies backpressure. It
// returns those that were not in flight here already.
func (s *StreamSource) admit(msgs []goredis.XMessage) []goredis.XMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	admitted := make([]goredis.XMessage, 0, len(msgs))
	for _, m := range msgs {
		if _, ok := s.inflight[m.ID]; !ok {
			s.inflight[m.ID] = struct{}{}
			admitted = append(admitted, m)
		}
	}
	return admitted
}

// handOver emits admitted entries in order. When ctx ends first it
// releases the ones not handed over (they stay pending) and returns false.
func (s *StreamSource) handOver(ctx context.Context, out chan<- *envelope.Envelope, admitted []goredis.XMessage, deliveries func(id string) int64) bool {
	for i, m := range admitted {
		if !s.emit(ctx, out, m, deliveries(m.ID)) {
			ids := make([]string, 0, len(admitted)-i)
			for _, rest := range admitted[i:] {
				ids = append(ids, rest.ID)
			}
			s.release(ids...)
			return false
		}
	}
	return true
}

// emit hands one admitted entry to the relay and wires its settlement:
// final outcomes queue an XACK, a failure leaves the entry pending for the
// reclaim pass. It returns false when ctx ended first.
func (s *StreamSource) emit(ctx context.Context, out chan<- *envelope.Envelope, m goredis.XMessage, deliveries int64) bool {
	if m.Values == nil {
		// A pending entry deleted since (XDEL, MAXLEN) comes back without
		// fields: there is nothing left to deliver.
		s.log.Warn("redis stream entry no longer exists; acknowledging it", "source", s.name,
			"stream", s.opts.stream, "entry_id", m.ID)
		s.ack(m.ID)
		return true
	}
	env := s.newEnvelope(m, deliveries)
	if int64(len(env.Body)) > s.opts.maxBytes {
		// Redelivering it would only fail again, forever.
		s.log.Warn("redis stream entry is over max_body_bytes; acknowledging it without delivery", "source", s.name,
			"stream", s.opts.stream, "entry_id", m.ID, "bytes", len(env.Body), "max_body_bytes", s.opts.maxBytes)
		s.ack(m.ID)
		return true
	}
	id := m.ID
	plugin.OnSettle(env, s.opts.ack, func(_ context.Context, final bool) {
		if final {
			s.ack(id)
			return
		}
		s.release(id)
	})
	select {
	case out <- env:
		return true
	case <-ctx.Done():
		return false
	}
}

// newEnvelope turns a stream entry into an envelope.
func (s *StreamSource) newEnvelope(m goredis.XMessage, deliveries int64) *envelope.Envelope {
	e := decodeEntry(s.opts.stream, m.ID, m.Values)
	env := envelope.New(s.name, kind, e.body)
	env.ID, env.Subject, env.ContentType, env.Headers = e.id, e.subject, e.contentType, e.headers
	env.Attributes["redis.stream"] = s.opts.stream
	env.Attributes["redis.entry_id"] = m.ID
	env.Attributes["redis.group"] = s.opts.group
	env.Attributes["redis.consumer"] = s.opts.consumer
	if deliveries > 0 {
		env.Attributes["redis.deliveries"] = deliveries
	}
	env.Attributes["ordering_key"] = s.opts.stream
	if sc := traceOf(env); sc.IsValid() {
		env.Trace = sc
	}
	return env
}

// deliveries looks up how many times each entry was delivered (XPENDING),
// this delivery included. A failed lookup returns nothing: the entries go
// out without redis.deliveries.
func (s *StreamSource) deliveries(ctx context.Context, n *node, msgs []goredis.XMessage) map[string]int64 {
	if len(msgs) == 0 {
		return nil
	}
	lo, hi := msgs[0].ID, msgs[0].ID
	for _, m := range msgs[1:] {
		if idLess(m.ID, lo) {
			lo = m.ID
		}
		if idLess(hi, m.ID) {
			hi = m.ID
		}
	}
	cctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()
	pend, err := n.cmd.XPendingExt(cctx, &goredis.XPendingExtArgs{
		Stream: s.opts.stream, Group: s.opts.group, Consumer: s.opts.consumer,
		Start: lo, End: hi, Count: int64(len(msgs) + s.opts.prefetch),
	}).Result()
	if err != nil {
		s.log.Debug("redis source could not read delivery counts", "source", s.name, "stream", s.opts.stream, "err", err)
		return nil
	}
	counts := make(map[string]int64, len(pend))
	for _, p := range pend {
		counts[p.ID] = p.RetryCount
	}
	return counts
}

// reclaimLoop runs every tick (half of ack_wait) until ctx ends: it
// refreshes the entries in flight, then, once this consumer's own pending
// entries are read, reclaims idle ones.
func (s *StreamSource) reclaimLoop(ctx context.Context, out chan<- *envelope.Envelope) {
	defer s.reclaim.Done()
	t := time.NewTicker(s.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n := s.node()
		if err := s.refresh(ctx, n); err != nil && ctx.Err() == nil {
			s.log.Debug("redis source could not refresh its entries in flight", "source", s.name, "stream", s.opts.stream, "err", err)
		}
		if !s.resumed.Load() {
			continue
		}
		if err := s.claim(ctx, out, n); err != nil && ctx.Err() == nil {
			s.log.Warn("redis source could not reclaim idle entries", "source", s.name, "stream", s.opts.stream, "err", err)
		}
	}
}

// refresh resets the idle time of this consumer's entries in flight
// (XCLAIM to itself, JUSTID, which leaves the delivery count alone), so no
// reclaim pass, here or in another consumer, takes an entry still being
// delivered.
func (s *StreamSource) refresh(ctx context.Context, n *node) error {
	s.mu.Lock()
	ids := slices.Sorted(maps.Keys(s.inflight))
	s.mu.Unlock()
	for chunk := range slices.Chunk(ids, maxClaimBatch) {
		cctx, cancel := context.WithTimeout(ctx, s.callTimeout)
		err := n.cmd.XClaimJustID(cctx, &goredis.XClaimArgs{
			Stream: s.opts.stream, Group: s.opts.group, Consumer: s.opts.consumer, Messages: chunk,
		}).Err()
		cancel()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return err
		}
	}
	return nil
}

// claim takes the group's entries idle longer than ack_wait (XAUTOCLAIM),
// as many as there are free prefetch slots, and delivers them again: what
// this consumer failed, and what dead consumers left pending.
func (s *StreamSource) claim(ctx context.Context, out chan<- *envelope.Envelope, n *node) error {
	cursor := "0-0"
	for range maxClaimRounds {
		free := s.freeSlots()
		if free <= 0 {
			return nil
		}
		cctx, cancel := context.WithTimeout(ctx, s.callTimeout)
		msgs, next, err := n.cmd.XAutoClaim(cctx, &goredis.XAutoClaimArgs{
			Stream: s.opts.stream, Group: s.opts.group, Consumer: s.opts.consumer,
			MinIdle: s.opts.ackWait, Start: cursor, Count: int64(free),
		}).Result()
		cancel()
		if err != nil {
			return fmt.Errorf("XAUTOCLAIM %s %s: %w", s.opts.stream, s.opts.group, err)
		}
		// An entry in flight here is still being delivered: the claim only
		// refreshed it.
		if fresh := s.admit(msgs); len(fresh) > 0 {
			s.log.Info("redis source redelivering entries idle longer than ack_wait", "source", s.name,
				"stream", s.opts.stream, "entries", len(fresh), "ack_wait", s.opts.ackWait.String())
			counts := s.deliveries(ctx, n, fresh)
			if !s.handOver(ctx, out, fresh, func(id string) int64 { return counts[id] }) {
				return nil
			}
		}
		if next == "" || next == "0-0" {
			return nil
		}
		cursor = next
	}
	return nil
}

// ackLoop acknowledges settled entries in batches (XACK), retrying with the
// reconnect backoff while Redis is unreachable. An entry stays in flight
// until its XACK succeeds, so no reclaim pass redelivers it meanwhile. The
// loop ends once Close flushed the queue, or when Close gives up.
func (s *StreamSource) ackLoop() {
	defer close(s.ackDone)
	r := retrier{rc: s.opts.reconnect, rnd: s.rnd}
	for {
		s.mu.Lock()
		batch := slices.Clone(s.acks[:min(len(s.acks), maxAckBatch)])
		closing := s.closing
		s.mu.Unlock()
		if len(batch) == 0 {
			if closing {
				s.stopAcks()
				return
			}
			select {
			case <-s.ackWake:
			case <-s.life.Done():
				s.stopAcks()
				return
			}
			continue
		}
		// NOGROUP: the group is gone (a restart lost it), and the entries
		// with it; there is nothing left to acknowledge.
		if err := s.xack(batch); err != nil && !hasCode(err, "NOGROUP") {
			if s.life.Err() != nil {
				s.stopAcks()
				return
			}
			r.failure()
			wait := r.wait()
			s.log.Warn("redis XACK failed; retrying", "source", s.name, "stream", s.opts.stream,
				"entries", len(batch), "wait", wait.String(), "err", err)
			if !backoff.Sleep(s.life, wait) {
				s.stopAcks()
				return
			}
			continue
		}
		r.success()
		s.mu.Lock()
		s.acks = s.acks[len(batch):]
		for _, id := range batch {
			delete(s.inflight, id)
		}
		s.mu.Unlock()
		notify(s.freed)
	}
}

func (s *StreamSource) xack(ids []string) error {
	ctx, cancel := context.WithTimeout(s.life, s.callTimeout)
	defer cancel()
	return s.node().cmd.XAck(ctx, s.opts.stream, s.opts.group, ids...).Err()
}

// stopAcks ends acknowledging: what is still queued stays pending in Redis
// (it is redelivered), and so do entries settled from now on.
func (s *StreamSource) stopAcks() {
	s.mu.Lock()
	for _, id := range s.acks {
		delete(s.inflight, id)
	}
	s.acks, s.ackStopped = nil, true
	s.mu.Unlock()
	notify(s.freed)
}

// Close flushes the acknowledgements still queued, within ctx, then closes
// the connections. The engine calls it once every message in flight is
// settled. An entry left unacknowledged stays pending: this consumer's next
// run reads it again, or another consumer reclaims it after ack_wait.
func (s *StreamSource) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	started := s.started
	s.mu.Unlock()
	notify(s.ackWake)
	var err error
	if started {
		select {
		case <-s.ackDone:
		case <-ctx.Done():
			s.mu.Lock()
			queued := len(s.acks)
			s.mu.Unlock()
			err = fmt.Errorf("source %s: %d acknowledgements not sent, their entries stay pending: %w", s.name, queued, ctx.Err())
		}
	}
	s.stopLife()
	if started {
		<-s.ackDone
	}
	for _, n := range s.nodes {
		_ = n.reader.Close()
		_ = n.cmd.Close()
	}
	return err
}

// closeReaders cuts the blocking reads short.
func (s *StreamSource) closeReaders() {
	for _, n := range s.nodes {
		_ = n.reader.Close()
	}
}

// release forgets entries without acknowledging them: they stay pending.
func (s *StreamSource) release(ids ...string) {
	s.mu.Lock()
	for _, id := range ids {
		delete(s.inflight, id)
	}
	s.mu.Unlock()
	notify(s.freed)
}

// ack queues an entry for XACK; it stays in flight until the XACK is done.
func (s *StreamSource) ack(id string) {
	s.mu.Lock()
	if s.ackStopped {
		delete(s.inflight, id)
		s.mu.Unlock()
		notify(s.freed)
		return
	}
	s.acks = append(s.acks, id)
	s.mu.Unlock()
	notify(s.ackWake)
}

// freeSlots is how many more entries may be in flight (prefetch).
func (s *StreamSource) freeSlots() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opts.prefetch - len(s.inflight)
}

// waitSlots waits for a free prefetch slot and returns how many are free,
// or 0 when ctx ended first.
func (s *StreamSource) waitSlots(ctx context.Context) int {
	for {
		if free := s.freeSlots(); free > 0 {
			return free
		}
		select {
		case <-s.freed:
		case <-ctx.Done():
			return 0
		}
	}
}

func (s *StreamSource) node() *node {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodes[s.cur]
}

func (s *StreamSource) rotate() {
	s.mu.Lock()
	s.cur = (s.cur + 1) % len(s.nodes)
	s.mu.Unlock()
}

func (s *StreamSource) setStatus(err error) {
	s.mu.Lock()
	s.status = err
	s.mu.Unlock()
}

// notify wakes whoever waits on ch, without blocking.
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
