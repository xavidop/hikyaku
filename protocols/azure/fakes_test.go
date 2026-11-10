package azure

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// --- Service Bus ---

// fakeSB is a Service Bus namespace: a receiver, sessions to accept and a
// sender.
type fakeSB struct {
	recv     *fakeReceiver
	sessions chan *fakeSession
	sender   *fakeSender
	closed   atomic.Bool
	// acceptWait is how long acceptNextSession waits before CodeTimeout.
	acceptWait time.Duration
}

func newFakeSB() *fakeSB {
	return &fakeSB{recv: newFakeReceiver(), sessions: make(chan *fakeSession, 16), sender: &fakeSender{maxPerBatch: 1000},
		acceptWait: 20 * time.Millisecond}
}

func (f *fakeSB) newReceiver(sbEntity) (sbReceiver, error) { return f.recv, nil }

func (f *fakeSB) acceptNextSession(ctx context.Context, _ sbEntity) (sbSessionReceiver, error) {
	select {
	case s := <-f.sessions:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(f.acceptWait):
		return nil, &azservicebus.Error{Code: azservicebus.CodeTimeout}
	}
}

func (f *fakeSB) newSender(string) (sbSender, error) { return f.sender, nil }
func (f *fakeSB) Close(context.Context) error        { f.closed.Store(true); return nil }

// settlements records what a receiver did with messages.
type settlements struct {
	mu        sync.Mutex
	completed []string
	abandoned []string
	settleErr error
}

func (s *settlements) CompleteMessage(_ context.Context, m *azservicebus.ReceivedMessage, _ *azservicebus.CompleteMessageOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed = append(s.completed, m.MessageID)
	return s.settleErr
}

func (s *settlements) AbandonMessage(_ context.Context, m *azservicebus.ReceivedMessage, _ *azservicebus.AbandonMessageOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abandoned = append(s.abandoned, m.MessageID)
	return s.settleErr
}

func (s *settlements) done() (completed, abandoned []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.completed), slices.Clone(s.abandoned)
}

// fakeReceiver serves messages pushed on msgs; errors pushed on errs come
// first.
type fakeReceiver struct {
	settlements
	msgs chan *azservicebus.ReceivedMessage
	errs chan error

	mu       sync.Mutex
	asked    []int
	renewals map[string]int
	renewErr error
	lockFor  time.Duration
	peekErr  error
	closed   bool
}

func newFakeReceiver() *fakeReceiver {
	return &fakeReceiver{msgs: make(chan *azservicebus.ReceivedMessage, 100), errs: make(chan error, 10),
		renewals: map[string]int{}, lockFor: time.Minute}
}

func (r *fakeReceiver) ReceiveMessages(ctx context.Context, n int, _ *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	r.mu.Lock()
	r.asked = append(r.asked, n)
	r.mu.Unlock()
	return receive(ctx, n, r.msgs, r.errs)
}

func receive(ctx context.Context, n int, msgs chan *azservicebus.ReceivedMessage, errs chan error) ([]*azservicebus.ReceivedMessage, error) {
	select {
	case err := <-errs:
		return nil, err
	default:
	}
	var out []*azservicebus.ReceivedMessage
	select {
	case m := <-msgs:
		out = append(out, m)
	case err := <-errs:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	for len(out) < n {
		select {
		case m := <-msgs:
			out = append(out, m)
		default:
			return out, nil
		}
	}
	return out, nil
}

func (r *fakeReceiver) RenewMessageLock(_ context.Context, m *azservicebus.ReceivedMessage, _ *azservicebus.RenewMessageLockOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.renewals[m.MessageID]++
	if r.renewErr != nil {
		return r.renewErr
	}
	until := time.Now().Add(r.lockFor)
	m.LockedUntil = &until
	return nil
}

func (r *fakeReceiver) renewed(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.renewals[id]
}

func (r *fakeReceiver) askedFor() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.asked)
}

func (r *fakeReceiver) PeekMessages(context.Context, int, *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return nil, r.peekErr
}

func (r *fakeReceiver) Close(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *fakeReceiver) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// fakeSession is one locked session.
type fakeSession struct {
	settlements
	id   string
	msgs chan *azservicebus.ReceivedMessage
	errs chan error

	mu       sync.Mutex
	until    time.Time
	lockFor  time.Duration
	renewals int
	renewErr error
	closed   bool
}

func newFakeSession(id string, lockFor time.Duration, msgs ...*azservicebus.ReceivedMessage) *fakeSession {
	s := &fakeSession{id: id, msgs: make(chan *azservicebus.ReceivedMessage, 100), errs: make(chan error, 10),
		until: time.Now().Add(lockFor), lockFor: lockFor}
	for _, m := range msgs {
		m.SessionID = &s.id
		s.msgs <- m
	}
	return s
}

func (s *fakeSession) ReceiveMessages(ctx context.Context, n int, _ *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	return receive(ctx, n, s.msgs, s.errs)
}

func (s *fakeSession) SessionID() string { return s.id }

func (s *fakeSession) LockedUntil() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.until
}

func (s *fakeSession) RenewSessionLock(context.Context, *azservicebus.RenewSessionLockOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewals++
	if s.renewErr != nil {
		return s.renewErr
	}
	s.until = time.Now().Add(s.lockFor)
	return nil
}

func (s *fakeSession) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeSession) state() (renewals int, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.renewals, s.closed
}

// fakeSender records what it sends; a batch holds maxPerBatch messages.
type fakeSender struct {
	mu          sync.Mutex
	sent        []*azservicebus.Message
	batches     [][]*azservicebus.Message
	maxPerBatch int
	sendErr     error
	batchErr    error
	closed      int
}

type fakeSBBatch struct {
	max  int
	msgs []*azservicebus.Message
}

func (b *fakeSBBatch) AddMessage(m *azservicebus.Message, _ *azservicebus.AddMessageOptions) error {
	if len(b.msgs) >= b.max || len(m.Body) > 1000 {
		return azservicebus.ErrMessageTooLarge
	}
	b.msgs = append(b.msgs, m)
	return nil
}

func (b *fakeSBBatch) NumMessages() int32 { return int32(len(b.msgs)) }

func (s *fakeSender) SendMessage(_ context.Context, m *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, m)
	return nil
}

func (s *fakeSender) newBatch(context.Context) (sbBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.batchErr != nil {
		return nil, s.batchErr
	}
	return &fakeSBBatch{max: s.maxPerBatch}, nil
}

func (s *fakeSender) sendBatch(_ context.Context, b sbBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	msgs := b.(*fakeSBBatch).msgs
	s.batches = append(s.batches, msgs)
	s.sent = append(s.sent, msgs...)
	return nil
}

func (s *fakeSender) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed++
	return nil
}

// --- Event Hubs ---

// fakeHub is an event hub: a log of events per partition that partition
// clients read from their start position.
type fakeHub struct {
	mu         sync.Mutex
	partitions []string
	logs       map[string][]*azeventhubs.ReceivedEventData
	wake       chan struct{} // closed and replaced on every append
	opened     []openedPartition
	clients    []*fakePartition
	propsErr   error
	closed     bool
}

type openedPartition struct {
	partition string
	opts      azeventhubs.PartitionClientOptions
}

func newFakeHub(partitions ...string) *fakeHub {
	return &fakeHub{partitions: partitions, logs: map[string][]*azeventhubs.ReceivedEventData{}, wake: make(chan struct{})}
}

// publish appends events with the next sequence numbers.
func (h *fakeHub) publish(partition string, bodies ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, b := range bodies {
		seq := int64(len(h.logs[partition]))
		at := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Second)
		h.logs[partition] = append(h.logs[partition], &azeventhubs.ReceivedEventData{
			EventData:      azeventhubs.EventData{Body: []byte(b)},
			SequenceNumber: seq, Offset: "o" + string(rune('0'+seq%10)), EnqueuedTime: &at,
		})
	}
	close(h.wake)
	h.wake = make(chan struct{})
}

func (h *fakeHub) GetEventHubProperties(context.Context, *azeventhubs.GetEventHubPropertiesOptions) (azeventhubs.EventHubProperties, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return azeventhubs.EventHubProperties{Name: "hub", PartitionIDs: slices.Clone(h.partitions)}, h.propsErr
}

func (h *fakeHub) GetPartitionProperties(_ context.Context, p string, _ *azeventhubs.GetPartitionPropertiesOptions) (azeventhubs.PartitionProperties, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := int64(len(h.logs[p]))
	return azeventhubs.PartitionProperties{PartitionID: p, IsEmpty: n == 0, LastEnqueuedSequenceNumber: n - 1}, h.propsErr
}

func (h *fakeHub) newPartitionClient(p string, o *azeventhubs.PartitionClientOptions) (ehPartition, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.opened = append(h.opened, openedPartition{partition: p, opts: *o})
	next := 0
	switch {
	case o.StartPosition.SequenceNumber != nil:
		next = int(*o.StartPosition.SequenceNumber) + 1
	case o.StartPosition.Earliest != nil:
		next = 0
	default:
		next = len(h.logs[p])
	}
	pc := &fakePartition{hub: h, partition: p, next: next, errs: make(chan error, 4)}
	h.clients = append(h.clients, pc)
	return pc, nil
}

func (h *fakeHub) Close(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	return nil
}

func (h *fakeHub) openings() []openedPartition {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.opened)
}

type fakePartition struct {
	hub       *fakeHub
	partition string
	next      int
	errs      chan error
	closed    atomic.Bool
}

func (p *fakePartition) ReceiveEvents(ctx context.Context, count int, _ *azeventhubs.ReceiveEventsOptions) ([]*azeventhubs.ReceivedEventData, error) {
	for {
		select {
		case err := <-p.errs:
			return nil, err
		default:
		}
		p.hub.mu.Lock()
		log, wake := p.hub.logs[p.partition], p.hub.wake
		p.hub.mu.Unlock()
		if p.next < len(log) {
			end := min(p.next+count, len(log))
			out := slices.Clone(log[p.next:end])
			p.next = end
			return out, nil
		}
		select {
		case <-wake:
		case err := <-p.errs:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *fakePartition) Close(context.Context) error { p.closed.Store(true); return nil }

// fakeProducer records the batches it sends; a batch holds maxPerBatch
// events.
type fakeProducer struct {
	mu          sync.Mutex
	batches     []fakeEHBatch
	maxPerBatch int
	sendErr     error
	propsErr    error
	closed      int
}

type fakeEHBatch struct {
	max    int
	opts   azeventhubs.EventDataBatchOptions
	events []*azeventhubs.EventData
}

func (b *fakeEHBatch) AddEventData(e *azeventhubs.EventData, _ *azeventhubs.AddEventDataOptions) error {
	if len(b.events) >= b.max || len(e.Body) > 1000 {
		return azeventhubs.ErrEventDataTooLarge
	}
	b.events = append(b.events, e)
	return nil
}

func (b *fakeEHBatch) NumEvents() int32 { return int32(len(b.events)) }

func (p *fakeProducer) GetEventHubProperties(context.Context, *azeventhubs.GetEventHubPropertiesOptions) (azeventhubs.EventHubProperties, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return azeventhubs.EventHubProperties{}, p.propsErr
}

func (p *fakeProducer) newBatch(_ context.Context, o *azeventhubs.EventDataBatchOptions) (ehBatch, error) {
	return &fakeEHBatch{max: p.maxPerBatch, opts: *o}, nil
}

func (p *fakeProducer) sendBatch(_ context.Context, b ehBatch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sendErr != nil {
		return p.sendErr
	}
	p.batches = append(p.batches, *b.(*fakeEHBatch))
	return nil
}

func (p *fakeProducer) Close(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed++
	return nil
}
