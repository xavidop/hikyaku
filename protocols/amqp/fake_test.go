package amqp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// The fake broker stands in for RabbitMQ in the unit tests. It follows the
// client's ordering where the protocol relies on it: a closing channel
// notifies its exception before it closes the deliveries and fails the
// pending confirms, and a returned message is notified before its confirm.

// wait is how long the helpers wait for something the code under test
// should do.
const wait = 5 * time.Second

// fakeBroker hands out fake connections.
type fakeBroker struct {
	conns chan *fakeConn

	mu        sync.Mutex
	dials     []string
	configs   []amqp091.Config
	down      map[string]bool // URLs that refuse connections
	failDials int             // the next failDials dials fail
	dialErr   error           // what a failed dial returns (default: connection refused)
	setup     func(*fakeConn) // runs on every new connection
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{conns: make(chan *fakeConn, 64), down: map[string]bool{}}
}

func (b *fakeBroker) dial(ctx context.Context, rawURL string, cfg amqp091.Config) (conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.dials = append(b.dials, rawURL)
	b.configs = append(b.configs, cfg)
	fail := b.down[rawURL] || b.failDials > 0
	if b.failDials > 0 {
		b.failDials--
	}
	dialErr, setup := b.dialErr, b.setup
	b.mu.Unlock()
	if fail {
		if dialErr == nil {
			dialErr = &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return nil, dialErr
	}
	c := &fakeConn{url: rawURL, cfg: cfg, channels: make(chan *fakeChannel, 64)}
	if setup != nil {
		setup(c)
	}
	b.conns <- c
	return c, nil
}

func (b *fakeBroker) setDown(rawURL string, down bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.down[rawURL] = down
}

func (b *fakeBroker) dialed() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.dials...)
}

// nextConn waits for the next connection opened.
func (b *fakeBroker) nextConn(t *testing.T) *fakeConn {
	t.Helper()
	select {
	case c := <-b.conns:
		return c
	case <-time.After(wait):
		t.Fatal("no connection was opened")
		return nil
	}
}

// fakeConn is one connection.
type fakeConn struct {
	url      string
	cfg      amqp091.Config
	channels chan *fakeChannel

	mu      sync.Mutex
	closed  bool
	closes  []chan *amqp091.Error
	blocks  []chan amqp091.Blocking
	chans   []*fakeChannel
	chanErr error              // Channel fails with it
	setup   func(*fakeChannel) // runs on every new channel
}

func (c *fakeConn) Channel() (channel, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, amqp091.ErrClosed
	}
	if c.chanErr != nil {
		err := c.chanErr
		c.mu.Unlock()
		return nil, err
	}
	ch := &fakeChannel{
		conn: c, settled: make(chan settlement, 1024), published: make(chan publishRecord, 1024),
		consuming: make(chan struct{}), pending: map[uint64]*fakeConfirm{}, errs: map[string]error{},
	}
	c.chans = append(c.chans, ch)
	setup := c.setup
	c.mu.Unlock()
	if setup != nil {
		setup(ch)
	}
	c.channels <- ch
	return ch, nil
}

func (c *fakeConn) NotifyClose(r chan *amqp091.Error) chan *amqp091.Error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		close(r)
	} else {
		c.closes = append(c.closes, r)
	}
	return r
}

func (c *fakeConn) NotifyBlocked(r chan amqp091.Blocking) chan amqp091.Blocking {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		close(r)
	} else {
		c.blocks = append(c.blocks, r)
	}
	return r
}

func (c *fakeConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeConn) CloseDeadline(time.Time) error {
	c.shutdown(nil)
	return nil
}

// drop is the broker closing the connection with an exception.
func (c *fakeConn) drop(e *amqp091.Error) { c.shutdown(e) }

func (c *fakeConn) shutdown(e *amqp091.Error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	closes, blocks, chans := c.closes, c.blocks, c.chans
	c.closes, c.blocks = nil, nil
	c.mu.Unlock()
	if e != nil {
		for _, l := range closes {
			select {
			case l <- e:
			default:
			}
		}
	}
	for _, ch := range chans {
		ch.shutdown(e)
	}
	for _, l := range closes {
		close(l)
	}
	for _, l := range blocks {
		close(l)
	}
}

// block is the broker raising (or, with active false, clearing) a
// resource alarm.
func (c *fakeConn) block(t *testing.T, active bool, reason string) {
	t.Helper()
	c.mu.Lock()
	blocks := append([]chan amqp091.Blocking(nil), c.blocks...)
	c.mu.Unlock()
	for _, l := range blocks {
		select {
		case l <- amqp091.Blocking{Active: active, Reason: reason}:
		case <-time.After(wait):
			t.Fatal("nobody reads connection.blocked")
		}
	}
}

// nextChannel waits for the next channel opened on the connection.
func (c *fakeConn) nextChannel(t *testing.T) *fakeChannel {
	t.Helper()
	select {
	case ch := <-c.channels:
		return ch
	case <-time.After(wait):
		t.Fatal("no channel was opened")
		return nil
	}
}

// settlement is one ack or nack.
type settlement struct {
	op       string // ack or nack
	tag      uint64
	multiple bool
	requeue  bool
}

// publishRecord is one publish.
type publishRecord struct {
	exchange  string
	key       string
	mandatory bool
	msg       amqp091.Publishing
}

// publishAction is what the broker does with a publish.
type publishAction struct {
	nack   bool
	hold   bool               // never confirm
	ret    *amqp091.Return    // return the message first (with its properties)
	except *amqp091.Error     // close the channel with this exception
	fn     func(*fakeChannel) // runs before the confirm
}

// fakeChannel is one channel.
type fakeChannel struct {
	conn      *fakeConn
	settled   chan settlement
	published chan publishRecord
	consuming chan struct{} // closed by Consume

	mu         sync.Mutex
	closed     bool
	closes     []chan *amqp091.Error
	returns    []chan amqp091.Return
	calls      []string
	errs       map[string]error // by call; an *amqp091.Error also closes the channel
	deliveries chan amqp091.Delivery
	tag        string
	nextTag    uint64
	confirming bool
	seq        uint64
	pending    map[uint64]*fakeConfirm
	onPublish  func(publishRecord) publishAction
}

// record notes a call, and returns its configured failure.
func (ch *fakeChannel) record(call, op string) error {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return amqp091.ErrClosed
	}
	ch.calls = append(ch.calls, call)
	err := ch.errs[op]
	ch.mu.Unlock()
	if e, ok := err.(*amqp091.Error); ok {
		ch.shutdown(e)
	}
	return err
}

func (ch *fakeChannel) fail(op string, err error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.errs[op] = err
}

func (ch *fakeChannel) callLog() []string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return append([]string(nil), ch.calls...)
}

func (ch *fakeChannel) Qos(prefetchCount, _ int, global bool) error {
	return ch.record(fmt.Sprintf("qos %d global=%v", prefetchCount, global), "qos")
}

func (ch *fakeChannel) Consume(queue, consumer string, autoAck, _, _, _ bool, _ amqp091.Table) (<-chan amqp091.Delivery, error) {
	if err := ch.record(fmt.Sprintf("consume %s %s autoAck=%v", queue, consumer, autoAck), "consume"); err != nil {
		return nil, err
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.deliveries, ch.tag = make(chan amqp091.Delivery, 256), consumer
	close(ch.consuming)
	return ch.deliveries, nil
}

func (ch *fakeChannel) Cancel(consumer string, _ bool) error {
	if err := ch.record("cancel "+consumer, "cancel"); err != nil {
		return err
	}
	ch.mu.Lock()
	d := ch.deliveries
	ch.deliveries = nil
	ch.mu.Unlock()
	if d != nil {
		close(d)
	}
	return nil
}

func (ch *fakeChannel) settle(s settlement) error {
	ch.mu.Lock()
	closed := ch.closed
	ch.mu.Unlock()
	if closed {
		return amqp091.ErrClosed
	}
	ch.settled <- s
	return nil
}

func (ch *fakeChannel) Ack(tag uint64, multiple bool) error {
	return ch.settle(settlement{op: "ack", tag: tag, multiple: multiple})
}

func (ch *fakeChannel) Nack(tag uint64, multiple, requeue bool) error {
	return ch.settle(settlement{op: "nack", tag: tag, multiple: multiple, requeue: requeue})
}

func (ch *fakeChannel) Confirm(bool) error {
	if err := ch.record("confirm", "confirm"); err != nil {
		return err
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.confirming = true
	return nil
}

func (ch *fakeChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, _ bool, msg amqp091.Publishing) (confirmation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := msg.Headers.Validate(); err != nil {
		return nil, err
	}
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return nil, amqp091.ErrClosed
	}
	if !ch.confirming {
		ch.mu.Unlock()
		return nil, errNotConfirming
	}
	ch.seq++
	seq := ch.seq
	dc := &fakeConfirm{done: make(chan struct{})}
	ch.pending[seq] = dc
	on := ch.onPublish
	ch.mu.Unlock()
	rec := publishRecord{exchange: exchange, key: key, mandatory: mandatory, msg: msg}
	ch.published <- rec
	act := publishAction{}
	if on != nil {
		act = on(rec)
	}
	if act.fn != nil {
		act.fn(ch)
	}
	switch {
	case act.except != nil:
		ch.shutdown(act.except)
	case act.hold:
	default:
		if act.ret != nil {
			r := *act.ret
			r.Exchange, r.RoutingKey, r.MessageId, r.Headers, r.Body = exchange, key, msg.MessageId, msg.Headers, msg.Body
			ch.mu.Lock()
			returns := append([]chan amqp091.Return(nil), ch.returns...)
			ch.mu.Unlock()
			for _, l := range returns {
				l <- r
			}
		}
		ch.confirm(seq, !act.nack)
	}
	return dc, nil
}

// confirm acks or nacks publish seq.
func (ch *fakeChannel) confirm(seq uint64, ack bool) {
	ch.mu.Lock()
	dc := ch.pending[seq]
	delete(ch.pending, seq)
	ch.mu.Unlock()
	if dc != nil {
		dc.resolve(ack)
	}
}

func (ch *fakeChannel) NotifyClose(r chan *amqp091.Error) chan *amqp091.Error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		close(r)
	} else {
		ch.closes = append(ch.closes, r)
	}
	return r
}

func (ch *fakeChannel) NotifyReturn(r chan amqp091.Return) chan amqp091.Return {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		close(r)
	} else {
		ch.returns = append(ch.returns, r)
	}
	return r
}

func (ch *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	err := ch.record(fmt.Sprintf("queue.declare %s durable=%v autoDelete=%v exclusive=%v", name, durable, autoDelete, exclusive), "queue.declare")
	return amqp091.Queue{Name: name}, err
}

func (ch *fakeChannel) QueueDeclarePassive(name string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	err := ch.record("queue.declare.passive "+name, "queue.declare.passive")
	return amqp091.Queue{Name: name}, err
}

func (ch *fakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, _ bool, _ amqp091.Table) error {
	return ch.record(fmt.Sprintf("exchange.declare %s %s durable=%v autoDelete=%v internal=%v", name, kind, durable, autoDelete, internal), "exchange.declare")
}

func (ch *fakeChannel) QueueBind(name, key, exchange string, _ bool, _ amqp091.Table) error {
	return ch.record(fmt.Sprintf("queue.bind %s %s %s", name, exchange, key), "queue.bind")
}

func (ch *fakeChannel) IsClosed() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.closed
}

func (ch *fakeChannel) Close() error {
	ch.shutdown(nil)
	return nil
}

// shutdown closes the channel: the exception is notified first, then the
// deliveries and the listeners close, and the pending confirms fail.
func (ch *fakeChannel) shutdown(e *amqp091.Error) {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return
	}
	ch.closed = true
	closes, returns, pending, deliveries := ch.closes, ch.returns, ch.pending, ch.deliveries
	ch.closes, ch.returns, ch.pending, ch.deliveries = nil, nil, map[uint64]*fakeConfirm{}, nil
	ch.mu.Unlock()
	if e != nil {
		for _, l := range closes {
			select {
			case l <- e:
			default:
			}
		}
	}
	if deliveries != nil {
		close(deliveries)
	}
	for _, l := range closes {
		close(l)
	}
	for _, l := range returns {
		close(l)
	}
	for _, dc := range pending {
		dc.resolve(false)
	}
}

// awaitConsume waits until the channel consumes.
func (ch *fakeChannel) awaitConsume(t *testing.T) {
	t.Helper()
	select {
	case <-ch.consuming:
	case <-time.After(wait):
		t.Fatal("the channel never consumed")
	}
}

// deliver sends a delivery to the consumer, numbering it when its tag is
// zero, and returns its tag.
func (ch *fakeChannel) deliver(t *testing.T, d amqp091.Delivery) uint64 {
	t.Helper()
	ch.awaitConsume(t)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	require.NotNil(t, ch.deliveries, "the consumer is gone")
	ch.nextTag++
	if d.DeliveryTag == 0 {
		d.DeliveryTag = ch.nextTag
	}
	d.ConsumerTag = ch.tag
	ch.deliveries <- d
	return d.DeliveryTag
}

// nextSettlement waits for the next ack or nack.
func (ch *fakeChannel) nextSettlement(t *testing.T) settlement {
	t.Helper()
	select {
	case s := <-ch.settled:
		return s
	case <-time.After(wait):
		t.Fatal("nothing was acked or nacked")
		return settlement{}
	}
}

// noSettlement checks that nothing is acked or nacked within d.
func (ch *fakeChannel) noSettlement(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case s := <-ch.settled:
		t.Fatalf("unexpected %s of tag %d", s.op, s.tag)
	case <-time.After(d):
	}
}

// nextPublish waits for the next publish.
func (ch *fakeChannel) nextPublish(t *testing.T) publishRecord {
	t.Helper()
	select {
	case p := <-ch.published:
		return p
	case <-time.After(wait):
		t.Fatal("nothing was published")
		return publishRecord{}
	}
}

// fakeConfirm is a publisher confirm.
type fakeConfirm struct {
	once sync.Once
	done chan struct{}
	ack  bool
}

func (c *fakeConfirm) resolve(ack bool) {
	c.once.Do(func() {
		c.ack = ack
		close(c.done)
	})
}

func (c *fakeConfirm) Done() <-chan struct{} { return c.done }

func (c *fakeConfirm) Acked() bool {
	select {
	case <-c.done:
		return c.ack
	default:
		return false
	}
}
