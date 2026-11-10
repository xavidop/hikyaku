package store

import (
	"context"
	"sync"
	"time"
)

// Recorder appends history records off the delivery path: a bounded queue
// drained by one goroutine, each write retried with backoff. Record never
// blocks; a full queue drops the record and reports it, because history
// makes routing better but must never slow delivery down.
type Recorder struct {
	ch      chan recordJob
	onError func(op string, err error)
	retries int
	backoff time.Duration

	mu      sync.Mutex
	started bool
	closed  bool
	done    chan struct{}
}

type recordJob struct {
	store HistoryStore
	route string
	key   string
	rec   CallRecord
	opts  AppendOptions
}

// RecorderOptions configure a Recorder.
type RecorderOptions struct {
	// Size is the queue length (default 4096).
	Size int
	// Attempts per write (default 3).
	Attempts int
	// Backoff before the second attempt (default 50ms), doubling.
	Backoff time.Duration
	// OnError is told about every failed write ("append") and every dropped
	// record ("queue_full").
	OnError func(op string, err error)
}

// ErrQueueFull reports a record dropped because the queue was full.
var ErrQueueFull = errorString("store: history queue full")

type errorString string

func (e errorString) Error() string { return string(e) }

// NewRecorder returns a recorder; call Start before Record.
func NewRecorder(o RecorderOptions) *Recorder {
	if o.Size <= 0 {
		o.Size = 4096
	}
	if o.Attempts <= 0 {
		o.Attempts = 3
	}
	if o.Backoff <= 0 {
		o.Backoff = 50 * time.Millisecond
	}
	if o.OnError == nil {
		o.OnError = func(string, error) {}
	}
	return &Recorder{ch: make(chan recordJob, o.Size), onError: o.OnError, retries: o.Attempts, backoff: o.Backoff, done: make(chan struct{})}
}

// Start runs the writer until Close.
func (r *Recorder) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return
	}
	r.started = true
	go r.loop()
}

func (r *Recorder) loop() {
	defer close(r.done)
	for j := range r.ch {
		r.write(j)
	}
}

func (r *Recorder) write(j recordJob) {
	wait := r.backoff
	var err error
	for n := 1; n <= r.retries; n++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = j.store.Append(ctx, j.route, j.key, j.rec, j.opts)
		cancel()
		if err == nil {
			return
		}
		if n < r.retries {
			time.Sleep(wait)
			wait *= 2
		}
	}
	r.onError("append", err)
}

// Record queues a record; false when it was dropped (queue full or closed).
func (r *Recorder) Record(s HistoryStore, route, key string, rec CallRecord, o AppendOptions) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || s == nil {
		return false
	}
	select {
	case r.ch <- recordJob{store: s, route: route, key: key, rec: rec, opts: o}:
		return true
	default:
		r.onError("queue_full", ErrQueueFull)
		return false
	}
}

// Close stops taking records and waits (until ctx ends) for the queued ones
// to be written.
func (r *Recorder) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.ch)
	started := r.started
	r.mu.Unlock()
	if !started {
		go r.loop()
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
