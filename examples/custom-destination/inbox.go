package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// InboxOptions are the settings of an inbox source, under options: in the
// spec.
type InboxOptions struct {
	Dir      string `yaml:"dir" jsonschema:"required" jsonschema_description:"Directory to watch (created if missing). Each file is one message."`
	Pattern  string `yaml:"pattern,omitempty" jsonschema_description:"Glob of the file names to take (default *.json)."`
	Interval string `yaml:"interval,omitempty" jsonschema:"pattern=^[0-9.]+(ns|us|µs|ms|s|m|h)$" jsonschema_description:"How often the directory is listed, a duration (default 1s)."`
}

// Inbox turns the files dropped in a directory into messages, one per file.
// A file stays until the relay settles its message: it is removed once the
// relay is done with it (delivered, dead-lettered, unrouted or filtered),
// and sent again, with the same ID, when it failed. Write each file under
// another name and rename it into the directory, so the source never reads
// half of one.
type Inbox struct {
	name     string
	dir      string
	pattern  string
	interval time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	ids     map[string]string // file → message ID, until the file is removed
	pending map[string]bool   // sent, not settled yet
}

func newInbox(_ context.Context, spec plugin.Spec) (plugin.Source, error) {
	var o InboxOptions
	if err := spec.Decode(&o); err != nil {
		return nil, err
	}
	if o.Dir == "" {
		return nil, fmt.Errorf("inbox %s: options.dir is required", spec.Name)
	}
	if o.Pattern == "" {
		o.Pattern = "*.json"
	}
	if _, err := filepath.Match(o.Pattern, ""); err != nil {
		return nil, fmt.Errorf("inbox %s: options.pattern: %w", spec.Name, err)
	}
	interval := time.Second
	if o.Interval != "" {
		d, err := time.ParseDuration(o.Interval)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("inbox %s: options.interval %q is not a positive duration", spec.Name, o.Interval)
		}
		interval = d
	}
	return &Inbox{
		name: spec.Name, dir: o.Dir, pattern: o.Pattern, interval: interval, log: spec.Env.Log(),
		ids: map[string]string{}, pending: map[string]bool{},
	}, nil
}

// Name is the source name.
func (s *Inbox) Name() string { return s.name }

// Start lists the directory every interval and sends each file that is not
// in flight, until ctx ends.
func (s *Inbox) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return fmt.Errorf("inbox %s: %w", s.name, err)
	}
	tick := time.NewTicker(s.interval)
	defer tick.Stop()
	for {
		for _, file := range s.list() {
			env := s.claim(file)
			if env == nil {
				continue
			}
			select {
			case out <- env:
			case <-ctx.Done():
				s.release(file) // never handed over: sent again next time
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// list is the directory's files that match the pattern, by name.
func (s *Inbox) list() []string {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		s.log.Warn("inbox not listed", "dir", s.dir, "error", err)
		return nil
	}
	var files []string
	for _, e := range entries {
		if ok, _ := filepath.Match(s.pattern, e.Name()); ok && e.Type().IsRegular() {
			files = append(files, e.Name())
		}
	}
	return files
}

// claim reads a file that is not in flight into a message; nil when it is
// in flight or gone.
func (s *Inbox) claim(file string) *envelope.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending[file] {
		return nil
	}
	body, err := os.ReadFile(filepath.Join(s.dir, file))
	if err != nil {
		return nil // removed since the listing
	}
	id, ok := s.ids[file]
	if !ok {
		id = envelope.NewID()
		s.ids[file] = id
	}
	s.pending[file] = true
	env := envelope.New(s.name, "inbox", body)
	env.ID = id // the same on every send of the file: the idempotency key
	env.Subject = file
	if strings.HasSuffix(file, ".json") {
		env.ContentType = "application/json"
	}
	env.SetAttribute("inbox.file", file)
	// The relay's outcome comes back once per message. The inbox settles on
	// delivery whatever the source's ack: says: a file goes only once the
	// relay is done with it.
	plugin.OnSettle(env, "delivered", func(_ context.Context, final bool) { s.settled(file, final) })
	return env
}

// release puts back a file whose message was never handed over.
func (s *Inbox) release(file string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, file)
}

// settled removes a file the relay is done with; a failed one is sent again
// at the next listing.
func (s *Inbox) settled(file string, final bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, file)
	if !final {
		s.log.Debug("inbox message failed; sending it again", "file", file)
		return
	}
	delete(s.ids, file)
	// Under the lock, so a listing cannot claim the file again first.
	if err := os.Remove(filepath.Join(s.dir, file)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		s.log.Warn("inbox file not removed", "file", file, "error", err)
	}
}

// Health checks the directory is there.
func (s *Inbox) Health(context.Context) error {
	fi, err := os.Stat(s.dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", s.dir)
	}
	return nil
}
