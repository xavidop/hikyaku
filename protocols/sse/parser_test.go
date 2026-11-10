package sse

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parse reads every event of a stream, and what the parser ended with.
func parse(t *testing.T, r io.Reader) ([]Event, *Parser) {
	t.Helper()
	p := NewParser(r, 1<<10)
	var out []Event
	for {
		ev, err := p.Next()
		if errors.Is(err, io.EOF) {
			return out, p
		}
		require.NoError(t, err)
		out = append(out, ev)
	}
}

func ev(id, name, data string) Event { return Event{ID: id, Name: name, Data: []byte(data)} }

func TestParser(t *testing.T) {
	cases := []struct {
		name   string
		stream string
		want   []Event
		lastID string
	}{
		{"one event", "data: hello\n\n", []Event{ev("", "", "hello")}, ""},
		{"data lines join with LF", "data: a\ndata: b\ndata: c\n\n", []Event{ev("", "", "a\nb\nc")}, ""},
		{"event name", "event: update\ndata: x\n\n", []Event{ev("", "update", "x")}, ""},
		{"event name resets after dispatch", "event: a\ndata: 1\n\ndata: 2\n\n", []Event{ev("", "a", "1"), ev("", "", "2")}, ""},
		{"id persists across events", "id: 1\ndata: a\n\ndata: b\n\n", []Event{ev("1", "", "a"), ev("1", "", "b")}, "1"},
		{"empty id resets it", "id: 1\ndata: a\n\nid\ndata: b\n\n", []Event{ev("1", "", "a"), ev("", "", "b")}, ""},
		{"id with NUL is ignored", "id: 1\ndata: a\n\nid: 2\x003\ndata: b\n\n", []Event{ev("1", "", "a"), ev("1", "", "b")}, "1"},
		{"id-only block sets the last id", "id: 7\n\n", nil, "7"},
		{"id needs a blank line to count", "id: 1\ndata: a\n\nid: 9\n", []Event{ev("1", "", "a")}, "1"},
		{"comments are ignored", ": hi\ndata: x\n: there\n\n", []Event{ev("", "", "x")}, ""},
		{"comment-only stream", ": ping\n\n: ping\n\n", nil, ""},
		{"no colon is an empty value", "data\n\n", []Event{ev("", "", "")}, ""},
		{"two empty data lines", "data\ndata\n\n", []Event{ev("", "", "\n")}, ""},
		{"empty data value still dispatches", "data:\n\n", []Event{ev("", "", "")}, ""},
		{"no data is not dispatched", "event: x\n\nid: 3\nevent: y\n\n", nil, "3"},
		{"no space after colon", "data:x\n\n", []Event{ev("", "", "x")}, ""},
		{"only one space is stripped", "data:  x \n\n", []Event{ev("", "", " x ")}, ""},
		{"colon in the value", "data: a:b: c\n\n", []Event{ev("", "", "a:b: c")}, ""},
		{"CRLF", "data: a\r\ndata: b\r\n\r\n", []Event{ev("", "", "a\nb")}, ""},
		{"CR", "data: a\rdata: b\r\r", []Event{ev("", "", "a\nb")}, ""},
		{"mixed endings", "id: 1\r\ndata: a\r\rdata: b\n\r\ndata: c\r\n\n", []Event{ev("1", "", "a"), ev("1", "", "b"), ev("1", "", "c")}, "1"},
		{"unknown fields are ignored", "foo: bar\nData: x\nid : 4\ndata: y\n\n", []Event{ev("", "", "y")}, ""},
		{"unfinished event is dropped", "data: a\n\ndata: b\n", []Event{ev("", "", "a")}, ""},
		{"unfinished line is dropped", "data: a\n\ndata: b", []Event{ev("", "", "a")}, ""},
		{"byte order mark", "\xEF\xBB\xBFdata: x\n\n", []Event{ev("", "", "x")}, ""},
		{"only a leading BOM", "data: \xEF\xBB\xBFx\n\n", []Event{ev("", "", "\xEF\xBB\xBFx")}, ""},
		{"blank lines before the first event", "\n\n\r\ndata: x\n\n", []Event{ev("", "", "x")}, ""},
		{"JSON data", "event: order\nid: o-1\ndata: {\"total\": 3}\n\n", []Event{ev("o-1", "order", `{"total": 3}`)}, "o-1"},
		{"empty stream", "", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, r := range map[string]io.Reader{
				"whole":       strings.NewReader(c.stream),
				"byte a time": iotest.OneByteReader(strings.NewReader(c.stream)),
			} {
				got, p := parse(t, r)
				assert.Equal(t, c.want, got)
				assert.Equal(t, c.lastID, p.LastID())
			}
		})
	}
}

func TestParserRetry(t *testing.T) {
	cases := map[string]struct {
		stream string
		want   time.Duration
		set    bool
	}{
		"milliseconds":        {"retry: 1500\n\n", 1500 * time.Millisecond, true},
		"zero":                {"retry: 0\n", 0, true},
		"latest wins":         {"retry: 10\nretry: 20\n", 20 * time.Millisecond, true},
		"invalid keeps last":  {"retry: 10\nretry: 1.5\nretry: x\nretry: -1\nretry:\nretry: 2 \n", 10 * time.Millisecond, true},
		"decimal":             {"retry: 1.5\n", 0, false},
		"signed":              {"retry: +5\n", 0, false},
		"empty":               {"retry\n", 0, false},
		"overflow is ignored": {"retry: 99999999999999999999\n", 0, false},
	}
	for name, c := range cases {
		_, p := parse(t, strings.NewReader(c.stream))
		d, ok := p.Retry()
		assert.Equal(t, c.set, ok, name)
		assert.Equal(t, c.want, d, name)
	}
	events, _ := parse(t, strings.NewReader("retry: 5\ndata: x\n\n"))
	assert.Equal(t, []Event{ev("", "", "x")}, events, "retry does not make an event")
}

func TestParserSetLastID(t *testing.T) {
	p := NewParser(strings.NewReader("data: a\n\nid: 9\ndata: b\n\n"), 0)
	p.SetLastID("5")
	a, err := p.Next()
	require.NoError(t, err)
	assert.Equal(t, "5", a.ID, "a resumed stream carries the id it resumed from")
	b, err := p.Next()
	require.NoError(t, err)
	assert.Equal(t, "9", b.ID)
	assert.Equal(t, "9", p.LastID())
}

func TestParserDispatchesWithoutReadingAhead(t *testing.T) {
	// On a live stream nothing follows a CR until the next event: the parser
	// must not wait to see whether an LF comes.
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	p := NewParser(pr, 0)
	got := make(chan Event, 2)
	go func() {
		for {
			ev, err := p.Next()
			if err != nil {
				return
			}
			got <- ev
		}
	}()
	for _, step := range []struct{ chunk, want string }{
		{"data: one\r\r", "one"},
		{"\ndata: two\n\n", "two"}, // the LF closes the CR before it, not a line
	} {
		_, err := pw.Write([]byte(step.chunk))
		require.NoError(t, err)
		select {
		case e := <-got:
			assert.Equal(t, step.want, string(e.Data))
		case <-time.After(2 * time.Second):
			t.Fatalf("no event after %q", step.chunk)
		}
	}
	assert.Empty(t, got, "no event came from the stray LF")
}

func TestParserLimits(t *testing.T) {
	p := NewParser(strings.NewReader("data: "+strings.Repeat("x", 64)+"\n\n"), 16)
	_, err := p.Next()
	assert.ErrorIs(t, err, ErrTooLarge, "one data line over the limit")

	p = NewParser(strings.NewReader(strings.Repeat("data: 0123456789\n", 3)+"\n"), 16)
	_, err = p.Next()
	assert.ErrorIs(t, err, ErrTooLarge, "data lines that add up over the limit")

	p = NewParser(strings.NewReader(": "+strings.Repeat("x", 4<<10)+"\n"), 16)
	_, err = p.Next()
	assert.ErrorIs(t, err, ErrTooLarge, "any line far over the limit")

	p = NewParser(strings.NewReader("data: 0123456789abcdef\n\n"), 16)
	e, err := p.Next()
	require.NoError(t, err, "the limit itself is fine")
	assert.Len(t, e.Data, 16)

	p = NewParser(iotest.ErrReader(io.ErrUnexpectedEOF), 0)
	_, err = p.Next()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF, "read errors come through")
}
