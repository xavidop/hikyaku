package sse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"strconv"
	"time"
)

// Event is one dispatched server-sent event.
type Event struct {
	// ID is the last event ID when the event was dispatched: the latest id
	// field, which carries over to the events after it.
	ID string
	// Name is the event field, "" when the event had none (a browser
	// dispatches such an event as "message").
	Name string
	// Data is the event's data fields joined with "\n".
	Data []byte
}

// ErrTooLarge is returned when a line or an event's data is larger than the
// parser's limit.
var ErrTooLarge = errors.New("sse: event too large")

var bom = []byte("\xEF\xBB\xBF")

// Parser reads an event stream the way the WHATWG HTML standard interprets
// one ("9.2.6 Interpreting an event stream"):
//
//   - Lines end with LF, CR or CRLF. A blank line dispatches the event.
//   - A line starting with ":" is a comment. Otherwise the field name is
//     what comes before the first ":", and the value is what follows it,
//     less one leading space. A line without a colon is a field name with
//     an empty value. Field names are case-sensitive.
//   - data appends its value and a line feed to the data buffer; event sets
//     the event name; id sets the last event ID (unless the value contains
//     U+0000), which persists across events; retry sets the reconnection
//     time when its value is ASCII digits only (milliseconds). Other fields
//     are ignored.
//   - On a blank line the last event ID is updated. An event whose data
//     buffer is empty (no data field) is not dispatched; otherwise the data
//     loses its final line feed and the event is dispatched. Either way the
//     data buffer and the event name are reset.
//   - A leading byte order mark is skipped, and an event unfinished at the
//     end of the stream is discarded.
//
// The parser never reads ahead of a line ending, so on a live stream an
// event is returned as soon as its blank line arrives. Data is passed
// through as bytes, without UTF-8 replacement.
type Parser struct {
	r       *bufio.Reader
	max     int // the largest data buffer, and roughly the longest line
	line    []byte
	skipLF  bool // the last line ended with CR: a following LF belongs to it
	started bool // the byte order mark is behind us

	data     []byte
	name     string
	idBuf    string // the last event ID buffer
	lastID   string // the last event ID, as of the latest blank line
	retry    time.Duration
	retrySet bool
}

// lineSlack is how much longer than the data limit a line may be: room for
// the field name.
const lineSlack = 1 << 10

// NewParser reads events from r. maxBytes bounds an event's data (and a
// line); 0 or less means 10MiB.
func NewParser(r io.Reader, maxBytes int64) *Parser {
	if maxBytes <= 0 {
		maxBytes = 10 << 20
	}
	if maxBytes > math.MaxInt-lineSlack {
		maxBytes = math.MaxInt - lineSlack
	}
	return &Parser{r: bufio.NewReaderSize(r, 32<<10), max: int(maxBytes)}
}

// SetLastID seeds the last event ID, as when a client reconnects with
// Last-Event-ID: events without an id of their own then carry it. Call it
// before the first Next.
func (p *Parser) SetLastID(id string) {
	p.idBuf, p.lastID = id, id
}

// LastID is the last event ID as of the latest blank line, including one
// set by an event that had no data.
func (p *Parser) LastID() string { return p.lastID }

// Retry is the reconnection time of the latest valid retry field, and
// whether there was one.
func (p *Parser) Retry() (time.Duration, bool) { return p.retry, p.retrySet }

// Next returns the next event. At the end of the stream it returns io.EOF
// (an unfinished event is dropped); a read error is returned as it is.
func (p *Parser) Next() (Event, error) {
	for {
		line, err := p.readLine()
		if err != nil {
			return Event{}, err
		}
		if len(line) == 0 {
			if ev, ok := p.dispatch(); ok {
				return ev, nil
			}
			continue
		}
		if err := p.field(line); err != nil {
			return Event{}, err
		}
	}
}

// readLine returns the next line without its ending. The slice is valid
// until the next call.
func (p *Parser) readLine() ([]byte, error) {
	p.line = p.line[:0]
	if p.skipLF {
		p.skipLF = false
		b, err := p.r.Peek(1)
		if err != nil {
			return nil, err
		}
		if b[0] == '\n' {
			_, _ = p.r.Discard(1)
		}
	}
	for {
		if _, err := p.r.Peek(1); err != nil {
			return nil, err // a line cut off by the end of the stream is dropped
		}
		buf, _ := p.r.Peek(p.r.Buffered())
		i := bytes.IndexAny(buf, "\r\n")
		n := i
		if i < 0 {
			n = len(buf)
		}
		if len(p.line)+n > p.max+lineSlack {
			return nil, ErrTooLarge
		}
		p.line = append(p.line, buf[:n]...)
		if i < 0 {
			_, _ = p.r.Discard(n)
			continue
		}
		p.skipLF = buf[i] == '\r'
		_, _ = p.r.Discard(i + 1)
		break
	}
	if !p.started {
		p.started = true
		p.line = bytes.TrimPrefix(p.line, bom)
	}
	return p.line, nil
}

// field processes one non-blank line.
func (p *Parser) field(line []byte) error {
	if line[0] == ':' {
		return nil // a comment
	}
	name, value := line, []byte(nil)
	if i := bytes.IndexByte(line, ':'); i >= 0 {
		name, value = line[:i], bytes.TrimPrefix(line[i+1:], []byte(" "))
	}
	switch string(name) {
	case "data":
		if len(p.data)+len(value) > p.max {
			return ErrTooLarge
		}
		p.data = append(p.data, value...)
		p.data = append(p.data, '\n')
	case "event":
		p.name = string(value)
	case "id":
		if bytes.IndexByte(value, 0) < 0 {
			p.idBuf = string(value)
		}
	case "retry":
		if ms, ok := digits(value); ok {
			p.retry, p.retrySet = time.Duration(ms)*time.Millisecond, true
		}
	}
	return nil
}

// dispatch ends the event at a blank line.
func (p *Parser) dispatch() (Event, bool) {
	p.lastID = p.idBuf
	name := p.name
	p.name = ""
	if len(p.data) == 0 {
		return Event{}, false
	}
	data := make([]byte, len(p.data)-1)
	copy(data, p.data)
	if cap(p.data) > 64<<10 {
		p.data = nil // let a large event's buffer go
	} else {
		p.data = p.data[:0]
	}
	return Event{ID: p.lastID, Name: name, Data: data}, true
}

// digits parses a retry value: ASCII digits only, small enough to be a
// duration in milliseconds.
func digits(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	ms, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil || ms > math.MaxInt64/int64(time.Millisecond) {
		return 0, false
	}
	return ms, true
}
