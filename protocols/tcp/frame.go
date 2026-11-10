package tcp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

// Framing is how bytes are cut into messages: a tcp connection's stream, or
// one udp datagram. Sources and destinations share it, so a destination
// writes exactly what a source with the same frame and length_bytes reads.

// frameMode is the frame setting.
type frameMode int

const (
	// frameNewline: one message per line. A line ends at "\n" (in a udp
	// datagram, also where the datagram ends), a trailing "\r" is stripped,
	// and empty lines are skipped.
	frameNewline frameMode = iota
	// frameLength: each message follows a big-endian unsigned length of
	// lengthBytes bytes. A zero length carries no message.
	frameLength
	// frameRaw: a tcp connection's whole stream, until EOF, or one udp
	// datagram, is one message.
	frameRaw
)

// defaultLengthBytes is the size of the length prefix with frame: length.
const defaultLengthBytes = 4

// bodyStep bounds what a length-prefixed frame allocates before its bytes
// arrive: a peer that announces a large frame and stalls pins no more.
const bodyStep = 64 << 10

// Framing errors. A source closes a connection (or drops the rest of a
// datagram) on errFrameTooLarge and errTruncated: once a frame is lost, the
// stream cannot be cut into frames again.
var (
	// errFrameTooLarge: a frame over max_body_bytes (sources), or a message
	// longer than its length prefix can carry (destinations).
	errFrameTooLarge = errors.New("frame too large")
	// errTruncated: the stream or datagram ended inside a frame.
	errTruncated = errors.New("truncated frame")
	// errNewlineInBody: with frame: newline, a message that is not JSON
	// holds a newline, so it would arrive as several messages.
	errNewlineInBody = errors.New("newline in body")
	// errEmptyBody: an empty message, which no framing carries: a source
	// skips an empty line, frame or stream.
	errEmptyBody = errors.New("empty body")
)

// framing is a checked frame and length_bytes.
type framing struct {
	mode        frameMode
	lengthBytes int // frameLength: 1, 2, 4 or 8
}

// parseFraming checks frame and length_bytes and applies the defaults:
// newline framing, and a 4-byte prefix with frame: length.
func parseFraming(frame string, lengthBytes int) (framing, error) {
	var f framing
	switch frame {
	case "", "newline":
		f.mode = frameNewline
	case "length":
		f.mode = frameLength
	case "raw":
		f.mode = frameRaw
	default:
		return framing{}, fmt.Errorf("frame must be newline, length or raw, not %q", frame)
	}
	switch {
	case lengthBytes == 0:
		if f.mode == frameLength {
			f.lengthBytes = defaultLengthBytes
		}
	case f.mode != frameLength:
		return framing{}, errors.New("length_bytes applies with frame: length only")
	case lengthBytes == 1 || lengthBytes == 2 || lengthBytes == 4 || lengthBytes == 8:
		f.lengthBytes = lengthBytes
	default:
		return framing{}, fmt.Errorf("length_bytes must be 1, 2, 4 or 8, not %d", lengthBytes)
	}
	return f, nil
}

// String names the framing in logs: newline, length/4, raw.
func (f framing) String() string {
	switch f.mode {
	case frameLength:
		return "length/" + strconv.Itoa(f.lengthBytes)
	case frameRaw:
		return "raw"
	}
	return "newline"
}

// maxFrame is the longest message the length prefix can carry.
func (f framing) maxFrame() uint64 {
	if f.lengthBytes >= 8 {
		return math.MaxUint64
	}
	return 1<<(8*f.lengthBytes) - 1
}

// encode returns the bytes that carry body as one frame.
//
// With frame: newline, a body that already ends with "\n" is written as it
// is, and any other body gets one appended; the line ending ("\n" or
// "\r\n") is not part of the message. A body holding a newline before its
// end would arrive as several messages: JSON is compacted (its newlines are
// only whitespace), and anything else fails with errNewlineInBody. With
// frame: length, a body longer than the prefix can carry fails with
// errFrameTooLarge. Raw bodies are written as they are. An empty message
// (with newline, one that is only a line ending) fails with errEmptyBody,
// since a source would skip it.
func (f framing) encode(body []byte) ([]byte, error) {
	line := bytes.TrimSuffix(bytes.TrimSuffix(body, []byte("\n")), []byte("\r"))
	if len(body) == 0 || f.mode == frameNewline && len(line) == 0 {
		return nil, fmt.Errorf("%w: a source skips an empty message, so it would never arrive", errEmptyBody)
	}
	switch f.mode {
	case frameLength:
		size := uint64(len(body))
		if size > f.maxFrame() {
			return nil, fmt.Errorf("%w: %d bytes do not fit a %d-byte length prefix (at most %d)", errFrameTooLarge, size, f.lengthBytes, f.maxFrame())
		}
		var prefix [8]byte
		binary.BigEndian.PutUint64(prefix[:], size)
		out := make([]byte, 0, f.lengthBytes+len(body))
		out = append(out, prefix[8-f.lengthBytes:]...)
		return append(out, body...), nil
	case frameRaw:
		return body, nil
	}
	content, terminated := bytes.CutSuffix(body, []byte("\n"))
	if bytes.IndexByte(content, '\n') >= 0 {
		var compact bytes.Buffer
		if err := json.Compact(&compact, body); err != nil {
			return nil, fmt.Errorf("%w: the message is not JSON and holds a newline, so frame: newline would split it; use frame: length or raw", errNewlineInBody)
		}
		return append(compact.Bytes(), '\n'), nil
	}
	if terminated {
		return body, nil
	}
	out := make([]byte, 0, len(body)+1)
	out = append(out, body...)
	return append(out, '\n'), nil
}

// frameReader cuts a stream into frames.
type frameReader struct {
	f        framing
	r        *bufio.Reader
	max      int   // longest frame accepted
	datagram bool  // r is one udp datagram, whose end ends its last line
	err      error // once set, next returns it
}

// reader returns a frameReader for a tcp stream, with frames of at most
// maxBytes. A last line the end of the stream cuts off is a truncated
// frame: a sender that fails in the middle of a write leaves one, and it
// must not pass for a whole message.
func (f framing) reader(r *bufio.Reader, maxBytes int) *frameReader {
	return &frameReader{f: f, r: r, max: max(maxBytes, 0)}
}

// datagramReader returns a frameReader for one udp datagram. A datagram
// arrives whole, so its end ends its last line.
func (f framing) datagramReader(r *bufio.Reader, maxBytes int) *frameReader {
	fr := f.reader(r, maxBytes)
	fr.datagram = true
	return fr
}

// next returns the next frame, skipping empty ones. It returns io.EOF when
// the stream ends between frames, errFrameTooLarge or errTruncated when the
// stream cannot go on, and the read error otherwise. After an error, every
// call returns it again.
func (fr *frameReader) next() ([]byte, error) {
	if fr.err != nil {
		return nil, fr.err
	}
	var frame []byte
	var err error
	switch fr.f.mode {
	case frameLength:
		frame, err = fr.prefixed()
	case frameRaw:
		frame, err = fr.rest()
	default:
		frame, err = fr.line()
	}
	if err != nil {
		fr.err = err
		return nil, err
	}
	return frame, nil
}

// line reads the next non-empty line. A datagram's last line may end at
// its end instead of "\n".
func (fr *frameReader) line() ([]byte, error) {
	for {
		line, err := fr.readLine()
		if len(line) > 0 {
			if err != nil {
				fr.err = err // the datagram ended with this line
			}
			return line, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// readLine reads one line without its "\n" and trailing "\r". It returns
// io.EOF once the input ends: with a datagram's last line when it had no
// "\n", while a stream's is errTruncated.
func (fr *frameReader) readLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := fr.r.ReadSlice('\n')
		// Room for max bytes plus "\r\n"; the exact check comes once the
		// line is whole.
		if len(line)+len(chunk)-2 > fr.max {
			return nil, fmt.Errorf("%w: a line longer than %d bytes", errFrameTooLarge, fr.max)
		}
		line = append(line, chunk...)
		switch {
		case err == nil:
			return fr.checkLine(bytes.TrimSuffix(line[:len(line)-1], []byte("\r")), nil)
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			line = bytes.TrimSuffix(line, []byte("\r"))
			if len(line) > 0 && !fr.datagram {
				return nil, fmt.Errorf("%w: the stream ended inside a line, after %d bytes without a newline", errTruncated, len(line))
			}
			return fr.checkLine(line, io.EOF)
		default:
			return nil, err
		}
	}
}

func (fr *frameReader) checkLine(line []byte, err error) ([]byte, error) {
	if len(line) > fr.max {
		return nil, fmt.Errorf("%w: a line of %d bytes, over %d", errFrameTooLarge, len(line), fr.max)
	}
	return line, err
}

// prefixed reads the next length-prefixed frame of a non-zero length.
func (fr *frameReader) prefixed() ([]byte, error) {
	n := fr.f.lengthBytes
	for {
		var prefix [8]byte
		if _, err := io.ReadFull(fr.r, prefix[8-n:]); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("%w: the stream ended inside a %d-byte length prefix", errTruncated, n)
			}
			return nil, err // io.EOF: the stream ended between frames
		}
		size := binary.BigEndian.Uint64(prefix[:])
		if size > uint64(fr.max) {
			return nil, fmt.Errorf("%w: a frame of %d bytes, over %d", errFrameTooLarge, size, fr.max)
		}
		if size == 0 {
			continue
		}
		body, err := readN(fr.r, int(size))
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("%w: the stream ended after %d of %d bytes", errTruncated, len(body), size)
			}
			return nil, err
		}
		return body, nil
	}
}

// readN reads exactly n bytes, growing the buffer as they arrive once n is
// over bodyStep. On a short stream it returns what it read and io.EOF or
// io.ErrUnexpectedEOF.
func readN(r io.Reader, n int) ([]byte, error) {
	if n <= bodyStep {
		b := make([]byte, n)
		got, err := io.ReadFull(r, b)
		return b[:got], err
	}
	var buf bytes.Buffer
	buf.Grow(bodyStep)
	if _, err := io.CopyN(&buf, r, int64(n)); err != nil {
		return buf.Bytes(), err
	}
	return buf.Bytes(), nil
}

// rest reads the whole stream as one frame; an empty stream is io.EOF.
func (fr *frameReader) rest() ([]byte, error) {
	limit := int64(fr.max)
	if limit < math.MaxInt64 {
		limit++ // one byte over max tells a stream at the limit from a longer one
	}
	b, err := io.ReadAll(io.LimitReader(fr.r, limit))
	switch {
	case err != nil:
		return nil, err
	case len(b) > fr.max:
		return nil, fmt.Errorf("%w: a stream longer than %d bytes", errFrameTooLarge, fr.max)
	case len(b) == 0:
		return nil, io.EOF
	}
	fr.err = io.EOF // one frame per stream
	return b, nil
}
