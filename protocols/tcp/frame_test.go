package tcp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustFraming(t *testing.T, frame string, lengthBytes int) framing {
	t.Helper()
	f, err := parseFraming(frame, lengthBytes)
	require.NoError(t, err)
	return f
}

func TestParseFraming(t *testing.T) {
	cases := []struct {
		name        string
		frame       string
		lengthBytes int
		want        framing
		str         string
		err         string
	}{
		{name: "default is newline", want: framing{mode: frameNewline}, str: "newline"},
		{name: "newline", frame: "newline", want: framing{mode: frameNewline}, str: "newline"},
		{name: "length defaults to 4 bytes", frame: "length", want: framing{mode: frameLength, lengthBytes: 4}, str: "length/4"},
		{name: "length 1", frame: "length", lengthBytes: 1, want: framing{mode: frameLength, lengthBytes: 1}, str: "length/1"},
		{name: "length 2", frame: "length", lengthBytes: 2, want: framing{mode: frameLength, lengthBytes: 2}, str: "length/2"},
		{name: "length 8", frame: "length", lengthBytes: 8, want: framing{mode: frameLength, lengthBytes: 8}, str: "length/8"},
		{name: "raw", frame: "raw", want: framing{mode: frameRaw}, str: "raw"},
		{name: "unknown frame", frame: "text", err: `frame must be newline, length or raw, not "text"`},
		{name: "length_bytes 3", frame: "length", lengthBytes: 3, err: "length_bytes must be 1, 2, 4 or 8, not 3"},
		{name: "negative length_bytes", frame: "length", lengthBytes: -4, err: "length_bytes must be 1, 2, 4 or 8, not -4"},
		{name: "length_bytes without length", frame: "newline", lengthBytes: 2, err: "length_bytes applies with frame: length only"},
		{name: "length_bytes with the default frame", lengthBytes: 4, err: "length_bytes applies with frame: length only"},
		{name: "length_bytes with raw", frame: "raw", lengthBytes: 4, err: "length_bytes applies with frame: length only"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFraming(tc.frame, tc.lengthBytes)
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.str, got.String())
		})
	}
}

func TestEncode(t *testing.T) {
	long := func(n int) []byte { return bytes.Repeat([]byte("x"), n) }
	prefixed := func(prefix []byte, body []byte) []byte { return append(append([]byte{}, prefix...), body...) }
	cases := []struct {
		name        string
		frame       string
		lengthBytes int
		body        []byte
		want        []byte
		err         error
	}{
		{name: "newline appends a newline", frame: "newline", body: []byte("hello"), want: []byte("hello\n")},
		{name: "newline keeps a final newline", frame: "newline", body: []byte("hello\n"), want: []byte("hello\n")},
		{name: "newline keeps a final CRLF", frame: "newline", body: []byte("hello\r\n"), want: []byte("hello\r\n")},
		{name: "newline of an empty body", frame: "newline", body: nil, err: errEmptyBody},
		{name: "newline of a line ending only", frame: "newline", body: []byte("\r\n"), err: errEmptyBody},
		{name: "newline of a CR only", frame: "newline", body: []byte("\r"), err: errEmptyBody},
		{name: "newline of a blank line", frame: "newline", body: []byte(" "), want: []byte(" \n")},
		{name: "newline compacts multi-line JSON", frame: "newline", body: []byte("{\n  \"a\": 1,\n  \"b\": [1, 2]\n}\n"), want: []byte("{\"a\":1,\"b\":[1,2]}\n")},
		{name: "newline compacts CRLF JSON", frame: "newline", body: []byte("[\r\n 1\r\n]"), want: []byte("[1]\n")},
		{name: "newline refuses a multi-line text", frame: "newline", body: []byte("line one\nline two"), err: errNewlineInBody},
		{name: "newline refuses a text with an inner empty line", frame: "newline", body: []byte("a\n\n"), err: errNewlineInBody},
		{name: "length 1", frame: "length", lengthBytes: 1, body: []byte("hi"), want: []byte{2, 'h', 'i'}},
		{name: "length 1 at its limit", frame: "length", lengthBytes: 1, body: long(255), want: prefixed([]byte{255}, long(255))},
		{name: "length 1 over its limit", frame: "length", lengthBytes: 1, body: long(256), err: errFrameTooLarge},
		{name: "length 2", frame: "length", lengthBytes: 2, body: long(300), want: prefixed([]byte{1, 44}, long(300))},
		{name: "length 2 over its limit", frame: "length", lengthBytes: 2, body: long(65536), err: errFrameTooLarge},
		{name: "length 4", frame: "length", body: []byte("hi"), want: []byte{0, 0, 0, 2, 'h', 'i'}},
		{name: "length 8", frame: "length", lengthBytes: 8, body: []byte("hi"), want: []byte{0, 0, 0, 0, 0, 0, 0, 2, 'h', 'i'}},
		{name: "length keeps newlines", frame: "length", lengthBytes: 1, body: []byte("a\nb"), want: []byte{3, 'a', '\n', 'b'}},
		{name: "length of an empty body", frame: "length", lengthBytes: 2, body: nil, err: errEmptyBody},
		{name: "raw is the body", frame: "raw", body: []byte("a\nb\n"), want: []byte("a\nb\n")},
		{name: "raw of an empty body", frame: "raw", body: []byte{}, err: errEmptyBody},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mustFraming(t, tc.frame, tc.lengthBytes).encode(tc.body)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// readAll reads every frame of a stream, then the error that ended it.
func readAll(f framing, in io.Reader, maxBytes, bufSize int) ([]string, error) {
	return frames(f.reader(bufio.NewReaderSize(in, bufSize), maxBytes))
}

// readDatagram reads every frame of a datagram, then the error that ended it.
func readDatagram(f framing, in io.Reader, maxBytes, bufSize int) ([]string, error) {
	return frames(f.datagramReader(bufio.NewReaderSize(in, bufSize), maxBytes))
}

func frames(fr *frameReader) ([]string, error) {
	var frames []string
	for {
		b, err := fr.next()
		if err != nil {
			return frames, err
		}
		frames = append(frames, string(b))
	}
}

func TestFrameReader(t *testing.T) {
	long := strings.Repeat("y", 40)
	cases := []struct {
		name        string
		frame       string
		lengthBytes int
		datagram    bool // read in as one udp datagram, not a tcp stream
		in          string
		max         int
		want        []string
		err         error // the error that ends the stream; io.EOF when it ends cleanly
	}{
		{name: "newline lines", frame: "newline", in: "a\nbb\nccc\n", want: []string{"a", "bb", "ccc"}, err: io.EOF},
		{name: "newline strips CR", frame: "newline", in: "a\r\nb\r\n", want: []string{"a", "b"}, err: io.EOF},
		{name: "newline keeps an inner CR", frame: "newline", in: "a\rb\n", want: []string{"a\rb"}, err: io.EOF},
		{name: "newline skips empty lines", frame: "newline", in: "\n\na\n\r\n\nb\n\n", want: []string{"a", "b"}, err: io.EOF},
		{name: "newline stream cut inside a line", frame: "newline", in: "a\nlast", want: []string{"a"}, err: errTruncated},
		{name: "newline stream cut after a CR", frame: "newline", in: "a\nlast\r", want: []string{"a"}, err: errTruncated},
		{name: "newline stream ending in a bare CR", frame: "newline", in: "a\n\r", want: []string{"a"}, err: io.EOF},
		{name: "newline datagram ends its last line", frame: "newline", datagram: true, in: "a\nlast", want: []string{"a", "last"}, err: io.EOF},
		{name: "newline datagram strips CR from its last line", frame: "newline", datagram: true, in: "last\r", want: []string{"last"}, err: io.EOF},
		{name: "newline datagram last line over max", frame: "newline", datagram: true, in: "123456", max: 5, err: errFrameTooLarge},
		{name: "newline of an empty stream", frame: "newline", in: "", err: io.EOF},
		{name: "newline of an empty datagram", frame: "newline", datagram: true, in: "", err: io.EOF},
		{name: "newline of blank lines only", frame: "newline", in: "\n\r\n\n", err: io.EOF},
		{name: "newline line across buffer refills", frame: "newline", in: long + "\n" + long + "\n", max: 100, want: []string{long, long}, err: io.EOF},
		{name: "newline line at max", frame: "newline", in: "12345\n", max: 5, want: []string{"12345"}, err: io.EOF},
		{name: "newline line at max with CRLF", frame: "newline", in: "12345\r\n", max: 5, want: []string{"12345"}, err: io.EOF},
		{name: "newline line over max", frame: "newline", in: "ok\n123456\nnever\n", max: 5, want: []string{"ok"}, err: errFrameTooLarge},
		{name: "newline long line over max", frame: "newline", in: "ok\n" + long + long + "\n", max: 30, want: []string{"ok"}, err: errFrameTooLarge},
		{name: "newline long unterminated line over max", frame: "newline", in: "ok\n" + long + long, max: 30, want: []string{"ok"}, err: errFrameTooLarge},
		{name: "length 1", frame: "length", lengthBytes: 1, in: "\x01a\x02bb", want: []string{"a", "bb"}, err: io.EOF},
		{name: "length 2", frame: "length", lengthBytes: 2, in: "\x00\x03abc", want: []string{"abc"}, err: io.EOF},
		{name: "length 4", frame: "length", in: "\x00\x00\x00\x02hi\x00\x00\x00\x01!", want: []string{"hi", "!"}, err: io.EOF},
		{name: "length 8", frame: "length", lengthBytes: 8, in: "\x00\x00\x00\x00\x00\x00\x00\x02hi", want: []string{"hi"}, err: io.EOF},
		{name: "length keeps newlines and NULs", frame: "length", lengthBytes: 1, in: "\x03a\nb\x01\x00", want: []string{"a\nb", "\x00"}, err: io.EOF},
		{name: "length skips zero-length frames", frame: "length", lengthBytes: 2, in: "\x00\x00\x00\x01a\x00\x00", want: []string{"a"}, err: io.EOF},
		{name: "length of an empty stream", frame: "length", in: "", err: io.EOF},
		{name: "length truncated prefix", frame: "length", in: "\x00\x00\x00\x01a\x00\x00", want: []string{"a"}, err: errTruncated},
		{name: "length truncated body", frame: "length", lengthBytes: 1, in: "\x01a\x05abc", want: []string{"a"}, err: errTruncated},
		{name: "length datagram with several frames", frame: "length", lengthBytes: 1, datagram: true, in: "\x01a\x02bc", want: []string{"a", "bc"}, err: io.EOF},
		{name: "length datagram truncated", frame: "length", lengthBytes: 1, datagram: true, in: "\x01a\x05abc", want: []string{"a"}, err: errTruncated},
		{name: "length frame at max", frame: "length", lengthBytes: 1, in: "\x05abcde", max: 5, want: []string{"abcde"}, err: io.EOF},
		{name: "length frame over max", frame: "length", lengthBytes: 1, in: "\x01a\x06abcdef\x01b", max: 5, want: []string{"a"}, err: errFrameTooLarge},
		{name: "length huge prefix over max", frame: "length", lengthBytes: 8, in: "\xff\xff\xff\xff\xff\xff\xff\xff", max: 5, err: errFrameTooLarge},
		{name: "raw is the whole stream", frame: "raw", in: "a\nb\x00c", want: []string{"a\nb\x00c"}, err: io.EOF},
		{name: "raw is the whole datagram", frame: "raw", datagram: true, in: "a\nb", want: []string{"a\nb"}, err: io.EOF},
		{name: "raw of an empty stream", frame: "raw", in: "", err: io.EOF},
		{name: "raw at max", frame: "raw", in: "12345", max: 5, want: []string{"12345"}, err: io.EOF},
		{name: "raw over max", frame: "raw", in: "123456", max: 5, err: errFrameTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limit := tc.max
			if limit == 0 {
				limit = 1 << 20
			}
			f := mustFraming(t, tc.frame, tc.lengthBytes)
			read := readAll
			if tc.datagram {
				read = readDatagram
			}
			// The smallest buffer bufio allows, and one byte per read, so
			// lines and frames cross refills.
			for _, in := range []io.Reader{strings.NewReader(tc.in), iotest.OneByteReader(strings.NewReader(tc.in))} {
				got, err := read(f, in, limit, 16)
				assert.Equal(t, tc.want, got)
				require.ErrorIs(t, err, tc.err)
			}
		})
	}
}

func TestFrameReaderErrorsAreSticky(t *testing.T) {
	fr := mustFraming(t, "newline", 0).reader(bufio.NewReader(strings.NewReader("123456\nok\n")), 5)
	_, err := fr.next()
	require.ErrorIs(t, err, errFrameTooLarge)
	_, again := fr.next()
	assert.Equal(t, err, again, "a stream that lost a frame cannot be read on")
}

func TestFrameReaderPassesReadErrors(t *testing.T) {
	boom := errors.New("boom")
	for _, frame := range []string{"newline", "length", "raw"} {
		t.Run(frame, func(t *testing.T) {
			in := io.MultiReader(strings.NewReader("\x00\x00"), iotest.ErrReader(boom))
			_, err := readAll(mustFraming(t, frame, 0), in, 1<<20, 16)
			require.ErrorIs(t, err, boom)
		})
	}
}

func TestFrameReaderGrowsLargeFramesAsTheyArrive(t *testing.T) {
	body := bytes.Repeat([]byte("z"), 3*bodyStep+7)
	frame, err := mustFraming(t, "length", 0).encode(body)
	require.NoError(t, err)
	got, err := readAll(mustFraming(t, "length", 0), bytes.NewReader(frame), 1<<20, 4096)
	require.ErrorIs(t, err, io.EOF)
	require.Len(t, got, 1)
	assert.Equal(t, string(body), got[0])

	_, err = readAll(mustFraming(t, "length", 0), bytes.NewReader(frame[:len(frame)-1]), 1<<20, 4096)
	require.ErrorIs(t, err, errTruncated)
	assert.Contains(t, err.Error(), "after 196614 of 196615 bytes")
}

// Whatever a destination encodes, a source with the same framing reads back
// unchanged (empty messages aside: no framing carries them).
func TestFrameRoundTrip(t *testing.T) {
	messages := [][]byte{
		[]byte("plain text"),
		[]byte(`{"json":true}`),
		[]byte("ends with a newline\n"),
		[]byte("tab\tand \r inside"),
		bytes.Repeat([]byte("big"), 30000),
		{0x00, 0xff, 0x10},
	}
	streams := []framing{
		mustFraming(t, "newline", 0),
		mustFraming(t, "length", 1),
		mustFraming(t, "length", 2),
		mustFraming(t, "length", 4),
		mustFraming(t, "length", 8),
	}
	for _, f := range streams {
		t.Run(f.String(), func(t *testing.T) {
			var stream bytes.Buffer
			var want []string
			for _, m := range messages {
				b, err := f.encode(m)
				if errors.Is(err, errFrameTooLarge) {
					continue // over a 1- or 2-byte prefix
				}
				require.NoError(t, err)
				stream.Write(b)
				if f.mode == frameNewline {
					m = bytes.TrimSuffix(m, []byte("\n"))
				}
				want = append(want, string(m))
			}
			got, err := readAll(f, &stream, 1<<20, 4096)
			require.ErrorIs(t, err, io.EOF)
			assert.Equal(t, want, got)
		})
	}
	raw := mustFraming(t, "raw", 0)
	for _, m := range messages {
		b, err := raw.encode(m)
		require.NoError(t, err)
		got, err := readAll(raw, bytes.NewReader(b), 1<<20, 4096)
		require.ErrorIs(t, err, io.EOF)
		assert.Equal(t, []string{string(m)}, got)
	}
}
