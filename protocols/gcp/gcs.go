package gcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"time"

	"cloud.google.com/go/storage"
)

// newStorageClient builds a Cloud Storage client. Reads go through the JSON
// API, like every other call, so an emulator needs no separate download
// host.
func newStorageClient(ctx context.Context, c clientConfig) (*storage.Client, error) {
	opts := append(slices.Clone(c.opts), storage.WithJSONReads())
	return storage.NewClient(ctx, opts...)
}

// stallReader cuts a read that makes no progress for timeout. Only the time
// spent inside Read counts, so a reader held back by the relay's
// backpressure is not cut.
type stallReader struct {
	r       io.ReadCloser
	timeout time.Duration
	cancel  context.CancelFunc
}

func (s *stallReader) Read(p []byte) (int, error) {
	t := time.AfterFunc(s.timeout, s.cancel)
	defer t.Stop()
	return s.r.Read(p)
}

func (s *stallReader) Close() error {
	err := s.r.Close()
	s.cancel()
	return err
}

// readLine reads one line without its line ending. A line longer than limit
// is read to its end but not kept, and over is true. err is io.EOF after the
// last line.
func readLine(r *bufio.Reader, limit int64) (line []byte, over bool, err error) {
	for {
		chunk, rerr := r.ReadSlice('\n')
		if !over {
			// +2 leaves room for the \r\n that is trimmed below.
			if int64(len(line)+len(chunk)) > limit+2 {
				over, line = true, nil
			} else {
				line = append(line, chunk...)
			}
		}
		if errors.Is(rerr, bufio.ErrBufferFull) {
			continue
		}
		line = bytes.TrimRight(line, "\r\n")
		if !over && int64(len(line)) > limit {
			over, line = true, nil
		}
		return line, over, rerr
	}
}
