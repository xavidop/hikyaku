// Package wal is the disk buffer (buffer.type: disk): a bbolt write-ahead
// log of the messages the relay holds. A message is written before its
// source is told it is queued and deleted once its outcome is final, so a
// crash or a restart loses nothing: the next start delivers what is left.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/xavidop/hikyaku/envelope"
)

var bucket = []byte("queue")

// Log is an open write-ahead log. Its methods are safe for concurrent use.
type Log struct {
	db   *bolt.DB
	path string
	n    atomic.Int64
}

// Open opens (or creates) the log at path. sync false skips the fsync of
// each commit: faster, but a host crash (not a process crash) can lose the
// last writes. A second relay opening the same file fails fast instead of
// sharing it.
func Open(path string, sync bool) (*Log, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout:        2 * time.Second,
		NoSync:         !sync,
		NoFreelistSync: true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		if errors.Is(err, berrors.ErrTimeout) {
			return nil, fmt.Errorf("buffer %s is locked by another process (one relay per buffer file)", path)
		}
		return nil, fmt.Errorf("buffer %s: %w", path, err)
	}
	l := &Log{db: db, path: path}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		l.n.Store(int64(b.Stats().KeyN))
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("buffer %s: %w", path, err)
	}
	return l, nil
}

// Path is the log's file.
func (l *Log) Path() string { return l.path }

// Len is the number of messages in the log.
func (l *Log) Len() int { return int(l.n.Load()) }

// Append writes envs in one transaction (one fsync) and returns their
// sequence numbers, in order.
func (l *Log) Append(envs []*envelope.Envelope) ([]uint64, error) {
	if len(envs) == 0 {
		return nil, nil
	}
	vals := make([][]byte, len(envs))
	for i, env := range envs {
		b, err := json.Marshal(env)
		if err != nil {
			return nil, fmt.Errorf("buffer: encoding message %s: %w", env.ID, err)
		}
		vals[i] = b
	}
	seqs := make([]uint64, len(envs))
	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		for i, v := range vals {
			seq, err := b.NextSequence()
			if err != nil {
				return err
			}
			if err := b.Put(key(seq), v); err != nil {
				return err
			}
			seqs[i] = seq
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("buffer: %w", err)
	}
	l.n.Add(int64(len(envs)))
	return seqs, nil
}

// Delete removes a message once its outcome is final. Concurrent deletes
// share one transaction (bbolt's Batch), so a busy relay does not fsync per
// message.
func (l *Log) Delete(seq uint64) error {
	var removed bool
	err := l.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		removed = b.Get(key(seq)) != nil
		return b.Delete(key(seq))
	})
	if err != nil {
		return fmt.Errorf("buffer: %w", err)
	}
	if removed {
		l.n.Add(-1)
	}
	return nil
}

// DeleteMany removes messages in one transaction (one fsync).
func (l *Log) DeleteMany(seqs []uint64) error {
	if len(seqs) == 0 {
		return nil
	}
	removed := 0
	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		removed = 0
		for _, seq := range seqs {
			if b.Get(key(seq)) == nil {
				continue
			}
			if err := b.Delete(key(seq)); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("buffer: %w", err)
	}
	l.n.Add(-int64(removed))
	return nil
}

// Replay calls fn for every message, oldest first. An entry that no longer
// decodes is reported to bad and deleted, so it cannot block every start.
func (l *Log) Replay(fn func(seq uint64, env *envelope.Envelope), bad func(seq uint64, err error)) error {
	var broken []uint64
	err := l.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).ForEach(func(k, v []byte) error {
			seq := binary.BigEndian.Uint64(k)
			env := &envelope.Envelope{}
			if err := json.Unmarshal(v, env); err != nil {
				broken = append(broken, seq)
				if bad != nil {
					bad(seq, err)
				}
				return nil
			}
			fn(seq, env)
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("buffer: %w", err)
	}
	for _, seq := range broken {
		if err := l.Delete(seq); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the file.
func (l *Log) Close() error { return l.db.Close() }

func key(seq uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], seq)
	return k[:]
}
