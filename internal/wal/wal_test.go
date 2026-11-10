package wal

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/xavidop/hikyaku/envelope"
)

func msg(body string) *envelope.Envelope {
	e := envelope.New("in", "http", []byte(body))
	e.ContentType = "application/json"
	e.SetHeader("X-Trace", "t-1")
	e.SetAttribute("kafka.partition", 3)
	return e
}

func replay(t *testing.T, l *Log) ([]uint64, []*envelope.Envelope) {
	t.Helper()
	var seqs []uint64
	var envs []*envelope.Envelope
	require.NoError(t, l.Replay(func(seq uint64, env *envelope.Envelope) {
		seqs = append(seqs, seq)
		envs = append(envs, env)
	}, nil))
	return seqs, envs
}

func TestAppendReplayDeleteSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	l, err := Open(path, true)
	require.NoError(t, err)
	a, b, c := msg(`{"n":1}`), msg(`{"n":2}`), msg(`{"n":3}`)
	seqs, err := l.Append([]*envelope.Envelope{a, b})
	require.NoError(t, err)
	more, err := l.Append([]*envelope.Envelope{c})
	require.NoError(t, err)
	assert.Less(t, seqs[0], seqs[1])
	assert.Less(t, seqs[1], more[0], "sequences grow across appends")
	assert.Equal(t, 3, l.Len())
	require.NoError(t, l.Delete(seqs[1]))
	require.NoError(t, l.Delete(seqs[1]), "deleting twice is harmless")
	assert.Equal(t, 2, l.Len())
	require.NoError(t, l.Close())

	l, err = Open(path, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	assert.Equal(t, 2, l.Len(), "the count is rebuilt on open")
	got, envs := replay(t, l)
	require.Equal(t, []uint64{seqs[0], more[0]}, got, "oldest first, the delivered one gone")
	assert.Equal(t, a.ID, envs[0].ID)
	assert.Equal(t, `{"n":1}`, string(envs[0].Body))
	assert.Equal(t, "application/json", envs[0].ContentType)
	assert.Equal(t, "t-1", envs[0].Header("X-Trace"))
	assert.Equal(t, "in", envs[0].Source)
	assert.Equal(t, c.ID, envs[1].ID)
}

func TestSecondOpenFailsFast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	l, err := Open(path, false)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	_, err = Open(path, false)
	assert.ErrorContains(t, err, "is locked by another process")
}

func TestConcurrentDeletesShareTransactions(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "buffer.db"), true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	envs := make([]*envelope.Envelope, 200)
	for i := range envs {
		envs[i] = msg(`{}`)
	}
	seqs, err := l.Append(envs)
	require.NoError(t, err)
	var wg sync.WaitGroup
	for _, s := range seqs {
		wg.Add(1)
		go func() { defer wg.Done(); assert.NoError(t, l.Delete(s)) }()
	}
	wg.Wait()
	assert.Equal(t, 0, l.Len())
	got, _ := replay(t, l)
	assert.Empty(t, got)
}

func TestReplayDropsUndecodableEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	l, err := Open(path, true)
	require.NoError(t, err)
	seqs, err := l.Append([]*envelope.Envelope{msg(`{"ok":1}`)})
	require.NoError(t, err)
	require.NoError(t, l.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Put(key(seqs[0]+1), []byte("not json"))
	}))
	l.n.Add(1)
	var bad []uint64
	var good int
	require.NoError(t, l.Replay(func(uint64, *envelope.Envelope) { good++ }, func(seq uint64, _ error) { bad = append(bad, seq) }))
	assert.Equal(t, 1, good)
	assert.Equal(t, []uint64{seqs[0] + 1}, bad)
	assert.Equal(t, 1, l.Len(), "the broken entry is deleted")
	require.NoError(t, l.Close())
}

func TestAppendNothing(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "buffer.db"), true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	seqs, err := l.Append(nil)
	require.NoError(t, err)
	assert.Nil(t, seqs)
	assert.Equal(t, filepath.Base(l.Path()), "buffer.db")
}

func TestDeleteManyInOneTransaction(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "buffer.db"), true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	seqs, err := l.Append([]*envelope.Envelope{msg(`{}`), msg(`{}`), msg(`{}`)})
	require.NoError(t, err)
	require.NoError(t, l.DeleteMany([]uint64{seqs[0], seqs[2], seqs[2], 999}))
	assert.Equal(t, 1, l.Len(), "duplicates and unknown sequences count nothing")
	require.NoError(t, l.DeleteMany(nil))
	got, _ := replay(t, l)
	assert.Equal(t, []uint64{seqs[1]}, got)
}
