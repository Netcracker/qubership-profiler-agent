package hotstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func replayAll(t *testing.T, path string) (records [][]byte, clean bool) {
	t.Helper()
	clean, err := ReplayWal(path, func(_ int64, body []byte) error {
		records = append(records, append([]byte(nil), body...))
		return nil
	})
	require.NoError(t, err)
	return records, clean
}

func TestWalCleanCloseRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWal(path, 256, 100*time.Millisecond)
	require.NoError(t, err)
	_, err = w.Append([]byte("first"))
	require.NoError(t, err)
	offset, err := w.Append([]byte("second"))
	require.NoError(t, err)
	assert.Equal(t, int64(1+len("first")), offset, "second record starts after the first's varint prefix and body")
	require.NoError(t, w.Close())

	records, clean := replayAll(t, path)
	assert.True(t, clean, "a closed WAL carries a verified CRC footer")
	assert.Equal(t, [][]byte{[]byte("first"), []byte("second")}, records)
}

func TestWalTornTailIsTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWal(path, 256, 100*time.Millisecond)
	require.NoError(t, err)
	_, err = w.Append([]byte("kept"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	// Simulate a crash mid-append: a record length with only part of the body.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{40, 'o', 'r', 'n'})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	records, clean := replayAll(t, path)
	assert.False(t, clean, "no footer after a crash")
	assert.Equal(t, [][]byte{[]byte("kept")}, records)

	stat, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, int64(1+len("kept")), stat.Size(), "the torn tail is truncated in place")
}

func TestWalFooterCrcMismatchFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWal(path, 256, 100*time.Millisecond)
	require.NoError(t, err)
	_, err = w.Append([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Flip one record byte so the stored footer no longer matches.
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[1] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = ReplayWal(path, func(int64, []byte) error { return nil })
	assert.ErrorContains(t, err, "CRC mismatch")
}
