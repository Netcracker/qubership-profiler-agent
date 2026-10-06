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

// walRecordSize is the on-disk footprint of a short (< 128-byte) record: a
// one-byte varint length, the body, and the 4-byte per-record CRC (№26).
func walRecordSize(body string) int64 { return int64(1 + len(body) + 4) }

func TestWalCleanCloseRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWal(path, 256, 100*time.Millisecond)
	require.NoError(t, err)
	_, err = w.Append([]byte("first"))
	require.NoError(t, err)
	offset, err := w.Append([]byte("second"))
	require.NoError(t, err)
	assert.Equal(t, walRecordSize("first"), offset,
		"second record starts after the first's varint prefix, body, and CRC")
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
	assert.Equal(t, walRecordSize("kept"), stat.Size(), "the torn tail is truncated in place")
}

// TestWalRecordCrcCatchesMidFileCorruption pins the №26 per-record CRC: a
// flipped byte inside a record's body is caught on replay — before, it
// replayed silently unless a clean footer happened to be present — and
// everything from the corrupt record on is dropped as an untrusted tail.
func TestWalRecordCrcCatchesMidFileCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWal(path, 256, 100*time.Millisecond)
	require.NoError(t, err)
	_, err = w.Append([]byte("aaaa"))
	require.NoError(t, err)
	_, err = w.Append([]byte("bbbb"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[1] ^= 0xFF // first body byte of record 1
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	records, clean := replayAll(t, path)
	assert.False(t, clean)
	assert.Empty(t, records, "nothing after the corrupt record can be trusted")
	stat, err := os.Stat(path)
	require.NoError(t, err)
	assert.Zero(t, stat.Size(), "the untrusted tail is truncated away")
}

func TestWalFooterCrcMismatchFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWal(path, 256, 100*time.Millisecond)
	require.NoError(t, err)
	_, err = w.Append([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Flip a byte of the stored footer CRC so it no longer matches.
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = ReplayWal(path, func(int64, []byte) error { return nil })
	assert.ErrorContains(t, err, "CRC mismatch")
}

// TestReadWalRecordAt pins the №9 positioned-read path the seal pass uses:
// each record is fetched by its offset without touching the rest of the file,
// its per-record CRC is verified, and a bad offset fails loudly instead of
// decoding garbage.
func TestReadWalRecordAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWal(path, 256, 100*time.Millisecond)
	require.NoError(t, err)
	off1, err := w.Append([]byte("first"))
	require.NoError(t, err)
	off2, err := w.Append([]byte("second"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	body, read, err := ReadWalRecordAt(f, off2)
	require.NoError(t, err)
	assert.Equal(t, []byte("second"), body)
	// read counts fetched bytes: the record plus the fixed varint probe — a
	// per-record constant, never the rest of the file.
	assert.GreaterOrEqual(t, read, walRecordSize("second"))
	assert.Less(t, read, walRecordSize("second")+16)
	body, _, err = ReadWalRecordAt(f, off1)
	require.NoError(t, err)
	assert.Equal(t, []byte("first"), body)

	_, _, err = ReadWalRecordAt(f, off1+1)
	assert.Error(t, err, "an offset inside a record decodes as garbage and must fail the CRC or the frame")
	stat, err := os.Stat(path)
	require.NoError(t, err)
	_, _, err = ReadWalRecordAt(f, stat.Size())
	assert.Error(t, err, "an offset past the end has no record")
}
