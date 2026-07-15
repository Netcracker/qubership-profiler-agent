package hotstore

import (
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenPodRestartSameMsCollision pins the re-review finding 2:
// RestartTimeMs is stamped at TCP accept with millisecond precision, so two
// accepts of one pod in the same millisecond used to collide on the key and
// reopen over the first session's footered WALs — fresh writers at size 0
// meant colliding offsets, and replay stopped at the first footer, silently
// losing the second session. Every accept must get its own pod-restart, and
// neither session may lose calls.
func TestOpenPodRestartSameMsCollision(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-m", RestartTimeMs: janitorCallTs}
	appendOne := func(pr *PodRestart, ts int64) {
		t.Helper()
		_, err := pr.AppendDictionaryWord("com.example.Api.get")
		require.NoError(t, err)
		require.NoError(t, pr.AppendCall(ts, data.Call{
			Method: 0, Duration: 5, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
		}))
	}

	first, err := store.OpenPodRestart(key)
	require.NoError(t, err)
	appendOne(first, janitorCallTs)

	t.Run("a live same-ms accept gets its own pod-restart", func(t *testing.T) {
		second, err := store.OpenPodRestart(key)
		require.NoError(t, err)
		assert.NotEqual(t, first.Key, second.Key, "the colliding accept must not share the live pod-restart")
		assert.Greater(t, second.Key.RestartTimeMs, first.Key.RestartTimeMs)
		appendOne(second, janitorCallTs+1)
		require.NoError(t, second.Close())
	})

	require.NoError(t, first.Close())

	t.Run("a closed same-ms reopen gets its own pod-restart", func(t *testing.T) {
		third, err := store.OpenPodRestart(key)
		require.NoError(t, err)
		assert.NotEqual(t, first.Key, third.Key, "reopening over a closed pod-restart corrupts its WALs")
		appendOne(third, janitorCallTs+2)
		require.NoError(t, third.Close())
	})

	// Neither session lost calls: three distinct pod-restarts, one indexed
	// call each, and each session's calls.wal replays exactly its own record.
	rows, err := store.Calls(store.cfg.Bucket(janitorCallTs))
	require.NoError(t, err)
	require.Len(t, rows, 3)
	perPod := map[string]int{}
	for _, row := range rows {
		perPod[row.PodRestart]++
	}
	assert.Len(t, perPod, 3, "each accept must index under its own pod-restart")

	for podRestart := range perPod {
		prKey, err := ParsePodRestartKey(podRestart)
		require.NoError(t, err)
		records := 0
		clean, err := ReplayWal(filepath.Join(prKey.dir(store.cfg.DataDir), "calls.wal"),
			func(int64, []byte) error { records++; return nil })
		require.NoError(t, err)
		assert.True(t, clean, "each session's calls.wal must close with its own verified footer")
		assert.Equal(t, 1, records, "each session's calls.wal holds exactly its own record")
	}
}
