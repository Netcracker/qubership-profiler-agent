package hotstore

// №1 memory behaviour: closed pod-restarts hold a lazy dictionary handle,
// words intern per service, and PROFILER_MEM_BUDGET evicts closed in-RAM
// state. New tests continue the existing Go suite of this package (the RSS
// measurement below needs the Go runtime, which a Kotlin suite cannot see).

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDictionaryInterningSharesWords pins the per-service intern pool: two
// pod-restarts of one service must share the word's backing bytes.
func TestDictionaryInterningSharesWords(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	open := func(pod string) *PodRestart {
		pr, err := store.OpenPodRestart(PodRestartKey{
			Namespace: "ns", Service: "svc", PodName: pod, RestartTimeMs: 1})
		require.NoError(t, err)
		_, err = pr.AppendDictionaryWord("com.example.Service.handle")
		require.NoError(t, err)
		return pr
	}
	pr1, pr2 := open("pod-a"), open("pod-b")
	w1, ok := pr1.DictWord(0)
	require.True(t, ok)
	w2, ok := pr2.DictWord(0)
	require.True(t, ok)
	assert.Equal(t, w1, w2)
	assert.Equal(t, unsafe.StringData(w1), unsafe.StringData(w2),
		"pods of one service must share the interned word bytes")
}

// TestClosedDictionaryLazyReload pins the lazy handle: Close drops the maps,
// and every dictionary accessor reloads from dictionary.wal on demand with
// the same content.
func TestClosedDictionaryLazyReload(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	pr, err := store.OpenPodRestart(PodRestartKey{
		Namespace: "ns", Service: "svc", PodName: "pod-l", RestartTimeMs: 1})
	require.NoError(t, err)
	for _, w := range []string{"com.example.Api.get", errorMarkerParam, "request.id"} {
		_, err := pr.AppendDictionaryWord(w)
		require.NoError(t, err)
	}
	require.NoError(t, pr.Close())

	pr.mu.Lock()
	require.Nil(t, pr.dict, "Close must drop the dictionary maps")
	require.Nil(t, pr.dictIds)
	pr.mu.Unlock()

	w, ok := pr.DictWord(0)
	require.True(t, ok, "a closed pod-restart's dictionary reloads from the WAL")
	assert.Equal(t, "com.example.Api.get", w)
	id, ok := pr.DictId(errorMarkerParam)
	require.True(t, ok)
	assert.Equal(t, 1, id)
	assert.Len(t, pr.Dictionary(), 3)

	require.True(t, pr.unloadDictionary(), "the reloaded handle can be dropped again")
	assert.False(t, pr.unloadDictionary(), "a second unload is a no-op")
}

// TestJanitorMemBudgetEvictsClosedState pins the PROFILER_MEM_BUDGET
// enforcement: over budget, a closed pod-restart loses its dictionary maps
// and — being fully sealed — its chunk index, while a live pod-restart is
// never touched.
func TestJanitorMemBudgetEvictsClosedState(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), MemBudgetBytes: 1})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	open := func(pod string) *PodRestart {
		pr, err := store.OpenPodRestart(PodRestartKey{
			Namespace: "ns", Service: "svc", PodName: pod, RestartTimeMs: 1})
		require.NoError(t, err)
		_, err = pr.AppendDictionaryWord("com.example.Service.handle")
		require.NoError(t, err)
		pr.mu.Lock()
		pr.chunks[7] = []ChunkRef{{RollingSeq: 1, Offset: 0, Length: 10}}
		pr.mu.Unlock()
		return pr
	}
	closedPr, livePr := open("pod-closed"), open("pod-live")
	require.NoError(t, closedPr.Close())
	// Reload the lazy handle so the janitor has something to unload.
	require.Len(t, closedPr.Dictionary(), 1)

	stats, err := store.JanitorPass(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.DictionariesUnloaded)
	assert.EqualValues(t, 1, stats.ChunkIndexesReleased)

	closedPr.mu.Lock()
	assert.Nil(t, closedPr.dict, "the closed pod-restart's dictionary is unloaded")
	assert.Empty(t, closedPr.chunks, "the fully-sealed chunk index is released")
	closedPr.mu.Unlock()
	livePr.mu.Lock()
	assert.NotNil(t, livePr.dict, "a live pod-restart keeps its dictionary")
	assert.Len(t, livePr.chunks[7], 1, "a live pod-restart keeps its chunk index")
	livePr.mu.Unlock()

	stats, err = store.JanitorPass(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.Zero(t, stats.DictionariesUnloaded, "already-evicted state is not re-counted")
	assert.Zero(t, stats.ChunkIndexesReleased)
}

// TestMemBudgetKeepsUnsealedChunkIndex pins the seal guard and the §6.1
// memory-pressure trigger together: while sealing cannot run (the №2 gate is
// engaged), a closed pod-restart with calls its watermark does not cover
// keeps the chunk index the future seal needs — only the dictionary unloads.
// Once the gate lifts, the mem budget early-seals that oldest dirty bucket
// (trigger 3) and the now-unpinned chunk index is released in the same pass.
func TestMemBudgetKeepsUnsealedChunkIndex(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir(), MemBudgetBytes: 1})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	pr, err := store.OpenPodRestart(PodRestartKey{
		Namespace: "ns", Service: "svc", PodName: "pod-u", RestartTimeMs: 1})
	require.NoError(t, err)
	require.NoError(t, pr.AppendCall(1_700_000_000_000, data.Call{
		Method: 0, Duration: 10, ThreadName: "main",
		TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
	}))
	pr.mu.Lock()
	pr.chunks[7] = []ChunkRef{{RollingSeq: 1, Offset: 0, Length: 10}}
	pr.mu.Unlock()
	require.NoError(t, pr.Close())

	// A paused seal stays paused: RAM pressure must not override the №2 gate.
	store.sealPaused.Store(true)
	stats, err := store.JanitorPass(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.Zero(t, stats.ChunkIndexesReleased, "an unsealed call pins the chunk index")
	assert.Zero(t, stats.MemPressureSeals, "the backpressure gate blocks the early seal")
	pr.mu.Lock()
	assert.Len(t, pr.chunks[7], 1)
	pr.mu.Unlock()

	store.sealPaused.Store(false)
	stats, err = store.JanitorPass(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	assert.EqualValues(t, 1, stats.MemPressureSeals, "trigger 3 seals the oldest dirty bucket")
	assert.EqualValues(t, 1, stats.ChunkIndexesReleased, "the sealed bucket unpins the chunk index")
	pr.mu.Lock()
	assert.Empty(t, pr.chunks, "the chunk index is released once its calls sealed")
	pr.mu.Unlock()

	files, err := store.LocalParquet(pr.Key)
	require.NoError(t, err)
	require.Len(t, files, 1, "the early seal produced a real parquet file")
	assert.Equal(t, 1, files[0].RowCount)
}

func heapAlloc() int64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}

// TestClosedPodRestartsReleaseDictionaryRAM is the №1 acceptance test: 300
// connections with a fat dictionary, disconnect, then 300 more — the heap
// must not double, because closed pod-restarts drop their dictionary maps.
// Without the lazy handle the two closed waves would pin ~2 × 300 pods ×
// 600 words × two map entries each, far above the asserted ceiling.
func TestClosedPodRestartsReleaseDictionaryRAM(t *testing.T) {
	// Fsync only on close: the test measures RAM, not WAL durability.
	store, err := Open(Config{DataDir: t.TempDir(),
		DictFsyncRecords: 1 << 20, DictFsyncInterval: time.Hour})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	words := make([]string, 600)
	for i := range words {
		words[i] = fmt.Sprintf("com.example.generated.pkg%04d.GeneratedService%04d.handleRequest%04d", i, i, i)
	}
	wave := func(from int) {
		for p := from; p < from+300; p++ {
			pr, err := store.OpenPodRestart(PodRestartKey{
				Namespace: "ns", Service: "svc",
				PodName: fmt.Sprintf("pod-%04d", p), RestartTimeMs: int64(p)})
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range words {
				if _, err := pr.AppendDictionaryWord(w); err != nil {
					t.Fatal(err)
				}
			}
			if err := pr.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}

	base := heapAlloc()
	wave(0)
	afterWave1 := heapAlloc() - base
	wave(300)
	afterWave2 := heapAlloc() - base
	t.Logf("heap growth: %d KiB after 300 closed pod-restarts, %d KiB after 600", afterWave1>>10, afterWave2>>10)

	// 300 closed pod-restarts with unloaded dictionaries cost well under 8 MB
	// (pods map, WAL writers, the interned word pool); keeping the maps loaded
	// would cost ≥ 300 pods × 600 words × ~128 B ≈ 23 MB and fail this.
	const ceiling = 8 << 20
	assert.Less(t, afterWave1, int64(ceiling),
		"closed pod-restarts must not keep their dictionaries in RAM")
	assert.Less(t, afterWave2, afterWave1+ceiling,
		"a second wave of connections must not stack on the first — RSS must not double")
}
