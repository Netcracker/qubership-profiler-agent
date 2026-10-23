package hotread_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInternalStats pins /internal/v1/stats (02 §3): every group with its
// sketch bins, exact totals, the conditions applied, and the PK hash of each
// counted call. The SQL page is shrunk to two rows so the walk crosses page
// boundaries and a ts tie group, in two partitions.
func TestInternalStats(t *testing.T) {
	defer hotread.SetStatsPageRows(2)()
	store := openTestStore(t)
	podA := addPod(t, store, "pod-a", 1, "com.example.A.run", "com.example.B.run")
	podB := addPod(t, store, "pod-b", 1, "com.example.A.run")
	var pks []model.PK
	add := func(pr string, tsMs int64, method, duration, off int) {
		target := podA
		if pr == "pod-b" {
			target = podB
		}
		addCall(t, target, tsMs, data.Call{Method: data.TagId(method), Duration: data.LDuration(duration),
			ThreadName: "t", TraceFileIndex: 1, BufferOffset: off, RecordIndex: 0, CpuTime: 3})
		pks = append(pks, model.PK{PodNamespace: testNs, PodService: testSvc, PodName: pr, RestartTimeMs: 1,
			TraceFileIndex: 1, BufferOffset: int32(off)})
	}
	add("pod-a", baseMs+10, 0, 100, 0)
	add("pod-a", baseMs+10, 1, 200, 10) // a ts tie across the two-row page
	add("pod-a", baseMs+10, 0, 300, 20)
	add("pod-b", baseMs+20, 0, 400, 0)
	add("pod-a", baseMs+6*60_000, 1, 2000, 30) // the next partition
	srv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(srv.Close)

	window := url.Values{"from": {fmt.Sprint(baseMs)}, "to": {fmt.Sprint(baseMs + 30*60_000)}}
	get := func(extra ...string) stats.InternalResponse {
		t.Helper()
		v := url.Values{}
		for k, vs := range window {
			v[k] = vs
		}
		for i := 0; i < len(extra); i += 2 {
			v.Add(extra[i], extra[i+1])
		}
		var resp stats.InternalResponse
		require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/stats", v, &resp))
		return resp
	}

	resp := get("group_by", "method")
	assert.Equal(t, stats.Totals{Count: 5}, resp.Totals)
	assert.False(t, resp.Approximate)
	byKey := map[string]stats.InternalGroup{}
	for _, g := range resp.Groups {
		byKey[g.Key] = g
	}
	require.Len(t, byKey, 2)
	a := byKey["com.example.A.run"]
	assert.Equal(t, int64(3), a.Count)
	assert.Equal(t, int64(800), a.DurationMsSum)
	assert.Equal(t, int64(9), a.CPUTimeMsSum)
	sk, ok := a.Sketch.Sketch()
	require.True(t, ok)
	assert.Equal(t, int64(3), sk.Count(), "the sketch rides along instead of percentiles")
	assert.Equal(t, int64(2200), byKey["com.example.B.run"].DurationMsSum)

	hashes, err := stats.DecodeHashes(resp.PKHashes)
	require.NoError(t, err)
	want := make([]uint64, 0, len(pks))
	for _, pk := range pks {
		want = append(want, stats.PKHash(pk))
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	sort.Slice(hashes, func(i, j int) bool { return hashes[i] < hashes[j] })
	assert.Equal(t, want, hashes, "one hash per counted call, none twice across pages")

	t.Run("conditions apply", func(t *testing.T) {
		resp := get("group_by", "pod", "filter", "duration>=300ms")
		assert.Equal(t, int64(3), resp.Totals.Count)
		hashes, err := stats.DecodeHashes(resp.PKHashes)
		require.NoError(t, err)
		assert.Len(t, hashes, 3, "only the counted calls are hashed")
	})
	t.Run("the window bounds the rows", func(t *testing.T) {
		v := url.Values{"from": {fmt.Sprint(baseMs + 15)}, "to": {fmt.Sprint(baseMs + 30*60_000)},
			"group_by": {"service"}}
		var resp stats.InternalResponse
		require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/stats", v, &resp))
		assert.Equal(t, int64(2), resp.Totals.Count)
	})
	t.Run("time buckets on the caller's grid", func(t *testing.T) {
		resp := get("group_by", "time", "bucket_origin_ms", fmt.Sprint(baseMs-5), "bucket_ms", "60000")
		got := map[string]int64{}
		for _, g := range resp.Groups {
			got[g.Key] = g.Count
		}
		assert.Equal(t, map[string]int64{
			fmt.Sprint(baseMs - 5):           4,
			fmt.Sprint(baseMs - 5 + 360_000): 1,
		}, got, "the replica emits only non-empty buckets; query fills the rest")
	})
	t.Run("max_groups bounds the groups", func(t *testing.T) {
		resp := get("group_by", "method", "max_groups", "1")
		assert.Len(t, resp.Groups, 1)
		assert.True(t, resp.Approximate)
		assert.Equal(t, int64(5), resp.Totals.Count)
	})
	t.Run("invalid parameters", func(t *testing.T) {
		v := url.Values{"from": window["from"], "to": window["to"], "group_by": {"class"}}
		assert.Equal(t, http.StatusBadRequest, getJSON(t, srv, "/internal/v1/stats", v, nil))
	})
}
