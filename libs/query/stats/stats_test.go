package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func call(service, method string, tsMs int64, durationMs int32, isError bool) model.CallRow {
	return model.CallRow{
		PK:   model.PK{PodNamespace: "ns", PodService: service, PodName: service + "-0", RestartTimeMs: 1},
		TsMs: tsMs, DurationMs: durationMs, Method: method, ErrorFlag: isError,
		CpuTimeMs: int64(durationMs) / 2, WaitTimeMs: 1, QueueWaitMs: 2, SuspendMs: 3,
	}
}

func fieldRequest(groupBy string, limit int, order Order) Request {
	return Request{Query: model.CallsQuery{FromMs: 0, ToMs: 1000}, GroupBy: groupBy, Limit: limit, Order: order}
}

func merged(t *testing.T, r Request, rows ...model.CallRow) Response {
	t.Helper()
	agg := NewAggregator(r.Grouping(), r.Order, 0, nil)
	for i := range rows {
		require.NoError(t, agg.Add(&rows[i]))
	}
	m := NewMerger(nil)
	require.NoError(t, m.AddSource(agg.Groups(), agg.Totals(), agg.Approximate()))
	return m.Response(r)
}

func TestFieldGroupingRanksAndCuts(t *testing.T) {
	rows := []model.CallRow{
		call("billing", "a", 1, 100, false),
		call("billing", "a", 2, 100, true),
		call("orders", "b", 3, 1000, false),
		call("audit", "c", 4, 5, false),
		call("zeta", "d", 5, 5, false),
	}

	t.Run("count_desc with ties by key ascending", func(t *testing.T) {
		resp := merged(t, fieldRequest("service", 2, OrderCountDesc), rows...)
		require.Len(t, resp.Rows, 2)
		assert.Equal(t, "billing", resp.Rows[0].Key)
		assert.Equal(t, int64(2), resp.Rows[0].Count)
		assert.Equal(t, int64(1), resp.Rows[0].Errors)
		assert.Equal(t, int64(200), resp.Rows[0].DurationMsSum)
		assert.Equal(t, int64(100), resp.Rows[0].CPUTimeMsSum)
		assert.Equal(t, int64(100), *resp.Rows[0].P50Ms)
		assert.Equal(t, "audit", resp.Rows[1].Key, "audit, orders, and zeta tie at one call; the key breaks it")
		require.NotNil(t, resp.Other)
		assert.Equal(t, int64(2), resp.Other.Count, "orders and zeta")
		assert.Equal(t, int64(1005), resp.Other.DurationMsSum)
		assert.Equal(t, 4, resp.Distinct)
		assert.Equal(t, Totals{Count: 5, Errors: 1}, resp.Totals)
		assert.False(t, resp.Approximate)
	})
	t.Run("duration_sum_desc", func(t *testing.T) {
		resp := merged(t, fieldRequest("service", 1, OrderDurationSumDesc), rows...)
		require.Len(t, resp.Rows, 1)
		assert.Equal(t, "orders", resp.Rows[0].Key)
		assert.Equal(t, int64(4), resp.Other.Count)
	})
	t.Run("no other when every value fits", func(t *testing.T) {
		resp := merged(t, fieldRequest("method", 20, OrderCountDesc), rows...)
		assert.Len(t, resp.Rows, 4)
		assert.Nil(t, resp.Other)
	})
	t.Run("every stage A field", func(t *testing.T) {
		for field, want := range map[string]string{
			"namespace": "ns", "service": "billing", "pod": "billing-0", "method": "a",
		} {
			resp := merged(t, fieldRequest(field, 1, OrderCountDesc), rows...)
			assert.Equal(t, want, resp.Rows[0].Key, field)
		}
	})
}

// TestTimeBuckets pins the 02 §2.9 grid: bucket_ms = max(1, ceil(span /
// buckets)), every bucket a row including the empty ones, and a shorter last
// bucket that ends at to.
func TestTimeBuckets(t *testing.T) {
	r := Request{Query: model.CallsQuery{FromMs: 1000, ToMs: 1010}, GroupBy: GroupByTime, Buckets: 3}
	resp := merged(t, r,
		call("s", "m", 1000, 1, false), // bucket 0 [1000, 1004)
		call("s", "m", 1003, 1, false), // bucket 0, last ms
		call("s", "m", 1004, 1, false), // bucket 1 [1004, 1008)
		call("s", "m", 1009, 1, true),  // bucket 2 [1008, 1010), the short one
	)
	require.NotNil(t, resp.BucketMs)
	assert.Equal(t, int64(4), *resp.BucketMs)
	require.Len(t, resp.Rows, 3)
	assert.Equal(t, []any{int64(1000), int64(1004), int64(1008)},
		[]any{resp.Rows[0].Key, resp.Rows[1].Key, resp.Rows[2].Key})
	assert.Equal(t, []int64{2, 1, 1}, []int64{resp.Rows[0].Count, resp.Rows[1].Count, resp.Rows[2].Count})
	assert.Equal(t, int64(1), resp.Rows[2].Errors)
	assert.Equal(t, 3, resp.Distinct)
	assert.Nil(t, resp.Other)

	t.Run("empty buckets are rows with null percentiles", func(t *testing.T) {
		r := Request{Query: model.CallsQuery{FromMs: 0, ToMs: 600}, GroupBy: GroupByTime, Buckets: 6}
		resp := merged(t, r, call("s", "m", 250, 7, false))
		require.Len(t, resp.Rows, 6)
		for i, row := range resp.Rows {
			assert.Equal(t, int64(i*100), row.Key)
			if i == 2 {
				assert.Equal(t, int64(1), row.Count)
				assert.Equal(t, int64(7), *row.P99Ms)
				continue
			}
			assert.Zero(t, row.Count)
			assert.Nil(t, row.P50Ms)
		}
		body, err := json.Marshal(resp.Rows[0])
		require.NoError(t, err)
		assert.Contains(t, string(body), `"p50_ms":null`)
	})
	t.Run("fewer rows than buckets when the span does not divide", func(t *testing.T) {
		// span 10, buckets 6: bucket_ms = 2, so five buckets cover the window.
		r := Request{Query: model.CallsQuery{FromMs: 0, ToMs: 10}, GroupBy: GroupByTime, Buckets: 6}
		resp := merged(t, r)
		assert.Equal(t, int64(2), *resp.BucketMs)
		assert.Len(t, resp.Rows, 5)
	})
	t.Run("a span shorter than the bucket count gives 1 ms buckets", func(t *testing.T) {
		r := Request{Query: model.CallsQuery{FromMs: 0, ToMs: 5}, GroupBy: GroupByTime, Buckets: 60}
		resp := merged(t, r)
		assert.Equal(t, int64(1), *resp.BucketMs)
		assert.Len(t, resp.Rows, 5)
	})
}

// TestSpaceSaving: past maxGroups the lightest group gives its slot to the
// new value, which inherits its metric; the result is approximate and the
// heavy groups survive.
func TestSpaceSaving(t *testing.T) {
	agg := NewAggregator(Grouping{Field: "method"}, OrderCountDesc, 2, nil)
	add := func(method string, n int) {
		for i := 0; i < n; i++ {
			r := call("s", method, 1, 10, false)
			require.NoError(t, agg.Add(&r))
		}
	}
	add("heavy", 10)
	add("light", 2)
	assert.False(t, agg.Approximate())
	add("new", 1)
	assert.True(t, agg.Approximate())

	got := map[string]int64{}
	for _, g := range agg.Groups() {
		got[g.Key] = g.Count
	}
	assert.Equal(t, map[string]int64{"heavy": 10, "new": 3}, got,
		"new inherits light's two calls: an overestimate by at most the evicted count")
	assert.Equal(t, Totals{Count: 13}, agg.Totals(), "totals stay exact")

	t.Run("duration_sum_desc inherits the duration sum", func(t *testing.T) {
		agg := NewAggregator(Grouping{Field: "method"}, OrderDurationSumDesc, 1, nil)
		a := call("s", "a", 1, 50, false)
		b := call("s", "b", 1, 7, false)
		require.NoError(t, agg.Add(&a))
		require.NoError(t, agg.Add(&b))
		groups := agg.Groups()
		require.Len(t, groups, 1)
		assert.Equal(t, "b", groups[0].Key)
		assert.Equal(t, int64(57), groups[0].DurationMsSum)
		assert.Equal(t, int64(1), groups[0].Count, "the other members cover the calls since entry")
		assert.Equal(t, int64(7), *groups[0].Sketch.QuantileMs(0.5))
	})
	t.Run("a time grouping is never bounded", func(t *testing.T) {
		agg := NewAggregator(Grouping{Field: GroupByTime, BucketMs: 1}, OrderCountDesc, 1, nil)
		for ts := int64(0); ts < 5; ts++ {
			r := call("s", "m", ts, 1, false)
			require.NoError(t, agg.Add(&r))
		}
		assert.Len(t, agg.Groups(), 5)
		assert.False(t, agg.Approximate())
	})
}

func TestAggregatorCharges(t *testing.T) {
	var charged int64
	agg := NewAggregator(Grouping{Field: "method"}, OrderCountDesc, 0, func(n int64) error {
		charged += n
		return nil
	})
	r := call("s", "abc", 1, 10, false)
	require.NoError(t, agg.Add(&r))
	require.NoError(t, agg.Add(&r))
	assert.Equal(t, int64(GroupBytes+3+BinBytes), charged, "one group, one bin")

	denied := NewAggregator(Grouping{Field: "method"}, OrderCountDesc, 0, func(int64) error { return assert.AnError })
	assert.ErrorIs(t, denied.Add(&r), assert.AnError)
}

// TestSpaceSavingReleasesEvictedCharges is the review's case: with one slot
// and a 1,024-byte lease, alternating keys evict on every call. The lease must
// hold only the retained group, or row 14 fails with ErrNeverFits although
// one group costs 314 bytes.
func TestSpaceSavingReleasesEvictedCharges(t *testing.T) {
	lease := budget.New(1024, time.Millisecond, budget.Hooks{}).NewLease()
	defer lease.Release()
	charge := func(n int64) error {
		if n < 0 {
			lease.Shrink(-n)
			return nil
		}
		return lease.Grow(context.Background(), n)
	}
	agg := NewAggregator(Grouping{Field: "method"}, OrderCountDesc, 1, charge)
	for i := 0; i < 100; i++ {
		r := call("s", fmt.Sprintf("method%04d", i%2), int64(i), 1, false)
		require.NoError(t, agg.Add(&r), "row %d", i+1)
	}
	assert.True(t, agg.Approximate())
	assert.Equal(t, int64(GroupBytes+len("method0001")+BinBytes), lease.Held(),
		"only the retained group, its key, and its one bin stay charged")
}

func TestMergerCombinesSources(t *testing.T) {
	r := fieldRequest("service", 10, OrderCountDesc)
	hot := NewAggregator(r.Grouping(), r.Order, 0, nil)
	cold := NewAggregator(r.Grouping(), r.Order, 0, nil)
	for i := int64(0); i < 100; i++ {
		row := call("billing", "m", i, int32(i), false)
		if i%2 == 0 {
			require.NoError(t, hot.Add(&row))
		} else {
			require.NoError(t, cold.Add(&row))
		}
	}
	m := NewMerger(nil)
	// The hot groups cross the wire, so round-trip them the way the fan-out does.
	var wire []Group
	for _, g := range hot.Groups() {
		back, err := InternalGroupOf(g).Group()
		require.NoError(t, err)
		wire = append(wire, back)
	}
	require.NoError(t, m.AddSource(wire, hot.Totals(), false))
	require.NoError(t, m.AddSource(cold.Groups(), cold.Totals(), true))
	resp := m.Response(r)
	require.Len(t, resp.Rows, 1)
	assert.Equal(t, int64(100), resp.Rows[0].Count)
	assert.Equal(t, int64(4950), resp.Rows[0].DurationMsSum)
	assert.InDelta(t, 49, *resp.Rows[0].P50Ms, 1)
	assert.Equal(t, int64(100), resp.Totals.Count)
	assert.True(t, resp.Approximate, "one approximate source makes the result approximate")
}

func TestPKHashes(t *testing.T) {
	pk := model.PK{PodNamespace: "ns", PodService: "svc", PodName: "pod", RestartTimeMs: 1, TraceFileIndex: 2}
	other := pk
	other.RecordIndex = 1
	assert.NotEqual(t, PKHash(pk), PKHash(other))
	assert.Equal(t, PKHash(pk), PKHash(pk))

	hashes := []uint64{PKHash(pk), PKHash(other), 0, 1 << 63}
	back, err := DecodeHashes(EncodeHashes(hashes))
	require.NoError(t, err)
	assert.Equal(t, hashes, back)
	_, err = DecodeHashes("AAAA")
	assert.Error(t, err, "3 bytes are not a hash")

	set := NewHashSet([]uint64{5, 1}, []uint64{3, 5})
	assert.Equal(t, HashSet{1, 3, 5}, set)
	assert.True(t, set.Contains(3))
	assert.False(t, set.Contains(4))
}

func params(kv ...string) url.Values {
	v := url.Values{"from": {"1000"}, "to": {"2000"}}
	for i := 0; i < len(kv); i += 2 {
		v.Add(kv[i], kv[i+1])
	}
	return v
}

func TestParseRequest(t *testing.T) {
	ok := func(v url.Values) Request {
		t.Helper()
		r, detail := ParseRequest(v)
		require.Empty(t, detail)
		return r
	}
	bad := func(v url.Values, want string) {
		t.Helper()
		_, detail := ParseRequest(v)
		assert.Contains(t, detail, want)
	}

	r := ok(params("group_by", "time"))
	assert.Equal(t, DefaultBuckets, r.Buckets)
	assert.Equal(t, int64(17), r.Grouping().BucketMs, "ceil(1000 / 60)")
	assert.Equal(t, 1000, ok(params("group_by", "time", "buckets", "1000")).Buckets)
	r = ok(params("group_by", "service", "filter", "duration>1s"))
	assert.Equal(t, DefaultLimit, r.Limit)
	assert.Equal(t, OrderCountDesc, r.Order)
	assert.Len(t, r.Query.Filters, 1)
	assert.Equal(t, OrderDurationSumDesc, ok(params("group_by", "method", "order", "duration_sum_desc", "limit", "1000")).Order)

	bad(params(), "group_by is required")
	for _, g := range []string{"user.tenant", "class", "endpoint", "error", "retention_class", "param_name", "trace_id"} {
		assert.Equal(t, g, ok(params("group_by", g)).GroupBy)
	}
	bad(params("group_by", "duration"), "group_by=duration is not a grouping")
	bad(params("group_by", "9x"), "group_by=9x is not a grouping")
	bad(params("group_by", "time", "buckets", "0"), "buckets must be")
	bad(params("group_by", "time", "buckets", "1001"), "buckets must be")
	bad(params("group_by", "time", "buckets", "x"), "buckets must be")
	bad(params("group_by", "time", "limit", "5"), "limit applies to a field grouping")
	bad(params("group_by", "time", "order", "count_desc"), "order applies to a field grouping")
	bad(params("group_by", "service", "buckets", "5"), "buckets applies to group_by=time only")
	bad(params("group_by", "service", "limit", "0"), "limit must be")
	bad(params("group_by", "service", "limit", "1001"), "limit must be")
	bad(params("group_by", "service", "order", "start_desc"), "order must be count_desc or duration_sum_desc")
	bad(params("group_by", "service", "cursor", "x"), "cursor does not apply")
	bad(params("group_by", "service", "filter", "service=~("), "is not a valid regular expression")
	_, detail := ParseRequest(url.Values{"group_by": {"time"}})
	assert.Contains(t, detail, "from and to are required")

	t.Run("a window too wide for int64 buckets", func(t *testing.T) {
		v := url.Values{"from": {strconv.FormatInt(-1<<63, 10)}, "to": {strconv.FormatInt(1<<63-1, 10)},
			"group_by": {"time"}, "buckets": {"1"}}
		bad(v, "too wide")
	})
}

func TestInternalRequestKeepsTheExternalGrid(t *testing.T) {
	ext, detail := ParseRequest(params("group_by", "time", "buckets", "10", "filter", "service=billing"))
	require.Empty(t, detail)
	in, detail := ParseInternalRequest(ext.InternalValues(1550, 77))
	require.Empty(t, detail)
	assert.Equal(t, int64(1550), in.Query.FromMs, "the replica aggregates the hot part only")
	assert.Equal(t, Grouping{Field: GroupByTime, OriginMs: 1000, BucketMs: 100}, in.Grouping)
	assert.Equal(t, 77, in.MaxGroups)
	assert.Equal(t, model.CanonicalConditions(ext.Query.Filters), model.CanonicalConditions(in.Query.Filters))

	field, _ := ParseRequest(params("group_by", "pod", "order", "duration_sum_desc", "limit", "3"))
	in, detail = ParseInternalRequest(field.InternalValues(1000, 10))
	require.Empty(t, detail)
	assert.Equal(t, OrderDurationSumDesc, in.Order)
	assert.Equal(t, Grouping{Field: "pod"}, in.Grouping)

	_, detail = ParseInternalRequest(params("group_by", "time", "bucket_ms", "0"))
	assert.Contains(t, detail, "bucket_ms")
	_, detail = ParseInternalRequest(params("group_by", "time", "bucket_origin_ms", "1500"))
	assert.Contains(t, detail, "bucket_origin_ms")
}

// TestStageBGroupings places calls by params, the derived fields, error,
// retention_class, and param names (02 §2.9), with exact counts.
func TestStageBGroupings(t *testing.T) {
	row := func(method, class string, isError bool, params map[string][]string) model.CallRow {
		r := call("svc", method, 1, 10, isError)
		r.RetentionClass = class
		r.Params = params
		return r
	}
	rows := []model.CallRow{
		row("void a.B.run()", model.RetentionShortClean, false, map[string][]string{
			"user.tenant": {"acme", "globex", "acme"}, "web.url": {"http://h/orders/1"}, "web.method": {"GET"}}),
		row("a.B.stop", model.RetentionAnyError, true, map[string][]string{
			"user.tenant": {"acme"}, "web.url": {"/orders/2?x"}, "web.method": {"GET"}, "flag": {}}),
		row("#7", model.RetentionShortClean, false, map[string][]string{"user.tenant": {""}, "trace.id": {"t1"}}),
		row("c.D.go", model.RetentionLongClean, false, nil),
	}
	counts := func(groupBy string) (map[string]int64, Totals) {
		resp := merged(t, fieldRequest(groupBy, 100, OrderCountDesc), rows...)
		out := map[string]int64{}
		for _, r := range resp.Rows {
			out[r.Key.(string)] = r.Count
		}
		assert.Equal(t, len(out), resp.Distinct, groupBy)
		return out, resp.Totals
	}

	got, totals := counts("user.tenant")
	assert.Equal(t, map[string]int64{"acme": 2, "globex": 1}, got,
		"a call counts once in the row of each distinct value; an empty value is no key")
	assert.Equal(t, Totals{Count: 4, Errors: 1}, totals, "every call counts once in the totals")

	got, _ = counts("class")
	assert.Equal(t, map[string]int64{"a.B": 2, "c.D": 1}, got, "a method that names no class falls into no row")
	got, _ = counts("endpoint")
	assert.Equal(t, map[string]int64{"GET /orders/{id}": 2}, got)
	got, _ = counts("error")
	assert.Equal(t, map[string]int64{"true": 1, "false": 3}, got)
	got, _ = counts("retention_class")
	assert.Equal(t, map[string]int64{model.RetentionShortClean: 2, model.RetentionAnyError: 1, model.RetentionLongClean: 1}, got)
	got, _ = counts("trace_id")
	assert.Equal(t, map[string]int64{"t1": 1}, got)
	got, _ = counts(model.GroupByParamName)
	assert.Equal(t, map[string]int64{"user.tenant": 3, "web.url": 2, "web.method": 2, "trace.id": 1}, got,
		"a param recorded with no value is absent")
}

// TestSpaceSavingOverAParam bounds a high-cardinality param grouping, the
// expected case of 02 §2.9: the heaviest values stay, and the result is
// approximate.
func TestSpaceSavingOverAParam(t *testing.T) {
	agg := NewAggregator(Grouping{Field: "request.id"}, OrderCountDesc, 10, nil)
	add := func(id string, n int) {
		for range n {
			r := call("svc", "a.B.c", 1, 10, false)
			r.Params = map[string][]string{"request.id": {id}}
			require.NoError(t, agg.Add(&r))
		}
	}
	add("hot", 50)
	add("warm", 20)
	for i := range 100 {
		add(strconv.Itoa(i), 1)
	}
	assert.True(t, agg.Approximate())
	keys := map[string]int64{}
	for _, g := range agg.Groups() {
		keys[g.Key] = g.Count
	}
	assert.Len(t, keys, 10)
	assert.Equal(t, int64(50), keys["hot"], "the error bound, 170/10, is below the heavy values")
	assert.Equal(t, int64(20), keys["warm"])
	assert.Equal(t, Totals{Count: 170}, agg.Totals())
}
