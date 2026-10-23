package insight

import (
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The synthetic windows of the compare tests: the baseline hour, then the
// target hour.
const (
	baseStart   = t0
	targetStart = t0 + 3_600_000
)

var targetHour = Window{From: time.UnixMilli(targetStart), To: time.UnixMilli(targetStart + 3_600_000)}

// group is n calls of one root method in the hour that starts at start.
type group struct {
	start      int64
	method     string
	n          int
	durationMs int32
}

func windowCalls(groups ...group) []fakeCall {
	var out []fakeCall
	record := int32(0)
	for _, g := range groups {
		for i := 0; i < g.n; i++ {
			record++
			out = append(out, call("billing", g.method, g.start+int64(i)*1000, g.durationMs, record))
		}
	}
	return out
}

// isLookup reports whether a /stats request looks up groups: it has a
// condition beyond the service shorthand.
func isLookup(q url.Values) bool {
	return slices.ContainsFunc(q["filter"], func(f string) bool { return strings.HasPrefix(f, "method") })
}

func fmtInt(v int64) string { return strconv.FormatInt(v, 10) }

func statusOf(res CompareResult) map[string]string {
	out := map[string]string{}
	for _, g := range res.Groups {
		out[g.Key] = g.Status
		if g.Missing != "" {
			out[g.Key] += "/" + g.Missing
		}
	}
	return out
}

func TestCompareJoinsTwoWindows(t *testing.T) {
	for _, tc := range []struct {
		name  string
		calls []group
		limit int
		// partial makes the /stats answers for this window start partial;
		// lookup limits it to the lookups.
		partialStart int64
		lookupOnly   bool
		noSetOp      bool
		want         map[string]string
		lookups      int
	}{
		{
			// The plan's example: with limit 1, A=100/B=90 then A=80/B=110
			// gives two disjoint lists although both groups are in both
			// windows. The lookups find each in the other window.
			name: "a ranking swap across the top-N cut",
			calls: []group{
				{baseStart, "A", 100, 10}, {baseStart, "B", 90, 10},
				{targetStart, "A", 80, 10}, {targetStart, "B", 110, 10},
			},
			limit:   1,
			want:    map[string]string{"A": StatusCompared, "B": StatusCompared},
			lookups: 2,
		},
		{
			name: "a lookup that finds no calls proves a group new or vanished",
			calls: []group{
				{baseStart, "A", 100, 10}, {baseStart, "C", 5, 10},
				{targetStart, "B", 110, 10}, {targetStart, "C", 5, 10},
			},
			limit:   1,
			want:    map[string]string{"A": StatusVanished, "B": StatusNew},
			lookups: 2,
		},
		{
			name: "a response that lists every group proves absence without a lookup",
			calls: []group{
				{baseStart, "A", 100, 10}, {baseStart, "C", 5, 10},
				{targetStart, "B", 110, 10}, {targetStart, "C", 5, 10},
			},
			limit:   10,
			want:    map[string]string{"A": StatusVanished, "B": StatusNew, "C": StatusCompared},
			lookups: 0,
		},
		{
			// A failed source can hold A's target calls, so a partial lookup
			// proves nothing.
			name: "a partial lookup leaves a group unknown",
			calls: []group{
				{baseStart, "A", 100, 10}, {baseStart, "B", 90, 10},
				{targetStart, "B", 110, 10}, {targetStart, "C", 1, 10},
			},
			limit:        1,
			partialStart: targetStart, lookupOnly: true,
			want:    map[string]string{"A": StatusUnknown + "/" + WindowTarget, "B": StatusCompared},
			lookups: 2,
		},
		{
			name: "a partial response that lists every group still needs a lookup",
			calls: []group{
				{baseStart, "A", 100, 10},
				{targetStart, "B", 110, 10},
			},
			limit:        10,
			partialStart: targetStart,
			want:         map[string]string{"A": StatusUnknown + "/" + WindowTarget, "B": StatusNew},
			lookups:      1,
		},
		{
			name: "without a set condition on the field the group is outside the top N",
			calls: []group{
				{baseStart, "A", 100, 10}, {baseStart, "B", 90, 10},
				{targetStart, "A", 80, 10}, {targetStart, "B", 110, 10},
			},
			limit:   1,
			noSetOp: true,
			want: map[string]string{
				"A": StatusOutsideTop + "/" + WindowTarget, "B": StatusOutsideTop + "/" + WindowBaseline,
			},
			lookups: 0,
		},
		{
			name: "without a set condition a partial window leaves the group unknown",
			calls: []group{
				{baseStart, "A", 100, 10}, {baseStart, "B", 90, 10},
				{targetStart, "A", 80, 10}, {targetStart, "B", 110, 10},
			},
			limit:        1,
			noSetOp:      true,
			partialStart: targetStart,
			want: map[string]string{
				"A": StatusUnknown + "/" + WindowTarget, "B": StatusOutsideTop + "/" + WindowBaseline,
			},
			lookups: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, f := newFake(t, windowCalls(tc.calls...)...)
			if tc.noSetOp {
				delete(f.caps.Fields, "method")
			}
			if tc.partialStart != 0 {
				f.partial = func(path string, q url.Values) bool {
					return path == "/stats" && q.Get("from") == fmtInt(tc.partialStart) && (!tc.lookupOnly || isLookup(q))
				}
			}
			res, err := c.Compare(background, CompareInput{
				Target: targetHour, Filters: Filters{Service: "prod/billing"}, Limit: tc.limit, Order: OrderCountDesc,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, statusOf(res))

			var lookups int
			for _, q := range f.requests("/api/v1/stats") {
				if isLookup(q) {
					lookups++
				}
			}
			assert.Equal(t, tc.lookups, lookups, "lookups")
		})
	}
}

func TestCompareLooksUpOnlyTheMissingKeys(t *testing.T) {
	c, f := newFake(t, windowCalls(
		group{baseStart, "a.A.run()", 100, 10}, group{baseStart, "a.B.run()", 90, 10},
		group{targetStart, "a.A.run()", 80, 10}, group{targetStart, "a.B.run()", 120, 10},
	)...)
	res, err := c.Compare(background, CompareInput{
		Target: targetHour, Filters: Filters{Service: "prod/billing"}, Limit: 1, Order: OrderCountDesc,
	})
	require.NoError(t, err)
	var lookups []url.Values
	for _, q := range f.requests("/api/v1/stats") {
		if isLookup(q) {
			lookups = append(lookups, q)
		}
	}
	require.Len(t, lookups, 2)
	byFrom := map[string]url.Values{}
	for _, q := range lookups {
		byFrom[q.Get("from")] = q
	}
	assert.Equal(t, []string{"namespace=prod", "service=billing", `method=~^(?:a\.B\.run\(\))$`}, byFrom[fmtInt(baseStart)]["filter"],
		"the baseline lacks B, the target's top group")
	assert.Equal(t, []string{"namespace=prod", "service=billing", `method=~^(?:a\.A\.run\(\))$`}, byFrom[fmtInt(targetStart)]["filter"])
	assert.Equal(t, "1", byFrom[fmtInt(baseStart)].Get("limit"))

	require.Len(t, res.Groups, 2)
	b := res.Groups[0]
	assert.Equal(t, "a.B.run()", b.Key, "B changed more: 30 more calls of 10 ms against 20 fewer of A")
	assert.Equal(t, int64(90), b.Baseline.Count)
	assert.Equal(t, int64(120), b.Target.Count)
	assert.InDelta(t, 5, b.TimeMsPerMinDelta, 1e-9)
	assert.False(t, b.Slower, "more calls, not slower ones")
	assert.Contains(t, res.Notes, "only groups in the top 1 of either window are compared; a larger limit compares more")
	assert.Contains(t, res.Notes, "no group got slower by 20% or more and none is new, so no call trees were sampled")
	assert.Nil(t, res.Sample)
	assert.Empty(t, f.requests("/api/v1/calls"))
}

// slowTree is a call of Billing.charge whose time is in Repo.load and its
// SQL; the target's tree spends more in the SQL.
func slowTree(totalMs, sqlMs int64) *calltree.Tree {
	b := &treeBuilder{}
	root := b.node("a.Billing.charge", totalMs,
		b.node("a.Repo.load", sqlMs+10,
			sqlNode(b, "a.Jdbc.execute", calltree.ParamGroup{Value: "select * from orders where id = 1", DurationMs: sqlMs, Executions: 1})))
	return &calltree.Tree{Methods: b.methods, Params: []string{"sql"}, Root: root}
}

func TestCompareSamplesTheGroupsThatGotSlower(t *testing.T) {
	calls := windowCalls(
		group{baseStart, "a.Billing.charge", 10, 100}, group{baseStart, "a.Health.check", 50, 2},
		group{targetStart, "a.Billing.charge", 10, 300}, group{targetStart, "a.Health.check", 50, 1},
		group{targetStart, "a.Export.run", 2, 50},
	)
	for i := range calls {
		switch {
		case calls[i].row.Method != "a.Billing.charge":
		case calls[i].row.TsMs < targetStart:
			calls[i].tree = slowTree(100, 60)
		default:
			calls[i].tree = slowTree(300, 250)
		}
	}
	c, f := newFake(t, calls...)
	res, err := c.Compare(background, CompareInput{
		Target: targetHour, Filters: Filters{Service: "prod/billing"}, Sample: 4,
	})
	require.NoError(t, err)

	st := statusOf(res)
	assert.Equal(t, map[string]string{
		"a.Billing.charge": StatusCompared, "a.Health.check": StatusCompared, "a.Export.run": StatusNew,
	}, st)
	assert.Equal(t, "a.Billing.charge", res.Groups[0].Key, "the largest change first")
	assert.True(t, res.Groups[0].Slower)
	assert.False(t, res.Groups[1].Slower, "Health.check got faster")
	assert.Equal(t, StatusNew, res.Groups[2].Status)

	// Only the slower and the new groups are sampled, in both windows.
	lists := f.requests("/api/v1/calls")
	require.Len(t, lists, 2)
	for _, q := range lists {
		assert.Equal(t, `method=~^(?:a\.Billing\.charge|a\.Export\.run)$`, q["filter"][2])
		assert.Equal(t, "4", q.Get("limit"))
	}
	s := res.Sample
	require.NotNil(t, s)
	assert.Equal(t, []string{"a.Billing.charge", "a.Export.run"}, s.Groups)
	require.NotNil(t, s.Baseline)
	assert.Equal(t, int64(10), s.Baseline.Of)
	assert.Equal(t, int64(12), s.Target.Of)
	assert.Equal(t, 4, s.Target.Trees)

	require.NotEmpty(t, s.Methods)
	top := s.Methods[0]
	assert.Equal(t, "a.Jdbc.execute", top.Method)
	assert.InDelta(t, 60, top.BaselineSelfMsPerCall, 1e-9)
	assert.InDelta(t, 250, top.TargetSelfMsPerCall, 1e-9)
	assert.InDelta(t, 190, top.DeltaMsPerCall, 1e-9)
	assert.Equal(t, 4, top.BaselineCalls)
	require.Len(t, s.SQL, 1)
	assert.InDelta(t, 190, s.SQL[0].DeltaMsPerCall, 1e-9)
}

func TestCompareWindows(t *testing.T) {
	c, f := newFake(t)
	res, err := c.Compare(background, CompareInput{Target: targetHour})
	require.NoError(t, err)
	assert.Equal(t, time.UnixMilli(baseStart), res.Baseline.From, "the default baseline is the window before")
	assert.Equal(t, time.UnixMilli(targetStart), res.Baseline.To)

	res, err = c.Compare(background, CompareInput{Target: targetHour, BaselineShift: 24 * time.Hour})
	require.NoError(t, err)
	assert.Equal(t, time.UnixMilli(targetStart).Add(-24*time.Hour), res.Baseline.From)

	n := len(f.reqs)
	for _, in := range []CompareInput{
		{Target: targetHour, Baseline: hour, BaselineShift: time.Hour},
		{Target: targetHour, BaselineShift: -time.Hour},
		{Target: targetHour, BaselineShift: 30 * time.Minute},
		{Target: targetHour, Baseline: Window{From: time.UnixMilli(baseStart), To: time.UnixMilli(targetStart + 1)}},
		{Target: targetHour, GroupBy: GroupByTime},
		{Target: targetHour, Sample: MaxSample + 1},
		{Target: targetHour, Filters: Filters{Conditions: []string{"no operator"}}},
	} {
		_, err := c.Compare(background, in)
		assert.Error(t, err, "%+v", in)
	}
	assert.Len(t, f.reqs, n, "bad input sends no request")
}
