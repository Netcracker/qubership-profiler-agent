package insight

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// t0 is the start of the synthetic windows, in Unix ms.
const t0 = int64(1760432400000)

var hour = Window{From: time.UnixMilli(t0), To: time.UnixMilli(t0 + 3_600_000)}

func TestSampleMergesTreesByMethodAndStatement(t *testing.T) {
	// Two calls of one endpoint. Each tree has its own method dictionary, so
	// the merge keys by name, not by index; Repo.load runs in both, Cache.get
	// in one.
	b1 := &treeBuilder{}
	first := b1.roundTrip(t, b1.node("void a.Api.handle() (Api.java) [api.jar]", 1000,
		b1.node("a.Repo.load", 700,
			sqlNode(b1, "a.Jdbc.execute",
				calltree.ParamGroup{Value: "select * from t where id = 1", DurationMs: 600, Executions: 3},
				calltree.ParamGroup{Value: "sql:3:17", DurationMs: 5, Executions: 1, Unresolved: true})),
		b1.node("a.Cache.get", 100)), "sql")
	b2 := &treeBuilder{}
	second := b2.roundTrip(t, b2.node("a.Repo.load", 300,
		sqlNode(b2, "a.Jdbc.execute",
			calltree.ParamGroup{Value: "select * from t where id = 22", DurationMs: 250, Executions: 2},
			calltree.ParamGroup{Value: "sql:9:2", DurationMs: 7, Executions: 1, Unresolved: true})), "sql")

	agg := newSampleAgg()
	agg.add(first)
	agg.add(second)
	assert.Equal(t, 2, agg.trees)

	methods := agg.rankedMethods()
	byName := map[string]SampledMethod{}
	for _, m := range methods {
		byName[m.Method] = m
	}
	assert.Equal(t, "a.Jdbc.execute", methods[0].Method, "the most self time first")
	assert.Equal(t, SampledMethod{MethodStat: MethodStat{Method: "a.Jdbc.execute", SelfMs: 862, Executions: 2}, Calls: 2},
		byName["a.Jdbc.execute"])
	assert.Equal(t, SampledMethod{MethodStat: MethodStat{Method: "a.Repo.load", SelfMs: 138, Executions: 2}, Calls: 2},
		byName["a.Repo.load"])
	assert.Equal(t, 1, byName["a.Cache.get"].Calls)
	assert.Equal(t, int64(200), byName["a.Api.handle()"].SelfMs, "names are short, as explain prints them")

	sql := agg.rankedSQL()
	require.Len(t, sql, 2)
	assert.Equal(t, SampledSQL{
		SQLStat: SQLStat{Statement: "select * from t where id = 1", DurationMs: 850, Executions: 5}, Calls: 2,
	}, sql[0], "statements that differ in literals fold across calls; the slowest one's text stays")
	assert.Equal(t, SampledSQL{
		SQLStat: SQLStat{Statement: "(statement text unavailable)", DurationMs: 12, Executions: 2}, Calls: 2,
	}, sql[1], "unresolved references name per-call entries, so they fold into one")
}

// hotspotCalls are three root methods: Billing.charge is where the time
// goes, Health.check is the most frequent.
func hotspotCalls() []fakeCall {
	var calls []fakeCall
	record := int32(0)
	add := func(method string, n int, durationMs int32) {
		for i := 0; i < n; i++ {
			record++
			calls = append(calls, call("billing", method, t0+int64(record)*1000, durationMs+int32(i), record))
		}
	}
	add("com.example.Billing.charge(long)", 5, 2000)
	add("com.example.Billing.refund(long)", 3, 500)
	add("com.example.Health.check()", 50, 2)
	return calls
}

func TestHotspotsRanksGroupsAndSamplesTheListedOnes(t *testing.T) {
	c, f := newFake(t, hotspotCalls()...)
	res, err := c.Hotspots(background, HotspotsInput{
		Window: hour, Filters: Filters{Service: "prod/billing"}, Limit: 2, Sample: 3,
	})
	require.NoError(t, err)

	stats := f.requests("/api/v1/stats")
	require.Len(t, stats, 1)
	assert.Equal(t, "method", stats[0].Get("group_by"), "the default grouping")
	assert.Equal(t, "duration_sum_desc", stats[0].Get("order"), "the default order")
	assert.Equal(t, "2", stats[0].Get("limit"))

	require.Len(t, res.Groups, 2)
	assert.Equal(t, "com.example.Billing.charge(long)", res.Groups[0].Key)
	assert.Equal(t, int64(5), res.Groups[0].Count)
	require.NotNil(t, res.Other)
	assert.Equal(t, int64(50), res.Other.Count)
	assert.Equal(t, int64(58), res.Totals.Count)
	assert.Contains(t, res.Notes, "1 more groups with 50 calls are summed as other; a larger limit lists more")

	// method has no `in`, so the sample selects the two listed groups with an
	// anchored alternation.
	lists := f.requests("/api/v1/calls")
	require.Len(t, lists, 1)
	assert.Equal(t, "duration_desc", lists[0].Get("order"))
	assert.Equal(t, "3", lists[0].Get("limit"))
	assert.Equal(t, []string{
		"namespace=prod", "service=billing",
		`method=~^(?:com\.example\.Billing\.charge\(long\)|com\.example\.Billing\.refund\(long\))$`,
	}, lists[0]["filter"])

	s := res.Sample
	require.NotNil(t, s)
	assert.Equal(t, []string{"com.example.Billing.charge(long)", "com.example.Billing.refund(long)"}, s.Groups)
	assert.Equal(t, int64(8), s.Of, "the sample is drawn from the listed groups' calls")
	assert.Equal(t, 3, s.Trees)
	require.Len(t, s.Calls, 3)
	assert.Equal(t, int32(2004), s.Calls[0].DurationMs, "the slowest calls")
	require.Len(t, s.Methods, 1)
	assert.Equal(t, SampledMethod{MethodStat: MethodStat{Method: "com.example.Billing.charge(long)", SelfMs: 6009, Executions: 3}, Calls: 3},
		s.Methods[0])
	assert.Contains(t, res.Notes, "the sample reads 3 of 8 calls; a larger sample reads more")
}

func TestHotspotsSamplesEveryCallWhenEveryGroupIsListed(t *testing.T) {
	c, f := newFake(t, hotspotCalls()...)
	res, err := c.Hotspots(background, HotspotsInput{Window: hour, Filters: Filters{Service: "prod/billing"}})
	require.NoError(t, err)
	assert.Nil(t, res.Other)
	assert.Empty(t, f.requests("/api/v1/config"), "no group was cut, so no restriction is needed")
	assert.Equal(t, []string{"namespace=prod", "service=billing"}, f.requests("/api/v1/calls")[0]["filter"])
	assert.Empty(t, res.Sample.Groups)
	assert.Equal(t, int64(58), res.Sample.Of)
	assert.Equal(t, DefaultSample, res.Sample.Requested)
}

func TestHotspotsSampleWithoutASetConditionIsUnrestricted(t *testing.T) {
	c, f := newFake(t, hotspotCalls()...)
	delete(f.caps.Fields, "method")
	res, err := c.Hotspots(background, HotspotsInput{Window: hour, Limit: 1, Sample: 2})
	require.NoError(t, err)
	assert.Nil(t, f.requests("/api/v1/calls")[0]["filter"])
	assert.Empty(t, res.Sample.Groups)
	assert.Equal(t, int64(58), res.Sample.Of)
	assert.Contains(t, res.Notes, "the sample is drawn from every call under the filters, not only the listed groups: "+
		"the server cannot select method values in one condition")
}

func TestHotspotsSkipsCallsWithoutATree(t *testing.T) {
	calls := hotspotCalls()
	calls[4].row.TruncatedReason = "buffer_full"
	calls[3].noTree = true
	c, _ := newFake(t, calls...)
	res, err := c.Hotspots(background, HotspotsInput{Window: hour, Filters: Filters{Service: "prod/billing"}, Sample: 6})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"1 calls have no tree (buffer_full)",
		"1 trees failed to load: no call with this reference: it aged out of retention, or the reference is wrong",
	}, res.Sample.Skipped)
	assert.Equal(t, 4, res.Sample.Trees)
}

func TestHotspotsChecksItsInputBeforeAnyRequest(t *testing.T) {
	c, f := newFake(t)
	for _, in := range []HotspotsInput{
		{GroupBy: GroupByTime},
		{Limit: MaxHotspotLimit + 1},
		{Order: "start_desc"},
		{Sample: MaxSample + 1},
		{Sample: -1},
		{Filters: Filters{Service: "billing"}},
	} {
		_, err := c.Hotspots(background, in)
		assert.Error(t, err, "%+v", in)
	}
	assert.Empty(t, f.reqs)
}

func TestGroupConditionPicksTheOperatorTheBuildHas(t *testing.T) {
	// A stage A build: in on service, =~ only on method, = only on params.
	stageA := model.FilterCapabilities{
		Fields: map[string][]model.Op{
			"service": {model.OpEq, model.OpIn},
			"method":  {model.OpMatch},
		},
		ParamOperators: []model.Op{model.OpEq},
	}
	cond, covered := groupCondition(stageA, "service", []string{"billing", "a,b"})
	assert.Equal(t, `service in billing,a\,b`, cond)
	assert.Equal(t, []string{"billing", "a,b"}, covered)

	cond, _ = groupCondition(stageA, "method", []string{"a.B.c(int)"})
	assert.Equal(t, `method=~^(?:a\.B\.c\(int\))$`, cond)

	cond, covered = groupCondition(stageA, "user.tenant", []string{"acme", "zeta"})
	assert.Equal(t, "user.tenant=acme", cond, "= on a param covers one key")
	assert.Equal(t, []string{"acme"}, covered)

	// This build has in on params, so one condition covers every key.
	cond, covered = groupCondition(model.Capabilities(), "user.tenant", []string{"acme", "zeta"})
	assert.Equal(t, "user.tenant in acme,zeta", cond)
	assert.Equal(t, []string{"acme", "zeta"}, covered)

	cond, _ = groupCondition(model.Capabilities(), "endpoint", []string{"GET /orders/{id}", "POST /orders"})
	assert.Equal(t, "endpoint in GET /orders/{id},POST /orders", cond)

	cond, covered = groupCondition(model.Capabilities(), "trace_id", []string{"ab", "cd"})
	assert.Equal(t, "trace_id=~^(?:ab|cd)$", cond, "trace_id has =~ and no in")
	assert.Equal(t, []string{"ab", "cd"}, covered)

	cond, covered = groupCondition(model.Capabilities(), model.GroupByParamName, []string{"user.tenant", "web.url"})
	assert.Empty(t, cond, "param_name keys are param names, which no one condition selects")
	assert.Nil(t, covered)

	cond, covered = groupCondition(model.FilterCapabilities{}, "service", []string{"billing"})
	assert.Empty(t, cond, "a build without filter capabilities has no set condition")
	assert.Nil(t, covered)
}

func TestTimelineMapsTheTimeBuckets(t *testing.T) {
	c, r := newReplay(t, nil, ok("stats_time.json"))
	to := time.UnixMilli(1760433300000).UTC()
	res, err := c.Timeline(background, TimelineInput{
		Window: Window{From: to.Add(-15 * time.Minute), To: to}, Filters: Filters{Service: "prod/billing"},
	})
	require.NoError(t, err)
	q := r.reqs[0].URL.Query()
	assert.Equal(t, "time", q.Get("group_by"))
	assert.Equal(t, "30", q.Get("buckets"), "the operation's default")
	assert.Equal(t, []string{"namespace=prod", "service=billing"}, q["filter"])
	assert.Equal(t, int64(600_000), res.BucketMs)
	require.Len(t, res.Buckets, 2)
	assert.Equal(t, time.UnixMilli(1760432400000).UTC(), res.Buckets[0].Start)
	assert.Equal(t, to, res.Buckets[1].End)
	assert.Nil(t, res.Buckets[1].P95Ms)
}
