package parser

import (
	"context"
	"sort"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type TsStat struct {
	Calls      int
	Traces     int
	Suspends   int
	SuspendSum int
}

func PodStat(ctx context.Context, p *ParsedPodDump) {
	min, max := time.Now().Add(24*time.Hour), time.UnixMilli(0)
	calls := 0
	traces := 0
	suspends := 0

	stat := map[time.Time]*TsStat{}
	list := []time.Time{}
	trunk := func(t time.Time) time.Time {
		if min.After(t) {
			min = t
		}
		if max.Before(t) {
			max = t
		}
		return t.Truncate(time.Second)
	}
	check := func(t time.Time) {
		if _, has := stat[t]; !has {
			stat[t] = &TsStat{}
			list = append(list, t)
		}
	}
	for _, call := range p.Calls.List {
		t := trunk(call.Time)
		check(t)
		stat[t].Calls++
		calls++
	}
	// for _, trace := range p.Traces.List {
	// 	t := trunk(trace.Time)
	// 	check(t)
	// 	stat[t].Traces++
	// 	traces++
	// }
	for _, susp := range p.Suspend.List {
		t := trunk(susp.Time)
		check(t)
		stat[t].Suspends++
		stat[t].SuspendSum += susp.Amount
		suspends++
	}

	log.Debug(ctx, " * Statistics: [%v - %v] Total %d calls, %d traces, %d suspends",
		min.Format(time.RFC3339Nano), max.Format(time.TimeOnly),
		calls, traces, suspends)

	sort.Slice(list, func(i, j int) bool {
		return list[i].Before(list[j])
	})

	for _, t := range list {
		log.Trace(ctx, " %v : %3d calls, %3d traces (%2d suspends with sum=%4d)",
			t.Format(time.RFC3339), stat[t].Calls, stat[t].Traces, stat[t].Suspends, stat[t].SuspendSum)
	}

}
