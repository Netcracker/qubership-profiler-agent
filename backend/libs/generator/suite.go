package generator

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/parser"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type (
	Suite struct {
		PodCount int
		Pods     []*Pod
		Vu2Pods  map[string]map[int]int // scenario -> vuId -> podId
		Shared   *LoadedData
	}
)

var mux sync.RWMutex
var suite *Suite

func PrepareSuite(ctx context.Context, opts Options, shared *LoadedData) (res *Suite, err error) { // singleton
	mux.Lock()
	defer mux.Unlock()
	if suite != nil {
		log.Info(ctx, "already has test suite with %d/%d pods (using %d tcp, %d td and %d top dumps)",
			len(suite.Pods), opts.PodCount, len(suite.Shared.TcpDumps), len(suite.Shared.TdDumps), len(suite.Shared.TopDumps))
		return suite, nil
	}

	res = &Suite{
		PodCount: opts.PodCount,
		Pods:     []*Pod{},
		Vu2Pods:  map[string]map[int]int{}, // cache of relations between VUs and generated pods
		Shared:   shared,
	}

	// generate data for pods at start
	if err == nil {
		for i := 0; i < opts.PodCount; i++ {
			pod := res.generatePod(i, opts)
			res.Pods = append(res.Pods, pod)
		}
		suite = res
	}

	log.Info(ctx, "prepared test suite with %d/%d pods (using %d tcp, %d td and %d top dumps)",
		len(res.Pods), opts.PodCount, len(res.Shared.TcpDumps), len(res.Shared.TdDumps), len(res.Shared.TopDumps))

	for i, pod := range res.Pods {
		log.Info(ctx, "pod #%d: name '%s', restart %d (tcp '%s')",
			i, pod.PodName, pod.Restart.UnixMilli(), pod.Dumps.Tcp.Origin.FileName)
	}

	return res, err
}

func (s *Suite) Pod(vuIndex int, scenario string) *Pod {
	mux.Lock()
	defer mux.Unlock()
	if _, has := s.Vu2Pods[scenario]; !has {
		s.Vu2Pods[scenario] = map[int]int{}
	}
	i := -1
	if _, has := s.Vu2Pods[scenario][vuIndex]; has {
		i = s.Vu2Pods[scenario][vuIndex]
	} else {
		i = s.available(scenario)
		//i = (vuIndex - 1) % len(s.Pods)
		fmt.Printf("* [scenario: %s] vu#%d got pod#%d/%d pods : %s _ %d \n",
			scenario, vuIndex, i, len(s.Pods), s.Pods[i].PodName, s.Pods[i].Restart.UnixMilli())
		s.Vu2Pods[scenario][vuIndex] = i
	}
	if i == -1 { // TODO return error?
		i = 0
	}
	return s.Pods[i] // 1 <= vu <= suite.PodCount
}

func (s *Suite) RandomTcpDump() *parser.ParsedPodDump {
	d := s.Shared.TcpDumps[rand.Intn(len(s.Shared.TcpDumps))]
	return &parser.ParsedPodDump{LoadedTcpData: d}
}

func (s *Suite) RandomTdDump() *DumpFile {
	return s.Shared.TdDumps[rand.Intn(len(s.Shared.TdDumps))]
}

func (s *Suite) RandomTopDump() *DumpFile {
	return s.Shared.TopDumps[rand.Intn(len(s.Shared.TopDumps))]
}

func (s *Suite) generatePod(podNum int, opts Options) *Pod {
	//pod := 1 + rand.Intn(opts.PodCount)
	pod := podNum
	//delta := rand.Intn(int(opts.Duration())) / 2
	ts := time.Now().Truncate(time.Second).Add(time.Duration(podNum) * time.Millisecond)

	return &Pod{
		Namespace: opts.Prefixes.Namespace,
		Service:   fmt.Sprintf("%s-%d", opts.Prefixes.Service, pod),
		PodName:   fmt.Sprintf("%s-%d_%d", opts.Prefixes.PodName, pod, ts.UnixMilli()),
		Restart:   ts,
		Dumps: PodDumps{
			Tcp: s.RandomTcpDump(),
			Td:  s.RandomTdDump(),
			Top: s.RandomTopDump(),
		},
	}
}

func (s *Suite) available(scenario string) int {
	n := len(s.Pods)
	assigned := s.Vu2Pods[scenario]

	avail := map[int]bool{}
	for p := 0; p < n; p++ {
		avail[p] = true
	}
	for _, v := range assigned {
		avail[v] = false
	}
	for k, v := range avail {
		if v {
			return k
		}
	}
	return -1
}
