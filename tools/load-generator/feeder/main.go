// Command feeder drives N virtual dumpers into a collector: every pod is one
// emulated agent running the full DumperThread state machine
// (backend/docs/design/virtual-dumper.md) — seven streams, 5 s flush cycles,
// reconnects with dictionary resend — with the load shape parameterized per
// load-testing-plan.md §4.
//
// Usage against the local stand:
//
//	kubectl -n profiler-load port-forward svc/profiler-backend-collector-agent 1715:1715 &
//	go run ./tools/load-generator/feeder -addr localhost:1715 -pods 20 -threads 4 -calls-per-sec 5 -duration 15m
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/vdumper"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func main() {
	var (
		addr      = flag.String("addr", "localhost:1715", "collector agent address")
		pods      = flag.Int("pods", 20, "emulated pods (one TCP connection each)")
		duration  = flag.Duration("duration", 15*time.Minute, "total run time; 0 runs until SIGINT")
		namespace = flag.String("namespace", "load", "emulated k8s namespace")
		service   = flag.String("service", "load-svc", "emulated service name")
		logLevel  = flag.String("log-level", "info", "trace|debug|info|warning|error")
		report    = flag.Duration("report", 30*time.Second, "stats report cadence; 0 disables")
		seed      = flag.Int64("seed", 1, "workload reproducibility seed")

		threads     = flag.Int("threads", 8, "producer goroutines per pod (app threads); 0 keeps pods idle")
		callsPerSec = flag.Float64("calls-per-sec", 5, "root calls per second per thread, jittered")
		dictInitial = flag.Int("dict-initial", 2000, "dictionary words known at startup")
		dictGrowth  = flag.Float64("dict-growth-per-min", 10, "new dictionary words per minute")

		durThresholds = flag.String("duration-thresholds", "100ms,1s,10s", "duration-class thresholds")
		durShares     = flag.String("duration-shares", "0.90,0.07,0.025,0.005",
			"duration-class shares; one more than thresholds")
		stackDepth  = flag.Int("stack-depth", 10, "mean stack depth (geometric)")
		sqlShare    = flag.Float64("sql-share", 0.2, "share of calls carrying an sql value")
		sqlBytes    = flag.Int("sql-bytes", 1024, "mean sql value size")
		sqlDedup    = flag.Float64("sql-dedup", 0.9, "sql value reuse probability (dedup hit rate)")
		xmlShare    = flag.Float64("xml-share", 0.05, "share of calls carrying an xml value")
		xmlBytes    = flag.Int("xml-bytes", 4096, "mean xml value size")
		suspendRate = flag.Float64("suspend-rate", 0.5, "suspend pauses per second per pod")
		errorShare  = flag.Float64("error-share", 0.01, "share of calls tagged call.red (any_error class)")
		cpuFrac     = flag.Float64("cpu-fraction", 0, "per-call cpu counter as a fraction of duration")
		waitFrac    = flag.Float64("wait-fraction", 0, "per-call wait counter as a fraction of duration")
		memBytes    = flag.Int("memory-bytes", 4096, "mean per-call memory.allocated counter")
	)
	flag.Parse()

	workload, err := buildWorkload(*durThresholds, *durShares, *stackDepth,
		*sqlShare, *sqlBytes, *sqlDedup, *xmlShare, *xmlBytes, *suspendRate, *errorShare, *dictGrowth)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	workload.CpuFraction = *cpuFrac
	workload.WaitFraction = *waitFrac
	workload.MemoryMeanBytes = *memBytes

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	ctx, err = log.SetLevelString(ctx, *logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	stats := newAggStats()
	if *report > 0 {
		go stats.reportLoop(ctx, *report)
	}

	fmt.Printf("feeder: %d pods × %d threads × %.1f calls/s -> %s for %s\n",
		*pods, *threads, *callsPerSec, *addr, *duration)
	var wg sync.WaitGroup
	for i := 0; i < *pods; i++ {
		cfg := vdumper.Config{
			Namespace: *namespace,
			Service:   *service,
			PodName:   fmt.Sprintf("%s-%d", *service, i),
			Connection: emulator.ConnectionOpts{
				ProtocolAddress: *addr,
				Timeout: profio.TcpTimeout{
					ConnectTimeout: 10 * time.Second,
					SessionTimeout: 24 * time.Hour,
					ReadTimeout:    30 * time.Second,
					WriteTimeout:   5 * time.Second,
				},
			},
			DictionaryInitial:    *dictInitial,
			ThreadsPerPod:        *threads,
			CallsPerSecPerThread: *callsPerSec,
			Seed:                 *seed + int64(i)*1000,
			Workload:             workload,
			Stats:                stats,
		}
		wg.Add(1)
		go func(i int, cfg vdumper.Config) {
			defer wg.Done()
			// Stagger the connects so a big fleet does not storm the listener.
			jitter := time.Duration(rand.Int63n(int64(2 * time.Second))) //nolint:gosec // startup spread, not crypto
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitter):
			}
			if err := vdumper.New(cfg).Run(ctx); err != nil && ctx.Err() == nil {
				fmt.Printf("feeder: pod %s stopped: %v\n", cfg.PodName, err)
			}
		}(i, cfg)
	}
	wg.Wait()
	stats.report("final")
	fmt.Println("feeder: done")
}

func buildWorkload(thresholds, shares string, stackDepth int,
	sqlShare float64, sqlBytes int, sqlDedup, xmlShare float64, xmlBytes int,
	suspendRate, errorShare, dictGrowth float64) (vdumper.Workload, error) {

	spec, err := vdumper.ParseDurationSpec(thresholds, shares)
	if err != nil {
		return vdumper.Workload{}, err
	}
	return vdumper.Workload{
		Duration:               spec,
		StackDepthMean:         stackDepth,
		RequestIdShare:         1.0,
		Sql:                    vdumper.BigParamSpec{Share: sqlShare, MeanBytes: sqlBytes, DedupHitRate: sqlDedup},
		Xml:                    vdumper.BigParamSpec{Share: xmlShare, MeanBytes: xmlBytes},
		SuspendPerSec:          suspendRate,
		ErrorShare:             errorShare,
		DictionaryGrowthPerMin: dictGrowth,
	}, nil
}

// aggStats aggregates the per-pod StatsListener events across the fleet and
// prints periodic totals — enough to eyeball a run without Prometheus.
type aggStats struct {
	mu           sync.Mutex
	bytes        map[string]uint64
	connects     int
	disconnects  int
	churns       int
	ackErrors    int
	dropped      int
	sessionReady durAgg
	ackFlush     durAgg
	started      time.Time
}

// durAgg keeps enough of a duration series for an average and a maximum.
type durAgg struct {
	n     int
	total time.Duration
	max   time.Duration
}

func (a *durAgg) add(d time.Duration) {
	a.n++
	a.total += d
	if d > a.max {
		a.max = d
	}
}

func (a durAgg) String() string {
	if a.n == 0 {
		return "n/a"
	}
	return fmt.Sprintf("avg %s, max %s", a.total/time.Duration(a.n), a.max)
}

func newAggStats() *aggStats {
	return &aggStats{bytes: map[string]uint64{}, started: time.Now()}
}

func (a *aggStats) Connected(int) { a.mu.Lock(); defer a.mu.Unlock(); a.connects++ }
func (a *aggStats) Disconnected(_ int, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.disconnects++
}
func (a *aggStats) Churned(int)                    { a.mu.Lock(); defer a.mu.Unlock(); a.churns++ }
func (a *aggStats) StreamOpened(string, int, bool) {}
func (a *aggStats) BytesSent(stream string, n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.bytes[stream] += uint64(n)
}
func (a *aggStats) AckError() { a.mu.Lock(); defer a.mu.Unlock(); a.ackErrors++ }
func (a *aggStats) Dropped(n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.dropped += n
}
func (a *aggStats) TcpConnected(time.Duration) {}
func (a *aggStats) SessionReady(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sessionReady.add(d)
}
func (a *aggStats) AckFlushed(_ string, d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ackFlush.add(d)
}

func (a *aggStats) reportLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.report("stats")
		}
	}
}

func (a *aggStats) report(prefix string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	elapsed := time.Since(a.started).Seconds()
	var total uint64
	streams := make([]string, 0, len(a.bytes))
	for s, n := range a.bytes {
		streams = append(streams, s)
		total += n
	}
	sort.Strings(streams)
	var b strings.Builder
	fmt.Fprintf(&b, "feeder %s: %.0fs, %.1f KB/s total, connects %d, reconnect-losses %d, ack-errors %d, dropped chunks %d\n",
		prefix, elapsed, float64(total)/elapsed/1024, a.connects, a.disconnects, a.ackErrors, a.dropped)
	fmt.Fprintf(&b, "  session-ready %s; ack-flush %s\n", a.sessionReady, a.ackFlush)
	for _, s := range streams {
		fmt.Fprintf(&b, "  %-12s %10.1f KB (%.2f KB/s)\n", s, float64(a.bytes[s])/1024, float64(a.bytes[s])/elapsed/1024)
	}
	fmt.Print(b.String())
}
