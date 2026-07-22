// Command checker watches a soak or contract run and fails with a report when
// a load-testing-plan.md §8 invariant breaks (contract: doc/checker.md). It
// polls the enabled sources on fixed intervals, evaluates every invariant on
// every tick, and latches violations: a failure after warm-up fails the run
// even when the final tick looks healthy.
//
// Sources beyond /metrics are optional: S3 listing (§8.5) via the collector's
// S3_* environment plus -s3, the query API probe (§8.7) via -query-url, the
// pod-restart watch (§8.8) via -kube-namespace, and the RSS limit (§8.6) via
// -rss-limit-bytes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	kenv "github.com/kelseyhightower/envconfig"

	appenv "github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

func main() {
	var (
		targets  = flag.String("targets", "", "comma-separated /metrics URLs to poll (collector replicas, maintain)")
		interval = flag.Duration("interval", 30*time.Second, "poll interval")
		warmup   = flag.Duration("warmup", 15*time.Minute, "run time excluded from invariant checks")
		window   = flag.Duration("window", 2*time.Hour, "sliding window for the trend invariants (§8.1, §8.5, §8.6)")
		maxLag   = flag.Duration("max-hot-lag", 25*time.Minute, "bound for hot_window_lag_seconds (§8.4): the gauge is the age of the OLDEST hot-index row, healthy at hot retention + eviction cadence; budget = hot retention + seal/upload chain + margin")
		duration = flag.Duration("duration", 0, "total run time; 0 runs until SIGINT/SIGTERM")

		maxScrapeGap = flag.Int("max-scrape-gap", 3, "consecutive failed polls of one source before target-unavailable latches")

		// §8.5: the S3 listing and the lifecycle timers its deadlines derive
		// from — same names and defaults as the backend env (doc/checker.md).
		s3Enabled       = flag.Bool("s3", false, "enable the §8.5 S3 checks; reads the collector's S3_* environment")
		s3Interval      = flag.Duration("s3-interval", 2*time.Minute, "S3 listing cadence (LIST is priced)")
		s3SmallFile     = flag.Int64("s3-small-file-bytes", 1<<20, "objects below this size count as small (§8.5)")
		s3SettleSlack   = flag.Duration("s3-settle-slack", time.Minute, "slack added to every §8.5 deadline")
		timeBucket      = flag.Duration("time-bucket", 5*time.Minute, "PROFILER_TIME_BUCKET of the stand")
		timeBucketGrace = flag.Duration("time-bucket-grace", 30*time.Second, "PROFILER_TIME_BUCKET_GRACE of the stand")
		sealInterval    = flag.Duration("seal-check-interval", 15*time.Second, "PROFILER_SEAL_CHECK_INTERVAL of the stand")
		uploadInterval  = flag.Duration("upload-check-interval", 30*time.Second, "PROFILER_UPLOAD_CHECK_INTERVAL of the stand")
		maintainCheck   = flag.Duration("maintain-check-interval", 5*time.Minute, "PROFILER_MAINTAIN_CHECK_INTERVAL of the stand")
		compactMinAge   = flag.Duration("compaction-min-age", 30*time.Minute, "PROFILER_COMPACTION_MIN_AGE of the stand")
		compactMinFiles = flag.Int("compaction-min-files", 4, "PROFILER_COMPACTION_MIN_FILES of the stand")
		compactDelGrace = flag.Duration("compaction-delete-grace", 5*time.Minute, "PROFILER_COMPACTION_DELETE_GRACE of the stand")

		// §8.6.
		rssLimit     = flag.Int64("rss-limit-bytes", 0, "pod memory limit; enables the §8.6 RSS check")
		goroutineTol = flag.Float64("goroutine-tolerance", 0.10, "relative fitted goroutine growth allowed over the window at a constant connection count (§8.6)")

		// §8.7: TTLs come from the PROFILER_RETENTION_* environment.
		queryURL        = flag.String("query-url", "", "query service base URL; enables the §8.7 sampled queries")
		freshnessBudget = flag.Duration("freshness-budget", 0, "max age of the newest call (§8.7); 0 uses -max-hot-lag")
		markerCount     = flag.Int("marker-count", 20, "pre-soak marker calls to track (§8.7)")
		ttlMargin       = flag.Duration("ttl-margin", 0, "safety margin before a marker's TTL (§8.7); 0 uses -s3-settle-slack")
		expectTTLDel    = flag.Bool("expect-ttl-deletion", false, "require markers to 404 after TTL + settle (§8.7, accelerated soak)")

		// §8.8.
		kubeNamespace = flag.String("kube-namespace", "", "namespace of the backend pods; enables the §8.8 restart watch")
		kubeSelector  = flag.String("kube-selector", "app.kubernetes.io/name=profiler-backend", "label selector for the §8.8 pod list")
		allowedRest   = flag.Int("allowed-restarts", 0, "budget for UNEXPECTED restart events (§8.8); injected restarts arrive as fault-log allowances, not through this flag")

		// Expected failures (fault runs; doc/checker.md).
		faultsLog  = flag.String("faults-log", "", "the runner's faults.jsonl; enables the expected-failure allowances")
		targetPods = flag.String("target-pods", "", "comma-separated <metrics URL>=<pod name> pairs scoping the scrape-gap allowance")
	)
	flag.Parse()
	if *targets == "" {
		fmt.Fprintln(os.Stderr, "checker: -targets is required, e.g. -targets http://collector-0:8081/metrics,http://collector-1:8081/metrics")
		os.Exit(2)
	}
	if *freshnessBudget == 0 {
		*freshnessBudget = *maxLag
	}
	if *ttlMargin == 0 {
		*ttlMargin = *s3SettleSlack
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	timers := s3Timers{
		timeBucket: *timeBucket, timeBucketGrace: *timeBucketGrace,
		sealCheckInterval: *sealInterval, uploadCheckInterval: *uploadInterval,
		maintainCheckInterval: *maintainCheck, compactionMinAge: *compactMinAge,
		compactionDeleteGrace: *compactDelGrace, settleSlack: *s3SettleSlack,
		compactionMinFiles: *compactMinFiles, smallFileBytes: *s3SmallFile,
	}

	st := &state{
		metrics: newHistory(*warmup, *window),
		gaps:    newGapTracker(*warmup),
	}
	if *faultsLog != "" {
		st.faults = newFaultState(*faultsLog)
	}
	podsByTarget := map[string]string{}
	if *targetPods != "" {
		for _, pair := range strings.Split(*targetPods, ",") {
			url, pod, ok := strings.Cut(strings.TrimSpace(pair), "=")
			if !ok || url == "" || pod == "" {
				fmt.Fprintf(os.Stderr, "checker: -target-pods entry %q is not <metrics URL>=<pod name>\n", pair)
				os.Exit(2)
			}
			podsByTarget[url] = pod
		}
	}

	var lister objectLister
	if *s3Enabled {
		var s3env appenv.S3
		if err := kenv.Process("", &s3env); err != nil {
			fmt.Fprintf(os.Stderr, "checker: S3 environment: %v\n", err)
			os.Exit(2)
		}
		params, err := s3env.Params()
		if err != nil {
			fmt.Fprintf(os.Stderr, "checker: S3 credentials: %v\n", err)
			os.Exit(2)
		}
		lister, err = newMinioLister(ctx, params, s3env.PathPrefix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "checker: S3 client: %v\n", err)
			os.Exit(2)
		}
		st.s3 = newS3State(timers, *window)
	}

	if *queryURL != "" {
		ttls, err := retentionTTLs()
		if err != nil {
			fmt.Fprintf(os.Stderr, "checker: retention TTLs: %v\n", err)
			os.Exit(2)
		}
		st.api = newAPIState(apiProbeConfig{
			baseURL:           strings.TrimRight(*queryURL, "/"),
			freshnessBudget:   *freshnessBudget,
			markerCount:       *markerCount,
			ttlMargin:         *ttlMargin,
			ttlSettle:         timers.settleChain(),
			expectTTLDeletion: *expectTTLDel,
			classTTL:          ttls,
		})
	}

	var pods podLister
	if *kubeNamespace != "" {
		var err error
		pods, err = newKubeLister(*kubeNamespace, *kubeSelector)
		if err != nil {
			fmt.Fprintf(os.Stderr, "checker: kubernetes: %v\n", err)
			os.Exit(2)
		}
		st.pods = newPodState(*allowedRest)
	}

	invariants := allInvariants(invariantConfig{
		maxHotLag:          *maxLag,
		rssLimitBytes:      *rssLimit,
		goroutineTolerance: *goroutineTol,
		maxScrapeGap:       *maxScrapeGap,
		targetPods:         podsByTarget,
	})

	fmt.Printf("checker: polling %s every %s; warmup %s, window %s\n",
		*targets, *interval, *warmup, *window)

	l := newLatch()
	run(ctx, runConfig{
		targets:    strings.Split(*targets, ","),
		interval:   *interval,
		s3Interval: *s3Interval,
		warmup:     *warmup,
		lister:     lister,
		pods:       pods,
	}, st, invariants, l)

	l.report()
	if n := l.unexpectedLen(); n > 0 {
		fmt.Printf("checker: FAIL — %d latched violation(s) (%d expected under fault allowances)\n",
			n, l.expectedLen())
		os.Exit(1)
	}
	if n := l.expectedLen(); n > 0 {
		fmt.Printf("checker: PASS — every latched violation (%d) matched a fault allowance\n", n)
		return
	}
	fmt.Println("checker: PASS — no invariant violations")
}

// retentionTTLs resolves the per-class TTLs the way the maintain service
// does: explicit PROFILER_RETENTION_* env wins, unset classes keep the
// tier-table defaults. The Maintain struct itself is not reused because its
// embedded S3 config would demand S3_ENDPOINT even with -s3 off.
func retentionTTLs() (map[string]time.Duration, error) {
	var env struct {
		Short    appenv.TTL `envconfig:"PROFILER_RETENTION_SHORT_CLEAN_TTL"`
		Normal   appenv.TTL `envconfig:"PROFILER_RETENTION_NORMAL_CLEAN_TTL"`
		Long     appenv.TTL `envconfig:"PROFILER_RETENTION_LONG_CLEAN_TTL"`
		Huge     appenv.TTL `envconfig:"PROFILER_RETENTION_HUGE_CLEAN_TTL"`
		AnyError appenv.TTL `envconfig:"PROFILER_RETENTION_ANY_ERROR_TTL"`
	}
	if err := kenv.Process("", &env); err != nil {
		return nil, err
	}
	out := model.DefaultClassTTL()
	for class, ttl := range map[string]appenv.TTL{
		model.RetentionShortClean:  env.Short,
		model.RetentionNormalClean: env.Normal,
		model.RetentionLongClean:   env.Long,
		model.RetentionHugeClean:   env.Huge,
		model.RetentionAnyError:    env.AnyError,
	} {
		if ttl > 0 {
			out[class] = time.Duration(ttl)
		}
	}
	return out, nil
}

type runConfig struct {
	targets    []string
	interval   time.Duration
	s3Interval time.Duration
	warmup     time.Duration
	lister     objectLister
	pods       podLister
}

// run polls until ctx is done, evaluating invariants on every tick so a
// violation is visible live, not only in the final report.
func run(ctx context.Context, cfg runConfig, st *state, invariants []invariant, l *latch) {
	tick := time.NewTicker(cfg.interval)
	defer tick.Stop()
	started := time.Now()
	var lastS3 time.Time
	for {
		now := time.Now()
		pastWarmup := now.Sub(started) >= cfg.warmup

		s, errs := scrapeAll(ctx, cfg.targets)
		for _, err := range errs {
			fmt.Printf("%s scrape: %v\n", now.Format(time.RFC3339), err)
		}
		for _, target := range cfg.targets {
			target = strings.TrimSpace(target)
			_, ok := s.targets[target]
			st.gaps.observe(target, ok)
		}
		st.metrics.append(s)

		if cfg.lister != nil && now.Sub(lastS3) >= cfg.s3Interval {
			lastS3 = now
			objects, err := cfg.lister.List(ctx)
			st.gaps.observe("s3", err == nil)
			if err != nil {
				fmt.Printf("%s s3 list: %v\n", now.Format(time.RFC3339), err)
			} else {
				sm := newS3Sample(now, objects, st.s3.timers)
				st.s3.append(sm)
				fmt.Printf("%s %s\n", now.Format(time.RFC3339), sm.logLine())
			}
		}

		if st.api != nil {
			err := st.api.poll(ctx, now, pastWarmup)
			st.gaps.observe("query-api", err == nil)
			if err != nil {
				fmt.Printf("%s query probe: %v\n", now.Format(time.RFC3339), err)
			}
		}

		if cfg.pods != nil {
			pods, err := cfg.pods.list(ctx)
			st.gaps.observe("kubernetes", err == nil)
			if err != nil {
				fmt.Printf("%s pod list: %v\n", now.Format(time.RFC3339), err)
			} else {
				st.pods.observe(pods, time.Now())
			}
		}

		// Tick order is scrape → fault events → evaluate (doc/checker.md):
		// an injection started before this evaluation must be visible to it,
		// and findings still match by their own observation times.
		if st.faults != nil {
			if err := st.faults.reload(); err != nil {
				fmt.Printf("%s faults log: %v\n", time.Now().Format(time.RFC3339), err)
			}
		}

		// Re-read the clock: the polls above (probe timeouts included) can
		// take tens of seconds, and the §8.5 deadlines compare against real
		// evaluation time.
		evaluate(time.Now(), pastWarmup, st, invariants, l)

		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// evaluate runs every invariant and latches the findings. Warm-up ticks are
// evaluated (the sources keep their own warm-up rules) but never latched.
func evaluate(now time.Time, pastWarmup bool, st *state, invariants []invariant, l *latch) {
	for _, inv := range invariants {
		for _, f := range inv.check(st) {
			if !pastWarmup {
				continue
			}
			label := "VIOLATION"
			if f.expected {
				label = "EXPECTED"
			}
			rec, isNew := l.record(now, inv, f)
			if isNew {
				fmt.Printf("%s %s %s (%s) %s: %s\n",
					now.Format(time.RFC3339), label, inv.name, inv.plan, f.subject, f.msg)
			} else if rec.count%10 == 0 && !f.expected {
				fmt.Printf("%s violation persists %s (%s) %s: %s (%d times)\n",
					now.Format(time.RFC3339), inv.name, inv.plan, f.subject, f.msg, rec.count)
			}
		}
	}
}
