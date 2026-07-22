# Soak invariant checker

Contract for `tools/load-generator/checker`: the watchdog that runs alongside a contract or soak run
(`load-testing-plan.md` §7.1, §7.4) and fails the run when a §8 invariant breaks. This document defines the data
sources, the violation model, and the pass/fail rule of every invariant; the implementation must not be trusted over
this contract.

The checker is a standalone binary run next to the runner, against port-forwarded endpoints (`doc/soak-runs.md` lists
the per-stand commands). It takes ready URLs and credentials through flags and environment variables and never
manages port-forwards or deploys anything.

## Violation model: latched, not last-seen

A soak verdict must reflect the whole run, not the final sample. Every invariant is a pure predicate over the current
state; the harness around it latches failures:

- Each poll tick evaluates every enabled invariant. A failed evaluation after warm-up creates (or updates) a latched
  violation record: `{invariant, subject, firstAt, lastAt, count, message}`. `subject` is the target URL, S3 prefix,
  marker PK, or pod name the invariant judged.
- A latched violation never clears. A collector whose RSS spiked over the limit for one tick fails the run even when
  the final tick looks healthy.
- Live output prints a violation line when a record is created and when it repeats; the final report prints every
  latched record with its first/last time and repeat count.
- Exit code is 0 only when the latch registry is empty at the end of the run; the final evaluation pass no longer
  decides anything on its own.

Samples taken during warm-up (`-warmup`, default 15m) are stored but never judged, as before.

## Sources

Each source is optional and enabled by its flags; with none of the new flags set the checker behaves as the §8.1–§8.4
metrics watcher.

| Source | Enables | Flags / env | Cadence |
| --- | --- | --- | --- |
| `/metrics` scrape | §8.1–§8.4, §8.6 | `-targets` (required) | `-interval` (30s) |
| S3 listing | §8.5 | `S3_ENDPOINT`, `S3_BUCKET`, `S3_PATH_PREFIX`, `S3_ACCESS_KEY[_FILE]`, `S3_SECRET_KEY[_FILE]` (same names the collector reads); enabled by `-s3` | `-s3-interval` (2m) |
| Query API probe | §8.7 | `-query-url` | `-interval` |
| Kubernetes pod list | §8.8 | `-kube-namespace` (enables), `-kube-selector` (default `app.kubernetes.io/name=profiler-backend`), kubeconfig or in-cluster config | `-interval` |

The S3 client is read-only: it must not create buckets or write objects (`s3.NewClient` calls `MakeBucket` and is
therefore not usable here — the checker builds a bare client from the same `s3.Params`).

### Scrape gaps

A target (metrics URL, S3 endpoint, query URL, or the k8s API) that fails for more than `-max-scrape-gap` consecutive
polls (default 3) after warm-up latches a `target-unavailable` violation. Silent absence must not hide a dead
component: §8.1–§8.7 skip a tick they have no data for, and this rule is what makes that skip safe.

## Invariants

### §8.1–§8.4 (implemented in phase 1, unchanged)

- **§8.1 hot-store-not-growing**: the per-target sum of the hot-store disk gauges must not grow monotonically by more
  than 5% over the `-window` (2h default).
- **§8.2 ingest-paused-not-sticky**: `profiler_backpressure_ingest_paused` nonzero in ≥ 1% of post-warm-up samples.
- **§8.3 no-refused-bytes**: `profiler_ingest_refused_bytes_total` must stay 0 at contract load.
- **§8.4 hot-window-lag-bounded**: `profiler_hotstore_hot_window_lag_seconds` must stay under `-max-hot-lag`.
  The gauge is the age of the **oldest** row still in the hot index, so its healthy level is
  `hot retention + eviction cadence`, not the seal latency; the budget is
  `hot retention + seal/upload chain + margin` (default 25m against the 15m default retention). Sustained growth
  past the budget means the hot→cold handoff is stuck.

These now latch like everything else; their predicates are unchanged.

### §8.5 S3 objects per hour prefix

Judges the listing of `parquet/v1/<class>/<yyyy>/<mm>/<dd>/<hh>/…` under the configured bucket and path prefix. Keys
are parsed with the same right-to-left rule as `libs/maintain` (`parseParquetKey`); keys that do not parse are
ignored, mirroring maintain's own tolerance for foreign objects.

The checker must not demand compaction before maintain could have run. Deadlines derive from the stand's timers,
passed as flags with the same names and defaults as the backend env
(`-time-bucket` 5m, `-time-bucket-grace` 30s, `-seal-check-interval` 15s, `-upload-check-interval` 30s,
`-maintain-check-interval` 5m, `-compaction-min-age` 30m, `-compaction-min-files` 4, `-compaction-delete-grace` 5m,
`-s3-settle-slack` 1m):

```text
objectsVisibleAt(bucket) = bucketEnd + timeBucketGrace + sealCheckInterval + uploadCheckInterval + settleSlack
compactionDueAt(bucket)  = max(objectsVisibleAt(bucket), bucketEnd + compactionMinAge)
                           + 2 × maintainCheckInterval + compactionDeleteGrace + settleSlack
```

Two maintain intervals, not one: a group settling right after a pass starts waits for the next pass to compact it,
and the write → grace → delete protocol removes its inputs one pass later still; back-to-back passes can also run
longer than the interval, which the slack absorbs.

Two sub-invariants:

1. **Compaction keeps up.** For every `(bucket, class)` group with `now ≥ compactionDueAt(bucket)`, the object count
   in the group must be ≤ `compactionMinFiles` (compaction residue below the trigger is legal by design). Groups
   before their deadline are never judged — and neither are groups whose newest LISTING predates the deadline: the
   evidence must postdate the deadline, or a group maintain compacted seconds ago reads as a miss. Every recurring
   one-shot §8.5 latch of the phase-5 fault runs (latched at `due+ε` from a listing at `due−75s`, count 1, gone by
   the next listing) was that race, not a compaction failure.
2. **Small-file share trends down.** Per hour prefix, the share of objects smaller than `-s3-small-file-bytes`
   (default 1 MB) is sampled on every listing. Once every bucket of the hour is past `compactionDueAt`, a share that
   grows monotonically across the sliding `-window` latches a violation. The window slides: one early drop cannot
   mask later unbounded growth.

Object-count and small-file series are also written to the checker log per listing, so a soak report can cite them.

### §8.6 collector RSS and goroutines

RSS, per metrics target: `process_resident_memory_bytes` must stay under `-rss-limit-bytes` (the pod memory limit;
required to enable the check — the limit is not exposed on `/metrics`), and must not grow monotonically over the
`-window` (same 5% tolerance as §8.1).

Goroutine trend, per metrics target that exposes `profiler_ingest_active_connections` (the collector; maintain and
query are exempt from this half): only samples where the same scrape returned both `go_goroutines` and the connection
gauge are used, so both series share time points. Over the `-window`:

```text
connections constant  ⇔  range(conns) ≤ max(0.01 × mean(conns), 2)
goroutines leaking    ⇔  fittedGrowth(window) > max(tolerance × mean, 10)
                          AND fittedGrowth(tail quarter) > allowed/4   (still climbing)
                          (tolerance: -goroutine-tolerance, default 0.10; judged only on
                           ≥ 8 points spanning the full window; the fit runs against real
                           scrape timestamps, so scrape gaps do not distort the slope)
```

The invariant fires when connections are constant and the goroutine count shows a sustained upward trend. When
connections move, the tick is not judged — §8.6 pins the leak signal, not the connection churn; a collector restart
drops the connection count, so restart-spanning windows fall out through the same gate.

Why a fitted trend and not the range: plan §8.6 asks for a *leak* signal, and a collector's goroutine count
legitimately oscillates at a constant connection count (seal/upload workers, scrape handlers) — the phase-4
verification soak latched a healthy 115–130 oscillation under the old `range > max(tolerance × mean, 10)` rule.
The §8.1-style strict `monotonicGrowth` is the opposite failure: per-scrape jitter of ±5 goroutines hides a real
+1-per-minute leak behind single-sample dips, so strict monotonicity never fires. A least-squares fit over the
window measures the trend through both kinds of noise. The still-climbing clause mirrors the runner's
`monotonic-growth` detector: growth that found a level (worker pools ramping after a start) is a startup shape, not
a leak; only growth whose tail quarter keeps its proportional share of the allowance fires. A tail thinned out by a
scrape gap (< 3 points) cannot prove the growth stopped, so the full-window verdict stands there.

### §8.7 sampled UI queries

All time windows are computed by the checker and sent as integer Unix milliseconds — `/api/v1` accepts nothing else
(`ParseWindow`, `libs/query/model/wire.go`).

- **Freshness**: every tick, `GET /api/v1/calls?from=<ms(now−probe)>&to=<ms(now)>&limit=…`; the newest `ts_ms` must
  be younger than `-freshness-budget` (default: `-max-hot-lag`). While the generator is feeding, the hot window must
  keep serving fresh rows.
- **Markers**: right after warm-up the checker samples `-marker-count` rows (default 20) from the earliest
  post-start window and records `(pk, ts_ms, retention_class)`. The `corrupted` class is excluded: it is reserved
  and no writer produces it today (`libs/query/model/tiers.go`). Every tick each marker is fetched with
  `GET /api/v1/calls/<pk>/trace?ts_ms=<ts>&retention_class=<class>`:
  - a 404 or 5xx for a marker younger than `classTTL − ttlMargin` latches a violation (old data must remain
    retrievable from cold until its TTL);
  - a marker older than its class TTL leaves the set.
  Class TTLs come from the same `PROFILER_RETENTION_*` environment variables the stand sets
  (`envconfig.Maintain.ClassTTLs()` semantics: unset keeps the tier-table default); `-ttl-margin` defaults to the
  §8.5 settle slack.
- **TTL deletion (optional)**: with `-expect-ttl-deletion`, a marker older than
  `classTTL + compactionDueAt-style settle` that still answers 200 latches a violation. Off by default; the
  accelerated-timer soak turns it on to validate the whole eviction chain.

### §8.8 no unexpected pod restarts

Kubernetes pods matching `-kube-namespace` + `-kube-selector` are listed every tick (client-go, kubeconfig or
in-cluster config — the `tools/migration/pkg/cleaner` pattern).

- **Baseline**: the first successful list. Restarts are not judged before it exists; if no list succeeds within
  `-max-scrape-gap` ticks, `kube-unavailable` latches.
- **Budget**: `-allowed-restarts` (default 0) is a total budget for the whole run:
  `Σ over containers of (restartCount − baselineRestartCount) + 1 per replacement pod`, where a replacement pod is a
  new UID matching the selector after the baseline; its own restart count adds on top. Exceeding the budget latches
  a violation naming the pods.
- A pod that disappears without a replacement latches its own violation.
- A failed list is treated like a scrape gap: the tick is not judged, the gap counter grows.

Healthy runs keep the default budget of 0. Injected restarts (T5) are NOT budgeted through this flag — a global
budget would let an unexpected restart hide behind an expected one. They arrive as scoped allowances from the
fault log (*Expected failures* below), matched per pod, per injection, per window.

## Expected failures (fault runs, phase 5)

T5/T7 runs inject failures on purpose; the checker must accept the *declared* consequences without accepting
anything else. The single mechanism is the scoped allowance, derived from the runner's fault log — never from a
global budget or a muted invariant.

- **Source**: `-faults-log <path>` points at the runner's `faults.jsonl`. The checker re-reads it on every tick,
  STRICTLY between the scrape phase and `evaluate()` — the tick order is scrape → read fault events → evaluate,
  so an injection that started before this tick's evaluation is always visible to it. A torn last line is
  ignored and re-read next tick.
- **Windows**: an injection's window opens at its `started` event and closes at `reverted` (stateful actions) or
  `ended` (instant actions) plus `settle`. An injection with no closing event yet is active: its window extends
  to now.
- **Observation time, not evaluation time**: every finding carries `observedAt` — the scrape, listing, probe, or
  pod-list time that produced the evidence — and the window match uses it. A violation measured before the
  injection's `started` stays unexpected even when the event lands between the scrape and the evaluation.
- **Scope**: an allowance is (signal × subject scope × window × budget). The `expects` list of the fault maps to
  invariants as follows:

  | `expects` entry | Invariant | Allowance semantics |
  | --- | --- | --- |
  | `restarts` | §8.8 | restart-or-replacement events for the TARGET pod, up to the injection's `restartBudget` (default 1), observed in the window; other pods, later events, and per-injection excess stay violations. A grace-0 collector kill measures at 2: the replacement plus one container restart when the fresh pod's first start collides with the dying process's `collector.lock` (T5.2) |
  | `scrape-gap` | target-available | gaps of the metrics target mapped to the target pod (`-target-pods`), in the window; unmapped targets never get this allowance |
  | `refused-bytes` | §8.3 | counter increments observed in the window are expected (and logged with their volume); increments outside stay violations |
  | `ingest-paused` | §8.2 | in-window samples leave the paused-ratio entirely (numerator and denominator) |
  | `hot-window-lag` | §8.4 | breaches observed in the window are expected |
  | `hot-store-growth` | §8.1 | the trend is not judged while a window overlaps the trend span (a mid-span outage makes the trend meaningless) |
  | `freshness`, `markers` | §8.7 | probe failures observed in the window are expected |
  | `compaction-lag` | §8.5 (1) | group deadlines shift by the closed windows' durations; groups are not judged while a window is open |
  | `small-file-share` | §8.5 (2) | listings observed in the window are not judged |
  | `ack-errors`, `pending-parquet-growth` | — | runner-side detectors; the checker does not watch them |

- **§8.3 predicate change**: `no-refused-bytes` judges counter *increments* between consecutive scrapes, not the
  cumulative value — a cumulative check would latch every tick after a legitimate, windowed refusal forever.
  The healthy-run behavior is unchanged (any increment from zero is a violation).
- **Latch and exit**: an expected finding latches with an `expected` mark; the final report prints expected and
  unexpected records as separate lists, and only unexpected records fail the run. Nothing is dropped — an
  expected latch is still evidence for the report.

`-target-pods` maps metrics targets to pod names for the scrape-gap scoping, e.g.
`-target-pods http://localhost:8082/metrics=profiler-backend-collector-1`.

## Exit and report

`checker: PASS` and exit 0 only when no *unexpected* violation latched. The final report lists every latched
violation (`invariant`, `subject`, first/last time, count, message), expected records under their own heading;
the exit code is 1 on any unexpected record. Flag errors exit 2.
