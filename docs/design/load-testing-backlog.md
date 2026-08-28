# Load-testing findings backlog

Status: frozen at the phase-6 close-out of the load campaign (2026-07-20, `load-testing-plan.md` §16). Owner: @vlsi.

Every item below came out of the campaign (`load-testing-report.md`), is self-contained enough to run as its own
session, and is deliberately **not** implemented by the campaign itself. Run citations point at artifact directories
(`runs/<timestamp>-<name>/`, kept outside the repository).

Classification rule: a **defect** is code behaving worse than its own contract or breaking passes; a **requirement**
is behavior the contract states honestly but that is unacceptable at production scale and needs a new mechanism; an
**improvement** softens a documented, accepted behavior. Items the campaign decided *not* to pursue live in
`deferred.md` with explicit revisit triggers, not here.

## P1 — Global read-path memory budget and admission control in `query`

**Class:** backend requirement. Blocks the incident / concurrent-wide T6 profile at cluster scale
(`tools/load-generator/doc/cluster-checklist.md`) and any production deployment facing concurrent wide queries.

**Problem.** `PROFILER_MAX_SCAN_BYTES` is a per-request guard over *compressed* object bytes
(`02-read-contract.md` §2.3.2): decoding multiplies the footprint, and N concurrent guard-passing queries hold
N budgets of decoded state at once. The read path has no global memory bound, so concurrency converts an
individually safe budget into an OOM.

**Evidence.** 3 GiB query pod, 3 UI + 5 incident VUs on wide ranges: OOMKilled after 34 seconds. 2 Gi with mixed
cold + incident load: OOMKilled after ~29 minutes. Stable only at a 256 MB budget with the incident profile off
(`load-testing-report.md` §7, 2026-07-16 stand; every restart caught by checker §8.8).

**Shape (to be designed, not prescribed here).** A process-wide budget the fan-out and cold readers draw from, plus
an admission gate that queues or rejects (`503` or `partial` + `budget_exceeded`) when the budget is exhausted. The
per-request fail-soft backstop (`deferred.md`, trigger fired) complements it but cannot replace it.

**Done when:** a repeat of the report-§7 OOM shape (eight concurrent guard-passing wide queries against a pod-sized
budget) completes with bounded RSS, no OOMKill, and every rejected or truncated query answering with a documented
status; `02-read-contract.md` documents the mechanism and its config.

**Closed 2026-07-22** (`runs/20260722T104300Z-t6-p1-readbudget`, SHA `2d897510`, `load-testing-report.md` §7). The
report-§7 shape — eight concurrent guard-passing wide queries + three UI VUs + concurrent ingest, pod-sized budget
(`PROFILER_READ_MEMORY_BUDGET=1GB` on the 3 Gi query pod), report-§7 bucket density — held 27 minutes with query
RSS peaking at 120 MB (of 3 Gi) and zero OOMKills, against a 2026-07-16 pre-budget run that OOM-killed in 34 s at
the same scan volume. `read_budget_denials_total` stayed 0 (the page-lease batching caps in-flight decode; the
admission gate is a backstop, not the operating point). Two local, non-portable caveats carry to the cluster T6
run: the single local MinIO is the S3-throughput ceiling for concurrent wide reads, so a positive
narrow-stays-fast measurement waits for a real S3 endpoint; and the HTTP 500 under contention is a benign
client-cancel (`context.Canceled` surfaced as Echo 500 — a cosmetic 499 follow-up), not an undeclared status to a
waiting client. `02-read-contract.md` §7.5 documents the mechanism.

## P1 — Fast-path purge of near-empty pod-restarts

**Class:** backend requirement. Decided by the campaign's protection review (`load-testing-plan.md` §7.5.4,
`load-testing-report.md` §8): without it the storm backlog is unbounded.

**Problem.** WAL purge waits for the call-index partition drop — hot retention plus eviction cadence — so the
effective lag is `max(WAL_PURGE_GRACE, hot-index lag)` (`01-write-contract.md` §3.5, `03-lifecycle.md` §3.9). Under
a sustained reconnect storm the tracked pod-restart set grows at `restart rate × purge lag`, purge eligibility
itself degrades as the backlog ages, and churn restarts are near-empty by construction (a dictionary and seconds of
data).

**Evidence.** 40 churn pods at ~42 restarts/min: the backlog first plateaued at ~220 tracked restarts, then purge
throughput collapsed from 0.72/s to 0.35/s against steady 0.7/s production — 327 tracked restarts and 110 MB of WAL,
both still climbing when the `wal-bytes-growth` detector ended the run
(`runs/20260717T133845Z-t5-reconnect-storm`, `load-testing-report.md` §8). At the production 1 h grace the same
storm floors at ~2,500 tracked restarts before any degradation.

**Shape.** A purge path that frees a pod-restart's WAL set once its rows are sealed and its total size is below a
floor, skipping the hot-index wait; bounds the backlog at `rate × grace` regardless of hot-window drift. The
follow-ups the campaign declined — reconnect rate limits and a tracked-restart cap — stay in `deferred.md` and must
not ride along.

**Done when:** the T5.1 storm spec (`tools/load-generator/specs/t5-reconnect-storm.yaml`) re-run at its longest
duration holds tracked restarts and WAL bytes at a flat plateau (no `wal-bytes-growth` firing), and
`03-lifecycle.md` §3.9 documents the fast-path gate.

## P2 — Per-PUT timeout in the uploader

**Class:** defect (the retry loop's liveness depends on the ambient context, which the design did not intend).

**Problem.** An S3 PUT has no per-attempt timeout: a crawling PUT pins its upload worker until the ambient context
ends, so a degraded-but-alive S3 path can hold every worker on slow attempts instead of failing fast and retrying.

**Evidence.** T7 S3-slow (1 s latency + 32 KB/s per-connection bandwidth toxics, 15 min): upload workers sat pinned
on crawling PUTs for the duration (`runs/20260717T191126Z-t7-s3-slow`, `load-testing-report.md` §9). The run passed
— the regime is absorbed — but the missing bound is real.

**Done when:** each PUT attempt carries a bounded timeout (config with a sane default) after which the attempt is
abandoned and the file re-queued with backoff; a repeat of the S3-slow toxics shows workers cycling attempts rather
than pinning.

## P2 — Call-partition drop racing seal/upload passes

**Class:** defect (transient pass failures; self-healing, but the pass errors are real).

**Problem.** Dropping an aged call-index partition can race an in-flight seal or upload pass that still has the
partition ATTACHed, failing the pass with `SQL logic error: no such table: call_index`.

**Evidence.** One seal and one upload pass on one replica failed and self-healed on the next cycle during the first
T7 agent-net run; the loop-error counters account it and the backlog drained in minutes
(`runs/20260717T231036Z`, `load-testing-report.md` §9).

**Done when:** the drop path and the pass scheduler are audited and serialized (or the pass tolerates the drop
explicitly); a synthetic test drops a partition mid-pass without a pass error. Worth closing before the cluster
soak, where a 24–48 h run multiplies the exposure.

## P2 — Suspend/params pipe readers mis-frame multi-phrase streams

**Class:** defect, open since phase 2 (`load-testing-plan.md` §12).

**Problem.** The `suspend` and `params` pipe readers mis-frame multi-phrase streams the real agent produces; the
virtual dumper works around the shape, so load runs do not exercise the broken path, but real-agent traffic does.

**Evidence.** Found during phase-2 calibration against the real agent (`load-testing-plan.md` §12; the companion
ack-cadence defect from the same calibration was fixed in `82aed788`).

**Done when:** both readers decode a captured real-agent multi-phrase stream correctly under test, and the
calibration tap run reports no framing divergence.

## P3 — Collector recovery-duration metric

**Class:** backend requirement (observability). Time-to-READY is currently derivable only from fault logs and probe
transitions.

**Evidence.** T5.2/T5.3 measured recovery (10–30 s per kill) from the runner's `faults.jsonl` `readyAt` events and
probe flips — nothing on `/metrics` covers it (`runs/20260717T142638Z-t5-restart`,
`runs/20260717T152844Z-t5-crashloop`, `load-testing-report.md` §8).

**Done when:** the collector exposes a recovery-duration metric (histogram or gauge of last recovery) spanning
process start to READY, visible on the pipeline dashboard; the crashloop spec can assert on it instead of log
scraping.

## P3 — Soften the `collector.lock` collision on hard kills

**Class:** improvement. The behavior is documented and accepted (`01-write-contract.md` §8): a grace-0 kill costs
one extra crash cycle because the replacement's first start finds the dying process's flock still held.

**Evidence.** Every T5.2/T5.3 kill measured the two-restart shape; recovery stayed flat across ten cycles at
10–30 s including kubelet backoff (`runs/20260717T142638Z-t5-restart`, `runs/20260717T152844Z-t5-crashloop`).

**Done when:** the collector waits for the flock with a bounded timeout in-process (instead of exiting), removing
the extra restart without weakening the two-writers guarantee; the T5.2 spec's `restartBudget` drops from 2 to 1.

## Not in this backlog

- **Wire-protocol ack windowing / pipelining** and the **per-pod-key reconnect rate limit + tracked-restart cap**:
  deliberately deferred with triggers — see `deferred.md`.
- **Accept-side connection cap**: not a backlog item; the decision belongs to the cluster T3 run, with its criteria
  frozen in `tools/load-generator/doc/cluster-checklist.md`.
