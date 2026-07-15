# Stage 5 progress

Stage 5 is the new profiler UI and the backend deltas it needs
(`07-ui-design.md`, `08-ui-backend-requirements.md`, `09-ui-screens.md`). This
document tracks the backend half: the R1/R5–R7/R11 requirements land here as
one phase per commit, R5 (server-side merge) before any React tree work — the
merge gates a usable tree. Status, decisions, and open issues per
`WORKFLOW.md` §7.

## Status

- [x] **Phase 1 — R1: project the dropped call columns** (02 §2.3, 08 R1)
  - [x] `libs/query/model` — `CallRow` / `CallJSON` carry `queue_wait_ms`, `suspend_ms`, `transactions`, `logs_generated`, `logs_written`, `file_read`, `file_written`, `net_read`, `net_written`
  - [x] `libs/query/cold` — the list-path `toCallRow` maps the columns `CallV2Projected` already read
  - [x] `libs/collector/hotstore` — the columns join `call_index` (additive ALTER for pre-upgrade partitions) and the insert path; `suspend_ms` is attributed at index time from the new in-RAM pause mirror (`PodRestart.SuspendPauses`), provisional like `error_flag` — the seal re-derives it
  - [x] `libs/collector/hotread` — `toCallRow` projects the new index columns
  - [x] `libs/tests/helpers/wire` — the calls generator writes format 4 (cpu/wait/memory, file/net, transactions/queue-wait)
  - [x] Test: `libs/tests/integration/parity_test.go` — the same synthetic calls render the identical `CallJSON` from `/internal/v1/calls` (SQLite index) and `/api/v1/calls` (sealed parquet), values asserted end to end
- [x] **Phase 2 — merged-v1 `Node` schema + MessagePack codec** (02 §2.5.3)
  - [x] `libs/calltree` — `Node` carries the uniform self/total pairs (`durationMs`/`selfDurationMs`, `suspensionMs`/`selfSuspensionMs`, `executions`/`selfExecutions`); `enterMsRel` is gone; the envelope stays `v: 1` (the "v1 redefined" note in 02 §2.5.3)
  - [x] `libs/calltree` — the codec re-numbers the Node fields per the new tag table; the decoder caps header-declared preallocations and canonicalises empty optional arrays to nil, so `Decode ∘ Encode` is a fixpoint
  - [x] `Build` fills the new fields per invocation for now — `executions = 1`, `selfDurationMs` computed at exit, suspension zero — the R5 merge (Phase 3) and the R7 timeline (Phase 4) land on top
  - [x] Tests: round-trip + wide values + the unknown-int-key fixture on the new numbers; `FuzzDecode` (error-never-panic on corrupted payloads, decode→encode→decode fixpoint on valid ones; 30 s local run clean)
- [x] **Phase 3 — R5/R6: server-side merge in `calltree.Build`** (08 R5–R6)
  - [x] `libs/calltree` — `Build` folds sibling invocations of one method under a parent into one node (`childIdx` lookup per parent), summing `durationMs` / `selfDurationMs` per invocation; `executions` rolls up bottom-up as `selfExecutions + Σ children.executions` — the old UI's `M_EXECUTIONS + M_CHILD_EXECUTIONS`
  - [x] The merged node carries everything the client collapse heuristic reads (07 §5.4, 08 §5): self/total duration, self/total executions for the fan-out check, and params presence; the collapse itself stays client-side
  - [x] Params concatenate across folded invocations in event order — the R11 aggregation (Phase 5) replaces this
  - [x] Tests: merge semantics on a three-invocation loop fixture, distinct siblings kept apart in first-seen order, self-recursion folding per level (never into an ancestor), hotspot flat-profile ranking, and an `assertMergeInvariants` walker (executions and self-duration arithmetic on every node)
- [x] **Phase 4 — R7: per-node suspension** (08 R7)
  - [x] `libs/calltree` — `Options.Suspend []SuspendInterval` feeds `Build`; the timeline is normalized (sorted, overlaps merged) once, and each invocation's `[enter, exit]` intersects it via binary search, split self/total like durations
  - [x] `libs/collector/hotread` — `GET /internal/v1/pods/{pod-restart}/suspend` serves the replica's `suspend.wal` RAM mirror (recovery reloads it, so recovered pod-restarts answer too) in the `suspend/v1` snapshot shape
  - [x] `libs/query` — the hot `/tree` branch fetches the timeline from the serving replica, the cold branch from the `suspend/v1` snapshot (`model.SuspendSnapshotKey`, now shared by the uploader and `cold.Suspend`); a missing snapshot or a pod-restart that left the replica degrades to zero suspension, transport errors are a 504
  - [x] Tests: `calltree` attribution suite (pause spanning a child boundary splits child/parent-self, merged invocations sum per work interval, out-of-order and overlapping timelines normalize, suspension invariants joined `assertMergeInvariants`); `tree_test.go` asserts per-node suspension end to end on both tiers
- [x] **Phase 5 — R11: param aggregation** (08 R11; contract formalised in 02 §2.5.3 first)
  - [x] `libs/calltree` — `Param` is a mini-tree: `groups []ParamGroup` (value, durationMs, executions, nested params, unresolved flag) replaces the flat value list; Param field numbers 1–2 enter the reserved registry
  - [x] `libs/calltree/params.go` — the aggregation ported from the Java `Hotspot`/`TreeBuilderTrace`: per-invocation attribution, SQL-signature group keys for `PARAM_BIG_DEDUP` params and `binds`, the online 256-group cap with smallest-into-`::other` eviction (a too-small newcomer folds straight in), binds nested under their invocation's SQL group
  - [x] `libs/calltree/msgpack.go` — `groups` at Param field 3; `ParamGroup` codec (nested params recurse, `unresolved` bool emitted only when true)
  - [x] Tests: the `sqlSignature` port pinned against the JS chain (literals with `''` escapes, digit-in-word cases), signature grouping + binds nesting, deterministic cap-3 eviction, and the acceptance fixture — 2000 distinct-signature SQL + 3 hot ones → 256 groups + `::other`, hot groups on top with nested binds; codec round-trip and fuzz re-run on the new shape

## Decisions log

- **2026-07-05 — hot-tier `suspend_ms` is attributed at index time.** The wire
  carries no per-call suspend field (both Go decoders leave
  `Call.SuspendDuration` zero), so the only source is the pod-restart's global
  pause timeline (`01` §5.1 step 4). The hot index intersects the call
  interval with the pauses known when the call is indexed, mirrored in RAM by
  `AppendSuspend` and reloaded from `suspend.wal` on recovery before the
  calls-WAL reconciliation. A pause that arrives after the insert is missed —
  the value is provisional exactly like the index `error_flag` and
  `retention_class`, and the seal-time derivation wins via the §6.3
  cold-preferred dedup. The alternative — recomputing at query time from the
  RAM mirror — was rejected: it needs pause access for recovered
  (non-live) pod-restarts on every list query, for a value the UI treats as
  indicative until the row goes cold.
- **2026-07-05 — Phase 2 ships the merged schema with per-invocation
  values.** `Build` keeps emitting one node per invocation until the R5 merge
  lands, but already through the merged-v1 wire shape (`executions = 1`,
  `selfDurationMs = durationMs − Σ children`, suspension zero). The schema
  and codec change once; the merge (Phase 3) and the suspension attribution
  (Phase 4) are then semantics-only diffs with no wire churn.
- **2026-07-05 — R7 data path: reuse the suspend artefacts both tiers already
  have.** `Build` takes the timeline as an explicit `Options.Suspend` input —
  the builder stays storage-agnostic. The hot tier serves it from the
  `suspend.wal` RAM mirror over a new internal endpoint (the mirror landed
  with Phase 1 for the index-time `suspend_ms`); the cold tier reads the
  `suspend/v1` snapshot the uploader has written since Stage 1 — no new
  storage. Both carry agent wall-clock Unix ms, the same clock as the trace
  timer epoch, so intervals intersect without translation. Degrade rule: a
  missing timeline (snapshot TTL'd, pod-restart left the replica between the
  blob fetch and the suspend fetch) renders the tree with zero suspension —
  the pre-R7 behaviour — while transport failures stay a 504.
- **2026-07-05 — merge keying is by method only.** The old UI merged by
  `(method id, signature)`; the signature axis served the dataflow analyzers,
  which Stage 5 defers (08 §10). Recursion cannot fold into an ancestor by
  construction — a node folds only into a sibling under the same parent, so
  depth structure is preserved and a self-recursive chain stays a chain.
- **2026-07-05 — the synthetic calls generator writes format 4.** The
  version-1 output could not carry the R1 counters, and the decoders
  (`libs/parser/pipe`, `libs/parser/streams`) have read formats 2–4 all along.
  Generated bytes are disposable by policy (`WORKFLOW.md` §6), so no fixture
  churn follows.

- **2026-07-05 — R11 deviations from the Java aggregation, and what was kept.**
  Kept: per-invocation attribution (a group gets one invocation's duration at
  most once), the 256 cap with smallest-first eviction into a per-param
  `::other` that never evicts, and the JS `signatures.sql` normalisation —
  including its quirk that purely numeric binds share one group. Changed:
  groups key per normalised value rather than per invocation value-set (the
  signature axis needs it; attribution stays once-per-invocation either way);
  eviction picks the true current minimum where the Java priority queue could
  act on a stale ordering; binds nest structurally under their invocation's
  most recent SQL group (the old UI only associated them visually), and binds
  whose SQL was folded into `::other` — or that had no SQL — surface as a
  top-level `binds` param. SQL-shapedness is detected from the wire
  (`PARAM_BIG_DEDUP`), not from a name list; `binds` is the one name-keyed
  param.

## Open issues

- The hot `/internal/v1/calls` row never carries `truncated_reason` /
  `trace_blob_size`: `CallIndexRow` does not read the partition's `blob_size`
  and `truncated_reason` columns, so a truncated call looks intact until it
  goes cold. Predates this stage; surfaced by the parity test (which passes
  because both tiers see un-truncated calls).
