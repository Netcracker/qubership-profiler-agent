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

- [x] **Phase 6 — UI 5.0: scaffold** (07 §10 step 5.0)
  - [x] `apps/ui` — Vite 8 + React 19.2 + TypeScript (strict) + AntD 6.5 app, served under `/ui` (Vite base + router basename); routes `/ui/calls`, `/ui/pods`, `/ui/tree/:pk`; dev proxy of `/api/v1` to a running query, `dev:mock` runs against MSW
  - [x] Typed `/api/v1` client: wire models mirror `libs/query/model/wire.go` (R1 columns included), PK path codec plus the component-wise comparator (02 §2.3.1), RFC 7807 parsing with the guard extensions; wide-query and cursor rejections are recognised by the api.go titles and details
  - [x] Hand-written MessagePack decoder for the merged-v1 `/tree` envelope (02 §2.5.3): generic int-keyed-map layer that skips unknown keys (forward compat), typed mapping with dictionary bounds checks, preallocation and depth caps; mirror encoder for tests and the mock
  - [x] MSW mock = the contract: the same validation order and problem bodies as api.go / guard.go / cursor.go / tree.go; deterministic hash-derived dataset (no committed fixtures); keyset pages in the shared (ts_ms DESC, pk ASC) order; span-layer guard; cursor TTL and frozen-query mismatch; cold-404 and truncated-404 details
  - [x] URL-as-state utilities for the 09 §6 scheme; the pagination cursor never enters the URL
  - [x] Tests: decoder round-trip and malformed-payload suite, fast-check fuzz (totality on arbitrary and corrupted bytes; decode∘encode identity on synthetic trees), URL state round-trip; `tsc` strict and `vite build` clean

- [x] **Phase 7 — UI 5.1: discovery + calls** (07 §10 step 5.1)
  - [x] Left rail: namespace → service → pod tree grouped client-side from
    `/pods`, tri-state service checkboxes (AntD check conduction), search,
    live/closed markers, restart counts, pin-pod from a calls row
  - [x] Controls: absolute range picker + 15m/1h/2h/4h quick ranges; Apply
    commits the draft window and selection to the URL, which keys the fetch;
    a shared URL reopens and loads the same view
  - [x] Calls table: the full column set (Start, Duration with heat dot +
    new-tab tree link carrying the cold hints, Pod, Title with sql/error/no-
    trace tags, CPU, Suspend, Queue, Calls, Tx, Disk IO, Net IO, Memory);
    duration chips (>500ms default), errors-only, hide-system/proxy (the
    dataFormat.mjs idleTags list, client-side), method-substring query;
    show/hide/reorder/resize columns persisted in localStorage; client sort
    scoped to the loaded pages
  - [x] Keyset pagination: accumulate pages, cross-page PK dedup, bounded
    auto-follow of empty pages (3 rounds, then a manual continue), expired
    cursor → restart-from-page-one banner
  - [x] 09 §5 states wired: empty, loading, partial (reasons, no invented
    counts), all-sources-failed 504, too-wide with one-click narrowing chips
    and the by_class breakdown, expired cursor, repeated empty pages
  - [x] Pods Info: pod-restart listing (restart time, data range, live or
    closed) for the selected services
  - [x] Tests: hook-level MSW suites (pagination order + dedup, too-wide,
    partial, cursor expiry, bounded empty pages), DOM-level CallsPage suite
    (rows render; too-wide chip re-runs the query), pods grouping/expansion
    unit tests, cold-404 and hint-decoding endpoint tests — 40 green
  - [x] Virtualisation measured on an 1100-row page in the browser: AntD 6
    `virtual` keeps the DOM at the visible window (5–17 row elements), heap
    ~64 MB; no SlickGrid escalation needed. Frame-time profiling needs a
    visible tab (rAF/ResizeObserver are suspended in hidden tabs) — noted in
    open issues

- [x] **Phase 8 — UI 5.2: call tree v1** (07 §10 step 5.2)
  - [x] `/tree` decode → client model: wire executions split into the old
    self/child pair, params carried as the R11 mini-tree, unresolved flags
    surfaced; ids stable for expansion state
  - [x] `sortNode` ported verbatim from profiler.mjs:6186 — child ordering
    (duration and self-duration comparators incl. the adjust-marker float),
    the collapse-levels protocol with its negative states (-1 fan-out, -2
    params/adjust pin, -3.. accumulate-above-a-break), and the bottom-up
    parent fan-out check; pinned by a behavioural test suite
  - [x] Visible rows replicate the old renderer: >10%-of-context subtrees
    auto-expand (root-scoped on first render, node-scoped on expand),
    pass-through chains skip on expand with a reveal affordance, params
    render as rows (groups, `::other` last, binds nested, unresolved tag)
  - [x] Virtualised render: fixed-row windowing (~60 LOC, no dependency) —
    params-as-rows keeps heights uniform, matching how the old UI rendered
    tags as tree items; 20k-row budget degrades expansion with a banner
    instead of freezing (07 §5.4)
  - [x] Search within the tree (search-elements.ts semantics: title +
    formatted numbers), ancestors force-expanded, chains revealed while a
    search is active; match count + highlight
  - [x] Node row: total(self) duration, suspension pair, ×N executions,
    shortened signature from the line_parser port (file:line + jar in the
    Ctrl+hover stats popover); kebab with Get stacktrace (modal + copy) and
    Mark red; the transform-backed operations arrive with 5.3
  - [x] Tree page: context header (identity, ts/duration/class chips, raw
    trace download), tabs Call Tree · Hotspots (5.3) · Parameters (whole-tree
    group aggregation); 09 §5 states — cold-404 told apart from
    truncated-404 by the backend problem titles, unresolved-params and
    too-large banners
  - [x] Tests: sortNode behaviour suite, line_parser port cases,
    initial-expansion/chain-skip/param-row/search fixtures, MSW page tests
    (hinted decode renders, cold state, truncated state) — 69 green

- [x] **Phase 9 — UI 5.3: the five computations** (07 §10 step 5.3)
  - [x] `transforms/flat-profile` — computeFlatProfile port: per-method
    aggregation within business-category contexts, the recursive-occurrence
    guard (self time always, totals once per outermost occurrence), param
    groups merged per (key, value); zero-self methods counted, not shown
  - [x] `transforms/merge` — outgoingCalls (mergeTopDown: occurrences merge
    into one subtree, nested self-recursion folds into the root, totals
    recomputed bottom-up), incomingCalls (mergeBottomUp: rooted at the
    method, grows to the callers, the time[] subtraction counts recursion
    once, optional category filter), findUsages (top-down caller paths with
    the minLevel guard against double-counting shared prefixes)
  - [x] `transforms/adjust` — factor/fraction config parser ('*' wildcards,
    longest pattern wins), the cascading-k walk with prevSelf* stash,
    ancestor totals and param-group kTags rescale; local hotspots =
    flat-profile over outgoingCalls
  - [x] `transforms/categories` — config parser ('>' assigns children, hsl
    palette, longest pattern wins), effective category propagated down with
    child overrides; drives row colouring and category-first hotspots
  - [x] UI: Hotspots tab (per-category sections, share bars, incoming pivot),
    ops on the tree rows (incoming/outgoing on hover + kebab: find usages,
    local hotspots, adjust quick-add, add category quick-add) opening a
    Drawer with a derived TreeView or profile; Adjust duration and Setup
    categories modals; a what-if banner while adjustments are active; the
    model rebuilds from the wire per config change, keeping transforms pure
  - [x] Tests: 13-case transform suite over a synthetic recursive fixture —
    hotspot ranking and category split, usage paths and recursion
    attribution, incoming/outgoing shapes and param merges, adjusted totals
    with cascade and param rescale, category propagation/overrides — 82
    green overall

- [x] **Phase 10 — UI 5.4: deploy** (07 §10 step 5.4)
  - [x] `apps/ui/embed.go` — `go:embed all:dist` behind `ui.Dist()`; a Go
    build without the npm step still compiles (committed `dist/.gitkeep`)
    and the query command serves /api/v1 with a warning instead of /ui
  - [x] `libs/query` — `Options.UI fs.FS`; /ui and /ui/* serve the bundle
    with an SPA fallback to index.html (client routes deep-link and
    refresh), immutable caching for the hashed assets/, gzip per route;
    /api/v1, /metrics, and the health routes untouched; covered by
    handler-level tests over an fstest.MapFS (traversal, fallback, caching,
    UI-less 404)
  - [x] Dockerfile — a node stage builds the bundle, so `docker compose up
    --build` produces a UI-carrying image with no host toolchain;
    `.dockerignore` keeps node_modules out of the context
  - [x] `tools/ui-seed` — emulated agents feed the dev stack over the real
    TCP protocol (dictionary, traces with nested enter/exit and sql tags,
    format-4 calls covering every R1 counter and both sides of the >500ms
    chip), then poll /api/v1/calls until the rows serve
  - [x] it-e2e — `playwright.query.config.ts` + `e2e-query/query-ui.spec.ts`
    against the embedded UI: discovery rail from /pods, service selection +
    Apply, duration-chip filtering, the new-tab tree drill with the cold
    hints in the URL, params and Hotspots on the tree page; `make query-ui`
    orchestrates stack-up → seed → test → down

- [ ] **Phase 11 — UI polish pass** (findings review, WP-A…WP-F)
  - [x] WP-A: tree row relaid out to the old order — operations menu (with the
    direction arrow) left of the bar, bar hugging the number, visible label
    `Class.method(args)` with a hidden copyable package; `parseMethod`
    hardened against spaces in arg lists; mock seed emits `(A,B)` like the
    agent
  - [x] WP-B: the pass-through reveal is reversible — every revealed chain
    node carries a `⤴N` fold tag; the head's fold restores the exact
    pre-reveal visible-row set
  - [x] WP-C: Ctrl/Cmd stats render in a fixed overlay keyed off the hovered
    node — the row DOM never remounts, so a text selection survives the
    modifier
  - [x] WP-D: Hotspots is a bottom-up tree again — dotted category names
    group hierarchically (`transforms/hotspot-tree.ts`), and a method row
    expands in place into its incoming callers via the lazy `notComputed`
    graft (old M_NOT_COMPUTED / Tree__computeIncoming); the one-shot
    incoming Drawer button is gone
  - [x] WP-E 10a: derived views live as closeable tabs (old dynamic_tabs)
    next to Call Tree · Hotspots · Parameters, each carrying its direction
    and re-deriving from the current model on Adjust/Setup changes; the
    one-shot Drawer is gone. 10b (state-restoring download) is scoped as an
    open issue below, pending a decision
  - [x] WP-F: virtualiser scale verified — the long-call mock seed now grows
    deep/wide trees; measurements below, no code regression found

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

- **2026-07-05 — UI data layer is a thin typed fetch, not RTK Query.** The
  contract removes everything a declarative cache would manage: data loads
  only on an explicit Apply (09 §2.2), pages 2..N are driven by an opaque
  cursor whose query is frozen server-side (02 §2.3.1), and `/tree` is
  immutable binary already covered by HTTP caching. RTK Query would need a
  custom baseQuery for MessagePack plus serializeQueryArgs/merge gymnastics
  for cursor accumulation, and the bundle ships inside the query image
  (07 §6), so the smaller dependency set wins. Revisit only if the app grows
  cross-screen server state that needs invalidation.
- **2026-07-05 — the MSW mock mirrors the backend's problem bodies
  verbatim.** The UI recognises rejections by the api.go title/detail texts
  ("query too wide"; details naming the cursor), so the mock reproduces the
  exact validation order and wording of api.go / guard.go / cursor.go /
  tree.go rather than inventing its own. When the real service and the mock
  disagree, the mock is right by definition — fix the backend or the
  contract, not the mock.

- **2026-07-05 — Apply gates the window and the selection; toolbar filters
  commit immediately.** 09 §2.2 words the no-refetch-until-Apply rule over
  "selection, range, or filters", but the too-wide rejection turns
  `suggested_filters` into one-click chips (09 §5) — a chip that still waits
  for Apply is not one click. Resolution: the expensive axes (service/pod
  selection, period) stay Apply-gated; the narrowing axes (duration chips,
  errors-only, retention classes, method query, hide-system) write the URL
  and refetch at once. `/pods` also follows the *draft* window as it changes,
  so services are selectable before the first Apply — it reads manifests,
  not parquet, and is not the fan-out the rule protects.
- **2026-07-05 — AntD 6 virtual table is enough for the calls list.**
  Measured against the mock with 1100 rows loaded: the virtual body keeps
  5–17 row elements in the DOM regardless of loaded count, heap stays
  ~64 MB. Keyset paging bounds the dataset itself, so the SlickGrid
  escalation path (07 §5.4 analogue for the list) stays closed. The app
  shell clamps to the viewport (`height: 100vh`) so the virtual body is the
  only scroller — with `minHeight` the page itself scrolled and the
  virtualiser never saw a bounded viewport.

- **2026-07-05 — params render as rows, so the tree virtualiser stays
  fixed-height.** The old UI rendered a node's tags as tree items, not as
  variable-height cells; keeping that shape means every row (node or param
  group) is one fixed-height line, and a ~60-LOC windowing component
  replaces a dynamic-height virtualiser dependency. The 09 §3.3 "params as
  mini-tree" reads exactly the same to the user: groups indent under the
  node, binds indent under their SQL group, `::other` last.
- **2026-07-05 — sortNode ported verbatim, including the negative-state
  protocol.** The -1/-2/-3.. states and the "levels resume accumulating
  above a break" behaviour are pinned by tests rather than simplified to
  the doc's 10%-heuristic summary (08 §5 explicitly asks for the exact
  logic). One UI-level addition, not in the old code: an active search
  reveals every skipped chain, because a match inside a skipped
  pass-through node would otherwise be unreachable.

- **2026-07-05 — 5.3 port deviations from profiler.mjs, recorded per 08 §5's
  "document any deviations".** (1) The old Tree__makeAdjustments never
  assigned `newDuration` (`var newDuration;` — a latent bug that wrote
  `undefined` into M_DURATION of adjusted nodes); the port computes the
  intent, child duration + scaled self. (2) The old hotspots grouped
  methods under javaModules package nodes; 07 §5.3 specifies category →
  flat, so the package grouping is dropped. (3) Merge identity is methodIdx
  alone — the signature axis went away with the server-side merge keying
  decision above. (4) Param merging extends the old flat tag merge to the
  R11 group mini-tree (values merge recursively, binds under their SQL).

- **2026-07-05 — the e2e run caught a real §2.2 divergence: echo hands path
  params over still percent-encoded.** 02 §2.2 pins the pk segment as
  percent-encoded, and a JS client's encodeURIComponent escapes the ':'
  separators to %3A — which `ParsePKPath` then rejected with "expected 7
  colon-separated components". The Go smoke test never saw it because
  `url.PathEscape` leaves ':' literal. Fixed by decoding in a `pkParam`
  helper in front of both point handlers; the MSW mock had decoded all
  along, so this was the backend diverging from the contract and the mock,
  exactly the failure mode the mock-is-contract policy is for. The full
  query-ui suite passes against the compose stack after the fix
  (discovery → selection → chips → tree drill → params → hotspots).

- **2026-07-05 — WP-A row layout: deviations from profiler.mjs.** (1) The
  direction arrow (`arrowthick-1-se`/`-nw`) doubles as the operations menu as
  before, but the on-hover incoming/outgoing quick buttons and the row-end
  kebab of the first port are gone — one left-side menu holds every action,
  so nothing scrolls off screen at depth. (2) The visible label drops the
  return type entirely; the old UI appended ` : ReturnType` after the args
  for non-void methods. It stays in the row title and the Ctrl+hover stats.
  (3) The hidden-but-copyable package span uses `font-size: 0` instead of the
  old off-screen 1-px `span.p` — same selection behaviour, no global CSS
  class. (4) `parseMethod` splits the word at the first space after stripping
  the source/jar suffixes, so an arg list containing spaces (`(A, B)`) parses
  instead of collapsing to the raw string; real dictionary words never carry
  the space (`line_parser.go:44`), and the mock seed now matches the agent
  (`(A,B)`).

- **2026-07-05 — WP-B reveal marks the whole chain, not just the head.** The
  first port revealed only the clicked node, so an interior chain node's own
  (shorter) skip immediately re-hid the levels below it — the old renderer
  threaded an `uncollapsed` flag down the chain for exactly this. Reveal now
  marks the head and the interior nodes; each of them shows a `⤴N` fold tag
  (a UI addition — the old renderer folded only from the head's icon), and
  the fold prunes exactly what the reveal added, so the skipped state
  returns bit-for-bit.

- **2026-07-05 — WP-C stats moved from a Popover wrap to a fixed overlay.**
  Wrapping the hovered row in `<Popover open>` reparented the row when the
  modifier went down, and the browser drops a text selection whose nodes
  move — the user could never Cmd-copy the label. The stats now render in a
  `position: fixed` bottom-right layer (pointer-events none) keyed off the
  hovered node; nothing in the row subtree changes, so the selection
  survives. Deviation from the old UI: the old tooltip floated next to the
  cursor; anchoring a floating layer to the row without touching its DOM
  costs positioning logic the fixed panel avoids.

- **2026-07-06 — WP-D restores the hotspot grouping hierarchy dropped by
  5.3 deviation #2, with corrections to the record.** The old UI split
  category (and module) names on '.' only when grouping hotspots
  (`allocateGroupNode`); the tree colouring never split — so the hierarchy
  returns as a hotspots-grouping feature, and `categories.ts` matching is
  untouched. Deviations from profiler.mjs: (1) the old javaModules package
  grouping stays dropped (07 §5.3 — category-first only); (2) group order
  follows the profile weight order (heaviest first, `unsorted` last) instead
  of re-sorting groups and methods together with sortNode, so headers never
  chain-collapse; (3) the flat AntD table (a port-era interim) is replaced by
  the shared TreeView in bottom-up direction, method rows carrying their
  aggregated params as expandable rows; (4) skeleton node ids are negative,
  like the old group nodes, so the positive ids the incoming graft mints
  cannot collide with them; (5) the zero-self footnote moves below the tree.

- **2026-07-06 — WP-F: the tree virtualiser scales; frame-time still needs a
  visible tab.** Setup: `synthetic.ts` grows long calls (>10 s) to depth 11
  with fanout 2–5, giving 1000–3000 visible rows once expanded (pinned by
  `tree-scale.test.ts`). Measured in the mock app (28.3 s call, tree search
  active so every chain is revealed): 1481 visible rows → 29–40 rendered row
  elements and ~450–570 total DOM nodes, the same order as the 22-row
  initial view (~394) — the DOM is O(visible window), not O(rows).
  `buildRows` over a 1933-node model producing 2948 rows takes 0.32 ms/run
  (vitest, M-series laptop), and it only reruns on expand/collapse/search —
  scrolling slices the memoised array (VirtualList keeps `rows` identity).
  rAF-based scroll frame timing was attempted twice (the preview browser and
  a claude-in-chrome tab) and both suspended rAF as hidden tabs — the same
  limitation the 5.1 calls-table note recorded; the 5.1 open issue now
  covers the tree view too. No regression found, no code changed beyond the
  seed.

- **2026-07-06 — WP-E 10a: derived-view tabs hold the recipe, not the
  result.** A tab stores `(op, methodIdx, category)`; the view derives from
  the current model in a per-tab cache keyed on the model instance. So an
  open tab follows Adjust/Setup edits like the old dynamic tabs did, while
  opening or closing an unrelated tab does not re-derive (a re-derivation
  mints fresh node ids and would wipe the other tabs' expansion state — the
  view remounts, via a derivation sequence number, only when its model
  actually changed). Deviation from profiler.mjs: the old tabs serialised
  the source node as a tree-path into the URL (`Tabs__scheduleCreate`);
  the new tabs are client-state only — URL persistence belongs to the 10b
  design below.

## Open issues

- **10b — downloadable self-contained HTML that restores state: design
  note, decision pending.** The old "download" posted the page state to the
  server (`profiler.mjs:3585-3593`: `pageState`, `adjustDuration`,
  `businessCategories`), which baked a single-page HTML; reopening it
  restored Adjust duration, Setup categories, and the created
  incoming/outgoing tabs through a read-only `ro` mode
  (`profiler.mjs:3537-3548`). The new UI is a static bundle embedded in the
  query binary, so the bake needs one of:
  1. *Backend endpoint* — `POST /api/v1/calls/{pk}/export` receives the
     serialised UI state, inlines the JS bundle, the MessagePack tree
     (base64), and the state into one HTML file. Pros: exact bytes of the
     already-served tree, works for cold calls while the server can still
     read them, output cacheable server-side. Cons: a new query endpoint
     and its guard/limits; the bundle must be re-inlined per build.
  2. *Client-side generator* — the running SPA assembles the HTML itself:
     it already holds the decoded wire, both configs, and the open tab
     specs; it embeds its own bundle (fetchable as a same-origin asset)
     plus the raw tree bytes and a `window.__restore` blob, then triggers a
     download. Pros: no backend change, works against any query version,
     the export equals what the user sees. Cons: the bundle fetch adds
     ~1 MB to the export; needs a boot path that prefers `__restore` over
     the router.
  State to serialise in either case: the call PK + hints, the adjust
  config text, the category config text, and the open derived tabs as
  `(op, methodIdx → method word, category)` — method *words*, not indexes,
  so a re-decode with a different dictionary order still resolves. Restore
  entry point: a `ro` boot flag that skips fetching, decodes the embedded
  tree, applies both configs, and replays the tab specs through the same
  `openOpTab` path. Leaning to option 2 (no new server surface, exports
  keep working after retention evicts the call), but NOT building it until
  the option is picked — this note is the 10b deliverable.
- The hot `/internal/v1/calls` row never carries `truncated_reason` /
  `trace_blob_size`: `CallIndexRow` does not read the partition's `blob_size`
  and `truncated_reason` columns, so a truncated call looks intact until it
  goes cold. Predates this stage; surfaced by the parity test (which passes
  because both tiers see un-truncated calls).
- Scroll frame-time profiling of the virtual calls table — and, as of WP-F,
  of the tree view — is pending a visible-browser session: hidden tabs
  suspend rAF and ResizeObserver delivery, so only structural metrics
  (bounded DOM, heap, buildRows cost) were measurable headlessly.
  Re-measure interactively before calling 5.1/WP-F performance done; the
  escalation options (headless virtualiser, SlickGrid) stay documented in
  the Stage 5 plan.
- The service→pods expansion for `/calls` uses the rail's `/pods` data,
  which follows the draft window; if the draft has moved past the committed
  window, a service selection can expand against slightly newer pod sets.
  Harmless at v1 cluster sizes; revisit if it ever surprises.
- 02 §2.7 words the `/pods` response as a bare array, but api.go returns
  `{ pods, partial, partial_reasons }` — the fan-out can partially fail on
  the pods path too, so the envelope is right and the doc sentence is stale.
  The UI and its mock follow the implementation; align 02 §2.7 when it is
  next touched.
