# 09 — UI screens (Stage 5)

Per-screen specification for the Stage 5 UI: layout, components, interactions, states, and the URL scheme. It
builds on [`07-ui-design.md`](07-ui-design.md) (contract, tree engine, deployment) and
[`08-ui-backend-requirements.md`](08-ui-backend-requirements.md) (backend deltas); those carry the rationale,
this carries the concrete screens. The visual reference is the wireframe artifact; this document is the
text-of-record so the wireframe is not the only carrier.

Three screens sit inside one app shell: **Discovery + Calls**, **Call Tree**, and **Pods Info**. The **states**
they share are specified once in §5.

## 1. App shell

```
┌────────────────────────────────────────────────────────────────┐
│ Profiler              [ Calls · Pods Info ]                (AS) │  top bar
├───────────────┬────────────────────────────────────────────────┤
│ Namespaces    │ Period — quick ranges — Apply                   │  controls
│  ⌕ search     ├────────────────────────────────────────────────┤
│  ▾ ns-1       │ Calls                       ⤓ ▤ ⚙              │
│    ☑ svc-a    │  All >10 >100 [>500] >3s >5s   ◱ errors  ⌕ query│  filters
│    ☐ svc-b    │ ┌────────────────────────────────────────────┐ │
│  ▸ ns-2       │ │ calls table — keyset-paginated             │ │
│  ▸ ns-3       │ └────────────────────────────────────────────┘ │
└───────────────┴────────────────────────────────────────────────┘
```

| Region | Contents |
|--------|----------|
| Top bar | Product name; tab switch (`Calls`, `Pods Info`); user menu. The call-tree route replaces the bar with a context header (§3). |
| Left rail | The `namespace → service → pod` tree (§2.1). Collapsible; hidden on the tree route. |
| Controls | Period picker, quick ranges, `Apply`. Global to the data screens; data loads only on `Apply`. |
| Content | The active screen. |

**Routing.** One SPA served at `/ui` (`go:embed`, 07 §6).

| Route | Screen |
|-------|--------|
| `/ui/calls` | Discovery + Calls |
| `/ui/pods` | Pods Info |
| `/ui/tree/:pk` | Call Tree (new tab from a calls row) |

**URL is the source of truth.** The route encodes the frozen window (`from`, `to`), the selected services and
pods, and the active filters; the tree route adds the `pk` plus its `ts_ms` and `retention_class` hints. A
shared URL reopens the same view. The pagination `cursor` is scroll state, not URL state. Full scheme in §6.

## 2. Discovery + Calls

The entry screen. Pick services for a time range, then read their calls. Filter-first — no summary dashboard
(07 §1).

### 2.1 Left rail — service selection

| Element | Behaviour |
|---------|-----------|
| Search | Filters the tree by service name; essential at cluster scale. |
| Namespace row | Collapsible group; not selectable itself. |
| Service row | The selection unit. A checkbox with three states: none, all pods, or partial (some pods). |
| Pod rows | Drill-down under a service. A live/closed dot marks whether the pod-restart is still ingesting. Selecting individual pods sets the parent to the partial state. |

Selection defaults to the service, not the pod: pods are ephemeral (restart-hash suffix), people debug a
service (07 §4). Built from `/pods` (`{namespace, service, pod, restart_time_ms, time_min_ms, time_max_ms}`),
grouped client-side. A selected service shows its pod-restart count and boundaries, and a specific pod-restart
can be pinned from a calls row or the tree — the PK and cold lookup are pod-restart-scoped, so that identity
must stay reachable.

### 2.2 Controls

Period picker (absolute range) plus quick ranges (`15 min`, `1 h`, `2 h`, `4 h`) and `Apply`. Changing the
selection, range, or filters does not refetch until `Apply` — the fan-out is expensive.

### 2.3 Calls table

| Group | Columns |
|-------|---------|
| Always | Start, Duration (heat dot + link to tree), Pod, Title (method + param tags) |
| Metrics (R1) | CPU, Suspend, Queue, Calls, Transactions, Disk IO, Net IO, Memory |

The metrics columns beyond CPU/Calls are the dropped-and-restored set (08 R1). Title shows the method plus a
tag for `sql`, `error`, and similar.

| Control | Behaviour |
|---------|-----------|
| Duration chips | `All`, `>10ms`, `>100ms`, `>500ms`, `>3s`, `>5s`. Default `>500ms`, so page one is not sub-millisecond noise. Maps to `duration_min_ms`. This is the v1 "slowest" affordance (true ranking is 08 R2). |
| Errors only | Toggle → `error_only=true`. |
| Hide system/proxy | Toggle, default on: hides proxy/health/idle-async noise (the old UI's `idleTags`, `profiler-ui/src/dataFormat.mjs`). Separate from the duration default; the noise-hide, not the threshold, is what made old first pages readable. Client-side filter unless a backend `hide_system` flag is added. |
| Query | Method substring; `$param=value` is the later param-filter (08 R3). |
| Columns | Show, hide, reorder, resize; persisted in local storage. |
| Sort | By column, within loaded pages. Cross-range ranking needs 08 R2 — label the scope so it does not read as global. |
| Row → tree | Clicking the Duration link opens `/ui/tree/:pk` in a new tab, carrying `ts_ms` and `retention_class`. |

**Pagination.** Keyset (`cursor`), infinite scroll or an explicit "load more"; no offset, no page numbers.
An empty page with a non-null `next_cursor` keeps paging; an expired cursor restarts from page one (07 §3.2).

**Banners.** Wide-query guard and partial results render as first-class banners (§5), not silent failures.

**Timestamps** display in the browser timezone (as the old UI did); the URL window stays in Unix ms.

## 3. Call Tree

One call, drilled down. The tree is a route in the same SPA (07 §5), opened in a new tab so a wide tree does
not lose the list.

### 3.1 Context header

Replaces the top bar. Shows the breadcrumb (`service / pod`), the call identity chip (`time · duration`), the
`trace_id` chip, and the deep-link-out buttons (`Open trace · Tempo`, `Logs · Grafana`). The links are the
correlation seam — provisioned, wiring deferred (07 §3.4).

### 3.2 Tabs and toolbar

Tabs: **Call Tree**, **Hotspots**, **Parameters**. The old Database and Gantt tabs are dropped.

Toolbar: tree search, `Adjust duration`, `Setup categories`, `Download`.

### 3.3 Tree rows

Each row is a merged node (08 R5–R7):

| Part | Content |
|------|---------|
| Caret | Expand / collapse. Expanding **skips degenerate chains** — a pass-through chain opens in one click to the next node that spends time or branches (07 §5.4). |
| Duration bar | Proportional to the node's share. |
| Metrics | `total (self)` duration, `inv`, `×N` direct executions, child `calls` — every metric as self and total. |
| Signature | `Class.method(args)`; `source:line` and `jar` parse from the method string on Ctrl+hover. |
| Category | A colour spanning the tagged subtree, plus a badge (§3.5). |

**Params under a node** are a mini-tree, not flat values (08 R11): metadata rows (`node.name`, `java.thread`,
`memory.allocated`, …) and aggregated `sql` groups — top shapes by time plus an `::other` bucket, similar SQL
grouped by a normalised signature, binds nested under their SQL. Rendered inline and summarised in the
Parameters tab.

### 3.4 Per-node operations

The three core operations — **Incoming calls**, **Outgoing calls**, **Get stacktrace** — sit directly on the
selected row or toolbar; a row kebab (`⋮`, on hover) holds the rest: **Find usages**, **Local hotspots**,
**Adjust duration**, **Add category**, **Mark red**. Ctrl+hover on a node shows the stats popover: self and
total duration and suspension, invocations, average, `source jar`, and `line`.

### 3.5 Hotspots and categories

**Setup categories** tags a method by pattern; the category propagates down its whole subtree (a child tag
overrides) and colours it. **Hotspots** is a flat self-time profile; when categories are set it groups by
category first, then flat-profiles within — so a business operation's share of time and its SQL read directly
(07 §5.3).

### 3.6 Rendering

Virtualised over the flattened visible nodes; lazy expansion; degenerate-chain collapse; a size guard against
a pathological blob (07 §5.4). One indented tree in v1 — flame/icicle is deferred and, if built, must apply
the same degenerate-frame skipping.

## 4. Pods Info

A tab listing the pod-restarts behind the current selection: `namespace / service / pod`, restart time, the
data time range (`time_min_ms`–`time_max_ms`), and a live/closed marker. Source: `/pods`. Diagnostic dumps
(thread, TOP, GC, heap) are not browsed here — each pod row links out to `dumps-collector`, which owns their
listing and download, so the user does not lose access. Analysing a dump as a tree is deferred (07 §8, 08 §10).

## 5. States

With a fan-out backend these are frequent, so each names what happened and the one action that resolves it.

| State | Trigger | Message and action |
|-------|---------|--------------------|
| Empty | Selection resolves to no pods | "The selected services have no pods in this window." Calls otherwise opens on the default last hour; Pods prompts "Pick a period and Apply to see pods." |
| Loading | Fan-out in flight | Skeleton rows; "Querying hot replicas + cold tier." |
| Partial | `partial: true` | Shows the rows that returned, with a banner listing `partial_reasons`; offers retry. It does not invent a source count. |
| All sources failed | `504` | "No source answered in time. Narrow the range or retry." |
| Too wide | `400` wide-query guard | Turns `suggested_filters` into one-click narrowing chips, sized by the dominant `retention_class` (`by_class`). |
| Cold call | `404` on `/tree` | Hot lookup failed and the cold lookup needs `ts_ms`; `retention_class` sharpens pruning but is optional. "Reopen the call from its row so the hints travel with it." |
| Blob truncated | `404`, `truncated_reason` set | "The trace for this call was dropped under load — no tree to show." |
| Big params unresolved | tree carries `unresolved` params | Renders the tree but flags the affected params (the value segment was evicted before seal); does not present them as complete. |
| Tree too large | size guard trips | Renders a bounded view with a warning and offers the raw-trace download, rather than freezing the tab. |
| Expired | `400` expired cursor | "The scroll position expired. Reload from the first page." |
| Repeated empty pages | empty page, non-null cursor | Keeps paging for a bounded number of empty rounds, then surfaces "no more in this range" with a manual continue. |

Severity is encoded in a pill (grey / blue / amber / red): amber is handled and usable, red is blocked and
needs a choice.

## 6. URL scheme

| Screen | Path | Query / path params |
|--------|------|---------------------|
| Calls | `/ui/calls` | `from`, `to` (Unix ms); `pod` (repeatable `ns/svc/pod`); `service` (repeatable); `duration_min_ms`; `error_only`; `retention_class` (repeatable); `q` (method / `$param`) |
| Pods Info | `/ui/pods` | `from`, `to`; `service` (repeatable) |
| Call Tree | `/ui/tree/:pk` | `pk` in the path (`ns:svc:pod:restart:file:off:rec`); `ts_ms`, `retention_class` as query hints |

The `cursor` is never in the URL — it is transient scroll state and expires (07 §3.2).

## 7. Non-goals for v1

Deferred, with the reasons in 07 §8: the summary Dashboard, ranked-by-duration and ranked-by-SQL views,
the rich `$param` query language, heap-dumps, authentication, the tracing/logs wiring (the seam stays), and
dump analysis (thread dump, stackcollapse, DBMS_HPROF, JFR).
