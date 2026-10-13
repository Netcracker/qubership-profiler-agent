# 09 — UI screens (Stage 5)

Per-screen specification for the Stage 5 UI: layout, components, interactions, states, and the URL scheme. It
builds on [`07-ui-design.md`](07-ui-design.md) (contract, tree engine, deployment) and
[`08-ui-backend-requirements.md`](08-ui-backend-requirements.md) (backend deltas); those carry the rationale,
this carries the concrete screens. The visual reference is the wireframe artifact; this document is the
text-of-record so the wireframe is not the only carrier.

Three screens sit inside one app shell: **Discovery + Calls**, **Call Tree**, and **Pods Info**. The **states**
they share are specified once in §5.

## 1. App shell

```text
┌─────────────────────────────────────────────────────────────────────────┐
│ ▮ Profiler   Calls           Time range: [ « │ Last 1 hour ▾ │ » │ ⊖ ]  ☾ │  header
├───────────┬───────────────┬─────────────────────────────────────────────┤
│ WORKSPACE◧│ Namespaces    │ [Apply]                                     │
│ ≡ Calls   │  ⌕ search     │ Calls                       ⤓ ▤ ⚙          │
│ ⬡ Pods    │  ▾ ns-1       │  All >10 >100 [>500] >3s >5s  ◱ errors ⌕    │  filters
│   Info    │    ☑ svc-a    │ ┌─────────────────────────────────────────┐ │
│           │    ☐ svc-b    │ │ calls table — keyset-paginated          │ │
│           │  ▸ ns-2       │ └─────────────────────────────────────────┘ │
└───────────┴───────────────┴─────────────────────────────────────────────┘
  navigation  discovery rail  content
  rail
```

| Region | Contents |
|--------|----------|
| Header | One blue bar on the data screens: the product name, the screen name, the screen's controls, and the theme switch at the far right. The call-tree route replaces it with a context header (§3). |
| Header controls | A slot that the active screen fills. Calls and Pods Info put the time range there: shift back, the period picker, shift forward, and zoom out (§2.2). |
| Navigation rail | Links to `Calls` and `Pods Info`. 196 px open, 52 px collapsed to icons; the state is kept in `localStorage`. Switching screens keeps the query string, so the time range and the selection carry over. Hidden on the tree route. |
| Discovery rail | The `namespace → service → pod` tree (§2.1), inside the screen. Collapsible. |
| Content | The active screen. |

**Theme.** The UI has a light and a dark theme. The switch in the header is a three-way choice with icons: Light,
Dark, and Auto. Auto is the default and follows the browser's `prefers-color-scheme`, live: an OS change applies
while the page is open. Light and Dark are stored in `localStorage` (`profiler.theme`) and win on every screen;
Auto removes the stored value. The tree route has no shell header, so its own header carries the switch. An HTML
export saves the mode in effect, so a copy exported under Auto opens in the mode Auto had resolved to.

**Fonts.** Inter for text at a 13 px base size, and JetBrains Mono as the fixed-width font for code and, through
the `.pf-mono` class, for numbers. Both are bundled as local `woff2` files: the UI is embedded in the query binary
and loads nothing from another host.

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

| Element | Behavior |
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

The time range lives in the header (§1): the period picker with an absolute range, quick ranges, and the display
time zone, between the shift-back and shift-forward buttons, followed by zoom out. A shift moves the window by
half its width, and zoom out doubles it around its center. Both commit an absolute window. Choosing a time range
applies at once (Grafana-style refetch on a range change).

`Apply` sits above the calls toolbar and carries only rail-selection edits. Only the rail service/pod selection is
`Apply`-gated — expanding a service into `pod` tuples is the expensive fan-out (02 §2.3). The toolbar's narrowing
filters (duration chips and expression, errors only, method query) write the URL and refetch immediately;
hide-system writes the URL but filters the loaded rows client-side (§2.3).

### 2.3 Calls table

| Group | Columns |
|-------|---------|
| Always | Start, Duration (heat dot + link to tree), Pod, Title (method + param tags) |
| Metrics (R1) | CPU, Suspend, Queue, Calls, Transactions, Disk IO, Net IO, Memory |

The metrics columns beyond CPU/Calls are the dropped-and-restored set (08 R1). Title shows the method plus a
tag for `sql`, `error`, and similar.

| Control | Behavior |
|---------|-----------|
| Duration chips | `All`, `>10ms`, `>100ms`, `>500ms`, `>3s`, `>5s`. Default `>500ms`, so page one is not sub-millisecond noise. Maps to `duration_min_ms`. This is the v1 "slowest" affordance (true ranking is 08 R2). |
| Errors only | Toggle → `error_only=true`. |
| Hide system/proxy | Toggle, default on: hides proxy/health/idle-async noise (the old UI's `idleTags`, `apps/agent/profiler-ui/src/dataFormat.mjs`). Separate from the duration default; the noise-hide, not the threshold, is what made old first pages readable. Client-side filter unless a backend `hide_system` flag is added. |
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

The tree route has no navigation rail, and this header replaces the shell's. From left to right:

| Element | Content |
|---------|---------|
| Back arrow | A link to Calls. An exported copy has no Calls screen and leaves the arrow out. |
| Product name | `Profiler`. |
| Identity | `namespace / service / pod`, with the pod name at full strength. |
| Endpoint chip | `METHOD /path`, from the root call's `web.method` and `web.url` params. Absent when the call has none. |
| Start time chip | The call's start in the display time zone. Its tooltip names the retention class. |
| Duration chip | The call's total duration. |
| Offline chip | `offline copy`, in an exported copy only. |
| Theme switch | At the far right (§1). |

The trace ID chip and the link-out buttons (`Open trace`, `Logs`) are the correlation seam — provisioned, wiring
deferred (07 §3.4). The buttons belong to the toolbar (§3.2), after `Download HTML`.

### 3.2 Tabs and toolbar

Four fixed tabs, drawn as line tabs: **Call Tree**, **Flame graph** (§3.6), **Hotspots**, **Parameters**. The old
Database and Gantt tabs are dropped. The open fixed tab is in the URL as `tab` (§6), so a shared link reopens it.

The `Incoming` / `Outgoing` / `Find usages` / `Local hotspots` results open as derived-view tabs after the fixed
ones, each with a close button. A derived tab holds the recipe `(op, method, category)` and re-derives from the
current model, so it follows Adjust and Setup edits. Derived tabs are client-side state only: they persist through
the 10b HTML export (07 §5.3), not the URL. Closing the active one returns to the fixed tab that the URL names.

The toolbar sits at the right end of the tab bar: `Adjust duration`, `Setup categories`, `Download HTML`. The slot
between `Setup categories` and `Download HTML` is reserved for `Explain` and stays empty until the assistant ships.
Without a tree (loading, cold call, error) there are no tabs; the toolbar then has a row of its own, with its
buttons disabled. The tree search belongs to the Call Tree tab, not to the toolbar.

### 3.3 Tree rows

Each row is a merged node (08 R5–R7), 28 px high. From left to right:

| Part | Content |
|------|---------|
| Caret | Expand / collapse. Expanding **skips degenerate chains** — a pass-through chain opens in one click to the next node that spends time or branches (07 §5.4). A leaf has an empty slot instead. |
| Three dots | The row's operations menu (§3.4). |
| Numbers | `total (self) ×calls`: the node's total duration, its self duration, and its direct executions. The suspension pair and the total executions are in the Ctrl+hover stats. |
| Signature | `package.Class.method(args)`, with the package dimmed. Clicking it selects the row; the text stays selectable, so the qualified name can be copied. `source:line` and `jar` parse from the method string on Ctrl+hover. |
| Chain badge | `+N` on a node that skips `N` pass-through levels; clicking it shows them. A revealed chain shows `−N`, which skips them again. |
| Category badge | The category name on the node where the category starts (§3.5). The nodes below inherit the category without a badge. |
| Share bar | 100 px for the whole call. The solid part is the node's self time and the light part is time in its children. Hidden on a narrow viewport. |
| Share | The node's total as a percentage of the call. |

**Highlighting is always on**; there is no switch for it. The thresholds are shares of this call, never absolute
times, because 4 s is normal for one call and a disaster for another:

| Number | Orange | Red | Bold |
|--------|--------|-----|------|
| Self time | from 10% of the call | from 25% of the call | with either color |
| Call count | from 10 | from 100 | with either color |
| Total | — | — | from 50% of the call |

The method name is bold when its self time is colored. A search match gets a yellow mark on the name, and the rows
that do not match are dimmed in place. `Mark red` tints the whole row.

**Params under a node** are a mini-tree, not flat values (08 R11): metadata rows (`node.name`, `java.thread`,
`memory.allocated`, …) and aggregated `sql` groups — top shapes by time plus an `::other` bucket, similar SQL
grouped by a normalized signature, binds nested under their SQL. Each one is a row under its method, before the
method's child calls: `time ×count`, the key as a badge, and the value, with SQL, XML, and JSON values colored. The
row truncates a long value, and a button opens the full one. The Parameters tab summarizes them.

**Connectors.** A rounded line runs from a method down to each method it calls. A parameter row has no line of its
own, because a parameter is not a call; only the line to the child calls passes it. `visible-rows.ts` records, per
row, which ancestor lines run past it, and the row turns that into one 18 px cell per level.

**The tab's controls**, above the rows:

| Control | Behavior |
|---------|----------|
| Search | Filters by method, numbers, and parameter text (`tree/search.ts`). Beside it: the match count, or the Ctrl+hover hint. |
| Legend | The two parts of the share bar: self time, and time in children. |
| Tree lines | Switch, on by default. Off hides the connectors and keeps the indent. |
| Row lines | Switch, on by default. Off hides the line between rows. |
| Help | A popover that takes one example row apart and explains the colors, the bar, the badges, parameter rows, the three-dot menu, and Ctrl+hover. |

The two switches are stored in `localStorage`. They are a reader's taste and not part of a view, so they are not in
the URL, and one choice holds for every call and every tab.

### 3.4 Per-node operations

Every operation is in the row's three-dot menu: **Incoming calls**, **Outgoing calls**, and **Get stacktrace**
first, then **Find usages**, **Local hotspots**, **Adjust duration**, **Add category**, and **Mark red**. Selecting
a row adds no buttons anywhere: there is no strip of operations beside the search field. Ctrl+hover on a node shows
the stats popover: self and total duration and suspension, invocations, average, `source jar`, and `line`.

### 3.5 Hotspots and categories

**Setup categories** tags a method by pattern; the category propagates down its whole subtree (a child tag
overrides). The tree shows it as a badge where it starts, in one of six theme colors. **Hotspots** is a flat self-time profile; when categories are set it groups by
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
listing and download, so the user does not lose access. Analyzing a dump as a tree is deferred (07 §8, 08 §10).

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
| Call Tree | `/ui/tree/:pk` | `pk` in the path (`ns:svc:pod:restart:file:off:rec`); `ts_ms`, `retention_class` as query hints; `tab` (`tree`, `flame`, `hotspots`, `params`) |

The `cursor` is never in the URL — it is transient scroll state and expires (07 §3.2).

On the tree route, `tab` names the open fixed tab. The default, `tree`, is left out of the URL, and a missing or
unknown value opens Call Tree. A tab switch replaces the history entry, so Back leaves the page instead of stepping
through tabs. Derived tabs never enter the URL (§3.2).

## 7. Non-goals for v1

Deferred, with the reasons in 07 §8: the summary Dashboard, ranked-by-duration and ranked-by-SQL views,
the rich `$param` query language, heap-dumps, authentication, the tracing/logs wiring (the seam stays), and
dump analysis (thread dump, stackcollapse, DBMS_HPROF, JFR).
