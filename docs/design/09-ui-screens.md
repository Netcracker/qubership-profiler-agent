# 09 — UI screens (Stage 5)

Per-screen specification for the Stage 5 UI: layout, components, interactions, states, and the URL scheme. It
builds on [`07-ui-design.md`](07-ui-design.md) (contract, tree engine, deployment) and
[`08-ui-backend-requirements.md`](08-ui-backend-requirements.md) (backend deltas); those carry the rationale,
this carries the concrete screens. The visual reference is the wireframe artifact; this document is the
text-of-record so the wireframe is not the only carrier.

Three screens sit inside one app shell: **Calls**, **Call Tree**, and **Pods Info**. The **states**
they share are specified once in §5.

## 1. App shell

```text
┌─────────────────────────────────────────────────────────────────────────┐
│ ▮ Profiler   Calls           Time range: [ « │ Last 1 hour ▾ │ » │ ⊖ ]  ☾ │  header
├───────────┬─────────────────────────────────────────────────────────────┤
│ WORKSPACE◧│ ⏷ duration > 500ms ✕  namespace = prod ✕  Add…  [Sort by ▾] │  filter bar
│ ≡ Calls   │ Duration All >10 >100 [>500] >3s >5s ◱ Errors ◱ System [Run]│  controls
│ ⬡ Pods    │ ┌─────────────────────────────────────────────────────────┐ │
│   Info    │ │ calls table — keyset-paginated                        ⚙ │ │
│           │ └─────────────────────────────────────────────────────────┘ │
└───────────┴─────────────────────────────────────────────────────────────┘
  navigation  content
  rail
```

| Region | Contents |
|--------|----------|
| Header | One blue bar on the data screens: the product name, the screen name, the screen's controls, and the theme switch at the far right. The call-tree route replaces it with a context header (§3). |
| Header controls | A slot that the active screen fills. Calls and Pods Info put the time range there: shift back, the period picker, shift forward, and zoom out (§2.2). |
| Navigation rail | Links to `Calls` and `Pods Info`. 196 px open, 52 px collapsed to icons; the state is kept in `localStorage`. Switching screens keeps the query string, so the time range and the filters carry over. Hidden on the tree route. |
| Content | The active screen. No screen has a filter sidebar: filters live in one bar above the content (§2.1). |

**Theme.** The UI has a light and a dark theme. The switch in the header is a three-way choice with icons: Light,
Dark, and Auto. Auto is the default and follows the browser's `prefers-color-scheme`, live: an OS change applies
while the page is open. Light and Dark are stored in `localStorage` (`profiler.theme`) and win on every screen;
Auto removes the stored value. The tree route has no shell header, so its own header carries the switch. An HTML
export saves the mode in effect, so a copy exported under Auto opens in the mode Auto had resolved to.

**Fonts.** Inter for text at a 13 px base size, and JetBrains Mono as the fixed-width font for code and, through
the `.pf-mono` class, for numbers. Both are bundled as local `woff2` files: the UI is embedded in the query binary
and loads nothing from another host. An HTML export embeds the font files too, so the copy keeps its fonts offline.

**Routing.** One SPA served at `/ui` (`go:embed`, 07 §6).

| Route | Screen |
|-------|--------|
| `/ui/calls` | Calls |
| `/ui/pods` | Pods Info |
| `/ui/tree/:pk` | Call Tree (new tab from a calls row) |

**URL is the source of truth.** The route encodes the window (`from`, `to`) and the applied filters; the tree
route adds the `pk` plus its `ts_ms` and `retention_class` hints. A shared URL reopens the same view. The
pagination `cursor` is scroll state, not URL state. Full scheme in §6.

## 2. Calls

The entry screen. Filter the calls of a time range, then read them. Filter-first — no summary dashboard (07 §1).
The layout follows the `CallsAlt.dc.html` board of the redesign mockups: the filter bar, a row of controls, and
the table. The histogram between the controls and the table comes later (redesign plan, task 3.8).

### 2.1 Filter bar

One token per condition, and all conditions must match. A condition is a field, an operator, and a value, in the
text form of the read contract's filter model (agent-access plan §5.1, draft): `duration>100ms`,
`namespace=prod-orders`, `pod in a,b`, `duration between 100ms..2s`. The grammar and the field catalog live in
`apps/ui/src/filter/`, the one place to change when the contract lands.

| Element | Behavior |
|---------|-----------|
| Input | Builds the next condition in three stages: field, operator, value. Each stage lists its options in a dropdown that narrows as the user types. |
| Fields | `duration`, `namespace`, `service`, `pod`, `endpoint`, `class`, `method`, `error`, `trace_id`, `request_id`, `retention_class`, and any other name as a call parameter, typed by hand. |
| Values | `in` and `not in` collect several values, applied with Enter. `None` and `Any` are offered for a field a call can lack. Namespace, service, and pod values come from `/pods` for the time range; other fields take the value as typed. A value that does not parse, such as a bad duration, shows an error line and adds nothing. |
| A word | A typed word that names no field offers `method =~ <word>`, quoted so that it matches as written, and then the word as a call parameter name. |
| A whole condition | Typing `duration>1s` offers it as one item, and so does a call parameter condition such as `user.tenant=acme`, whether or not value suggestions list that parameter. A bar narrowed to some fields, such as the one of Pods Info (§4), takes no call parameter. |
| Keyboard | Up and Down move the highlight, Enter picks, Escape closes the list, Ctrl+Enter runs the query. Backspace in an empty input steps back: a value, then the operator, then the field, then into the last token, which comes back with its value in the input for editing. |
| Disabled options | A field or an operator the backend cannot apply yet is listed but disabled, with the reason as a tooltip. |
| Disabled tokens | A condition from a link that the backend cannot apply yet shows as a crossed-out token with the reason. It is kept in the URL and not sent. |
| Browser-side tokens | A token that filters the loaded rows in the browser rather than the query, such as "Hide system/proxy" (§2.3), has a dashed outline and says so in its tooltip. |
| Preset tokens | A token the page owns, such as "Hide system/proxy", comes before the conditions. Its × removes it, and so does Backspace once no condition is left before the input. "Clear all filters" removes it too. |

**Before the `filter` parameter.** `/calls` takes no `filter` parameter yet, so the applied conditions travel as
its existing parameters (redesign plan §4.2.1):

| Condition | Sent as |
|-----------|---------|
| `namespace`, `service`, or `pod` with `=` or `in` | `pod` tuples, expanded from `/pods` in the browser and intersected across conditions, under the 6000-character request-line guard |
| `duration` with `>`, `>=`, `<`, `<=`, or `between` | `duration_min_ms` and `duration_max_ms`; a strict bound moves by 1 ms; several conditions intersect |
| `error=true` | `error_only=true` |
| `retention_class` with `=` or `in` | `retention_class`; several conditions intersect |
| `method =~ <word>`, the word without regular-expression syntax | `method`, the substring match; one per query |
| `trace_id = <id>`, `trace_id = None`, or `trace_id = Any` | Nothing: it filters the loaded rows in the browser (§2.3), from `apps/ui/src/filter/browser.ts`. `None` keeps the calls without a trace ID, and `Any` keeps the calls with one |

Everything else is disabled. Conditions that exclude each other, or identity filters that match no pod in the
window, send nothing and say so.

### 2.2 Controls

The time range lives in the header (§1): the period picker with an absolute range, quick ranges, and the display
time zone, between the shift-back and shift-forward buttons, followed by zoom out. A shift moves the window by
half its width, and zoom out doubles it around its center. Both commit an absolute window. Choosing a time range
applies at once (Grafana-style refetch on a range change).

**Draft and applied.** The filter bar, the duration chips, the two switches, and "Sort by" edit a draft. "Run
query", or Ctrl+Enter in the bar, applies it: the URL takes the draft and the query restarts from page one, even when
nothing changed. A relative time range such as `now-1h` resolves again on each run and when the range changes, so
the rerun reads up to the present; `/calls` and `/pods` share the resolved window, and "Load more" and a URL change
that filters only the loaded rows keep it. While the draft differs from the
applied query, a hint says "Filters changed. Run the query to update the results." Only the applied state reaches the
URL and `/calls`. A time range change and a row shortcut apply at once.

The filter bar, "Sort by", the duration chips, the switch labels, and "Run query" share one control height, 32 px
(`apps/ui/src/calls/control-height.ts`), so the two rows line up in both themes. A bar with more tokens than fit on
one line grows by whole token lines.

### 2.3 Calls table

| Group | Columns |
|-------|---------|
| Default | Start, Duration (heat dot + link to tree), Pod, Title (method + tags), `trace_id`, CPU, Queue, Calls, Net IO |
| Hidden by default (R1) | Suspend, Transactions, Disk IO, Memory |

The metrics columns beyond CPU/Calls are the dropped-and-restored set (08 R1). Title shows the method plus a tag
for `error`, `sql`, and `no tree` (the trace blob was dropped, `truncated_reason`).

Rows are 28 px high. Numbers and IDs use the 12 px fixed-width font. The heat dot before the duration is the
theme's ok color under 100 ms, warn under 1 s, mid under 3 s, and crit from 3 s on. The duration text is
right-aligned in a fixed six-character box, so the dot keeps one x position down the column.

**Pod and `trace_id` shortcuts.** A click on a row's pod or trace ID filters the calls by that value: the pod
name, or the full trace ID, of which the cell shows the first 12 characters. The table reports the click to its
page through `onPickFilter(field, value)`; a page that passes no handler gets plain text in both cells. The calls
page replaces that field's token with `field=value` and runs the query. `/calls` cannot filter by `trace_id` yet,
and a disabled token would make the click do nothing, so `trace_id=<id>` is not sent and filters the loaded rows in
the browser instead: by the ID the row's cell shows, with a dashed outline and a tooltip that says so, and a footer
count of the rows it hides. The pages loaded before the click are the rows it can find, so the click keeps them
rather than fetching page one again, unless the draft holds other unapplied changes; a call of the trace outside
them shows only after "Load more".

**`trace_id`.** A `/calls` row has no `trace_id` member (02 §2.3). The cell reads the ID from the call's own
params, under the names the tracing plugins record it with (`trace.id`, `brave.trace_id`, `X-B3-TraceId`), the
same names the tree searches (§3.1). A call without one reads "not traced". A tracer that records the ID on a frame
below the root leaves the row without it, although the tree still finds it; a `trace_id` in the row projection
closes that gap.

| Control | Behavior |
|---------|-----------|
| Duration chips | `All`, `>10ms`, `>100ms`, `>500ms`, `>3s`, `>5s`. A chip replaces every `duration` token with `duration > <chip>`; `All` removes them. A chip is lit when exactly one `duration >` token has its value. The default `>500ms` keeps page one free of sub-millisecond noise. This is the v1 "slowest" affordance (true ranking is 08 R2). |
| Errors only | A switch for the `error=true` token: it shows whether the token is present, and adds or removes it. |
| Hide system/proxy | A switch, on by default: hides proxy/health/idle-async noise (the old UI's `idleTags`, `apps/agent/profiler-ui/src/dataFormat.mjs`). Separate from the duration default; the noise-hide, not the threshold, is what made old first pages readable. It filters the loaded rows in the browser until `/config` serves the patterns for a `method !~` condition (redesign plan §4.6). The switch shows in the filter bar as the first token, `method !~ system/proxy`, with a dashed outline for "applies in the browser" and that limit in its tooltip. The token is part of the draft: the switch adds and removes it, its × or Backspace removes it and turns the switch off, and "Run query" applies it. The URL keeps `hide_system=1`, not the condition. |
| Sort by | Start time, duration, CPU time, queue wait, child calls, or net IO, and a direction button. It edits the draft and maps to `order=<field>_<asc\|desc>` (agent-access plan §5.2). The backend sorts by start time, newest first, only, so that order goes to the server and every other one sorts the loaded rows in the browser, with a footer note that it applies to the loaded rows only. `serverCanOrder` in `apps/ui/src/calls/sort-order.ts` decides, and the MSW mock sorts by every order, so `npm run dev:mock` shows the server path. |
| Run query | Applies the draft (§2.2). |
| Columns | The "Columns" button at the right end of the table footer, next to the row count: show, hide, reorder, reset. It has no header column of its own, so the data columns keep the width, and it is there while the table loads. The controls row above holds only what edits the query draft; column settings apply at once to the table. Drag a header border to resize. Persisted in local storage; an entry from before the redesign is migrated to the new column keys, and keys the table no longer has are dropped. |
| Sort | By column, within loaded pages. Cross-range ranking needs 08 R2. While a column sort is on, the footer says that it applies to the loaded rows only, so it does not read as global. |
| Row → tree | Clicking the Duration link opens `/ui/tree/:pk` in a new tab, carrying `ts_ms` and `retention_class`. |

**Pagination.** Keyset (`cursor`), infinite scroll or an explicit "load more"; no offset, no page numbers.
An empty page with a non-null `next_cursor` keeps paging; an expired cursor restarts from page one (07 §3.2).

**Banners.** Wide-query guard and partial results render as first-class banners (§5), not silent failures.

**Timestamps** display in the display time zone of the period picker, the browser's by default (as the old UI
did); the URL window stays in Unix ms. The Start cell shows the time alone for a call of today in that zone, and
the date and time otherwise (`Jul 12 23:59:59.999`, with the year for another year). Its tooltip holds the full
timestamp.

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

The `Incoming` / `Outgoing` / `Find usages` / `Local hotspots` results open as derived-view tabs that hold the
recipe `(op, method, category)` and re-derive from the current model, so they follow Adjust and Setup edits.
Tab state is client-side only; it persists through the 10b HTML export (07 §5.3), not the URL.

### 3.3 Tree rows

Each row is a merged node (08 R5–R7):

| Part | Content |
|------|---------|
| Caret | Expand / collapse. Expanding **skips degenerate chains** — a pass-through chain opens in one click to the next node that spends time or branches (07 §5.4). |
| Duration bar | Proportional to the node's share. |
| Metrics | `total (self)` duration, `inv`, `×N` direct executions, child `calls` — every metric as self and total. |
| Signature | `Class.method(args)`; `source:line` and `jar` parse from the method string on Ctrl+hover. |
| Category | A color spanning the tagged subtree, plus a badge (§3.5). |

**Params under a node** are a mini-tree, not flat values (08 R11): metadata rows (`node.name`, `java.thread`,
`memory.allocated`, …) and aggregated `sql` groups — top shapes by time plus an `::other` bucket, similar SQL
grouped by a normalized signature, binds nested under their SQL. Rendered inline and summarized in the
Parameters tab.

### 3.4 Per-node operations

The three core operations — **Incoming calls**, **Outgoing calls**, **Get stacktrace** — sit directly on the
selected row or toolbar; a row kebab (`⋮`, on hover) holds the rest: **Find usages**, **Local hotspots**,
**Adjust duration**, **Add category**, **Mark red**. Ctrl+hover on a node shows the stats popover: self and
total duration and suspension, invocations, average, `source jar`, and `line`.

### 3.5 Hotspots and categories

**Setup categories** tags a method by pattern; the category propagates down its whole subtree (a child tag
overrides) and colors it. **Hotspots** is a flat self-time profile; when categories are set it groups by
category first, then flat-profiles within — so a business operation's share of time and its SQL read directly
(07 §5.3).

### 3.6 Rendering

Virtualised over the flattened visible nodes; lazy expansion; degenerate-chain collapse; a size guard against
a pathological blob (07 §5.4). One indented tree in v1 — flame/icicle is deferred and, if built, must apply
the same degenerate-frame skipping.

## 4. Pods Info

A tab listing the pod-restarts behind the current filters: `namespace / service / pod`, restart time, the
data time range (`time_min_ms`–`time_max_ms`), and a live/closed marker. Source: `/pods`. Diagnostic dumps
(thread, TOP, GC, heap) are not browsed here — each pod row links out to `dumps-collector`, which owns their
listing and download, so the user does not lose access. Analyzing a dump as a tree is deferred (07 §8, 08 §10).

Pods Info has the filter bar of §2.1, limited to `namespace`, `service`, and `pod` with `=` and `in`, and an
`Apply` button. It shares the query string with Calls: it shows and edits only these three fields and keeps the
other conditions as they are.

## 5. States

With a fan-out backend these are frequent, so each names what happened and the one action that resolves it.

| State | Trigger | Message and action |
|-------|---------|--------------------|
| Empty | Filters match no pod, or exclude each other | "No pod in this window matches the namespace, service, and pod filters." or "The filters exclude each other, so no call can match." Calls otherwise opens on the default last hour; Pods prompts "Pick a time range to see pods." |
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
| Calls | `/ui/calls` | `from`, `to` (Unix ms or a relative expression such as `now-1h`); `filter` (repeatable, one condition in its text form, §2.1); `order` (`<field>_<asc\|desc>`, the fields of §2.3 "Sort by"; default `start_desc`, and an unknown value reads as the default); `hide_system=1` when the switch is on; `service` and `pod` for the selection of an old link (below) |
| Pods Info | `/ui/pods` | The same parameters as Calls; it reads and writes only the `namespace`, `service`, and `pod` conditions |
| Call Tree | `/ui/tree/:pk` | `pk` in the path (`ns:svc:pod:restart:file:off:rec`); `ts_ms`, `retention_class` as query hints |

The `cursor` is never in the URL — it is transient scroll state and expires (07 §3.2).

A calls URL holds the applied state in full, with the API's own parameter names:

```text
/ui/calls?from=now-1h&to=now&filter=duration>100ms&filter=namespace=prod-orders&order=start_desc&hide_system=1
```

A visit with no query string gets the last hour, `duration>500ms`, and `hide_system=1`, and the URL is written in
full from then on. With any query string there are no defaults: an absent `filter` means no filters, and an absent
`hide_system` means the switch is off. A `filter` value that does not parse is dropped.

**Old links.** A link without `filter` that carries the parameters of the old calls page (`service`, `pod`,
`duration_min_ms`, `duration_max_ms`, `error_only`, `retention_class`, `q`) and no `order`, which the new page always
writes, is converted to conditions, with that page's defaults: a lower duration bound of 500 ms unless
`duration_min_ms` is given, and hiding system calls unless `hide_system=false`.

The old page selected the union of its `service` and `pod` values, and conditions must all match, so they match every
combination of their values. The link keeps selecting the same calls:

- A pod of a selected service adds nothing to the union and is dropped.
- When the rest is every combination of its namespaces and services, or of its namespaces, services, and pods, it
  becomes conditions: `service=payments/billing&service=payments/ledger` becomes `namespace=payments` and
  `service in billing,ledger`.
- Otherwise, such as services from two namespaces or a service and a pod of another service, the selection stays as
  it was. It shows as one token, `service or pod in prod/orders, stage/billing`, that the query applies like the old
  page did: the pods of the services in the window, and the pods as listed, intersected with any namespace, service,
  or pod condition. The URL keeps it as `service` and `pod` parameters until its token is removed; they are the only
  old parameters written back.

## 7. Non-goals for v1

Deferred, with the reasons in 07 §8: the summary Dashboard, ranked-by-duration and ranked-by-SQL views,
the rich `$param` query language, heap-dumps, authentication, the tracing/logs wiring (the seam stays), and
dump analysis (thread dump, stackcollapse, DBMS_HPROF, JFR).
