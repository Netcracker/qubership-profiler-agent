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
and loads nothing from another host.

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
| A word | A typed word that names no field offers `method =~ <word>`, quoted so that it matches as written. |
| A whole condition | Typing `duration>1s` offers it as one item. |
| Keyboard | Up and Down move the highlight, Enter picks, Escape closes the list, Ctrl+Enter runs the query. Backspace in an empty input steps back: a value, then the operator, then the field, then into the last token, which comes back with its value in the input for editing. |
| Disabled options | A field or an operator the backend cannot apply yet is listed but disabled, with the reason as a tooltip. |
| Disabled tokens | A condition from a link that the backend cannot apply yet shows as a crossed-out token with the reason. It is kept in the URL and not sent. |

**Before the `filter` parameter.** `/calls` takes no `filter` parameter yet, so the applied conditions travel as
its existing parameters (redesign plan §4.2.1):

| Condition | Sent as |
|-----------|---------|
| `namespace`, `service`, or `pod` with `=` or `in` | `pod` tuples, expanded from `/pods` in the browser and intersected across conditions, under the 6000-character request-line guard |
| `duration` with `>`, `>=`, `<`, `<=`, or `between` | `duration_min_ms` and `duration_max_ms`; a strict bound moves by 1 ms; several conditions intersect |
| `error=true` | `error_only=true` |
| `retention_class` with `=` or `in` | `retention_class`; several conditions intersect |
| `method =~ <word>`, the word without regular-expression syntax | `method`, the substring match; one per query |

Everything else is disabled. Conditions that exclude each other, or identity filters that match no pod in the
window, send nothing and say so.

### 2.2 Controls

The time range lives in the header (§1): the period picker with an absolute range, quick ranges, and the display
time zone, between the shift-back and shift-forward buttons, followed by zoom out. A shift moves the window by
half its width, and zoom out doubles it around its center. Both commit an absolute window. Choosing a time range
applies at once (Grafana-style refetch on a range change).

**Draft and applied.** The filter bar, the duration chips, and the two switches edit a draft. "Run query", or
Ctrl+Enter in the bar, applies it: the URL takes the draft and the query restarts from page one, even when nothing
changed. While the draft differs from the applied query, a hint says "Filters changed. Run the query to update
the results." Only the applied state reaches the URL and `/calls`. A time range change and a row shortcut apply at
once.

### 2.3 Calls table

| Group | Columns |
|-------|---------|
| Default | Start, Duration (heat dot + link to tree), Pod, Title (method + tags), `trace_id`, CPU, Queue, Calls, Net IO |
| Hidden by default (R1) | Suspend, Transactions, Disk IO, Memory |

The metrics columns beyond CPU/Calls are the dropped-and-restored set (08 R1). Title shows the method plus a tag
for `error`, `sql`, and `no tree` (the trace blob was dropped, `truncated_reason`).

Rows are 28 px high. Numbers and IDs use the 12 px fixed-width font. The heat dot before the duration is the
theme's ok color under 100 ms, warn under 1 s, mid under 3 s, and crit from 3 s on.

**Pod and `trace_id` shortcuts.** A click on a row's pod or trace ID filters the calls by that value: the pod
name, or the full trace ID, of which the cell shows the first 12 characters. The table reports the click to its
page through `onPickFilter(field, value)`; a page that passes no handler gets plain text in both cells. The calls
page replaces that field's token with `field=value` and runs the query; a `trace_id` token stays disabled until
the backend takes filters.

**`trace_id`.** A `/calls` row has no `trace_id` member (02 §2.3). The cell reads the ID from the call's own
params, under the names the tracing plugins record it with (`trace.id`, `brave.trace_id`, `X-B3-TraceId`), the
same names the tree searches (§3.1). A call without one reads "not traced". A tracer that records the ID on a frame
below the root leaves the row without it, although the tree still finds it; a `trace_id` in the row projection
closes that gap.

| Control | Behavior |
|---------|-----------|
| Duration chips | `All`, `>10ms`, `>100ms`, `>500ms`, `>3s`, `>5s`. A chip replaces every `duration` token with `duration > <chip>`; `All` removes them. A chip is lit when exactly one `duration >` token has its value. The default `>500ms` keeps page one free of sub-millisecond noise. This is the v1 "slowest" affordance (true ranking is 08 R2). |
| Errors only | A switch for the `error=true` token: it shows whether the token is present, and adds or removes it. |
| Hide system/proxy | A switch, on by default: hides proxy/health/idle-async noise (the old UI's `idleTags`, `apps/agent/profiler-ui/src/dataFormat.mjs`). Separate from the duration default; the noise-hide, not the threshold, is what made old first pages readable. It filters the loaded rows in the browser until `/config` serves the patterns for a `method !~` condition (redesign plan §4.6). |
| Sort by | Start time, newest first: the one order `/calls` has. The other fields and the direction come with the `order` parameter. |
| Run query | Applies the draft (§2.2). |
| Columns | The gear button at the end of the header: show, hide, reorder, reset. Drag a header border to resize. Persisted in local storage; an entry from before the redesign is migrated to the new column keys, and keys the table no longer has are dropped. |
| Sort | By column, within loaded pages. Cross-range ranking needs 08 R2. While a column sort is on, the footer says that it applies to the loaded rows only, so it does not read as global. |
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
| Trace chip | `trace` and the first 12 characters of the call's trace ID, with the full ID in the tooltip. A call without one shows `not traced`. |
| Offline chip | `offline copy`, in an exported copy only. |
| Theme switch | At the far right (§1). |

The trace ID is a param of the call. The agent records it as `trace.id` (the Jaeger and OpenTelemetry plugins),
`brave.trace_id` (the Brave plugin), or `X-B3-TraceId` (the incoming header). The UI reads the root call's params
first. A tracer that starts its span a few frames down records the ID there, so the UI then takes the first ID it
meets in the tree.

### 3.2 Tabs and toolbar

Four fixed tabs, drawn as line tabs: **Call Tree**, **Flame graph** (§3.6), **Hotspots**, **Parameters**. The old
Database and Gantt tabs are dropped. The open fixed tab is in the URL as `tab` (§6), so a shared link reopens it.

The `Incoming` / `Outgoing` / `Find usages` / `Local hotspots` results open as derived-view tabs after the fixed
ones, each with a close button. A derived tab holds the recipe `(op, method, category)` and re-derives from the
current model, so it follows Adjust and Setup edits. Derived tabs are client-side state only: they persist through
the 10b HTML export (07 §5.3), not the URL. Closing the active one returns to the fixed tab that the URL names.

The toolbar sits at the right end of the tab bar: `Adjust duration`, `Setup categories`, `Download HTML`, each with an
`@ant-design/icons` icon (field-time, tags, download), as the link buttons have one. The slot between
`Setup categories` and `Download HTML` is reserved for `Explain` and stays empty until the assistant ships. Without
a tree (loading, cold call, error) there are no tabs; the toolbar then has a row of its own, with its
buttons disabled. The tree search belongs to the Call Tree tab, not to the toolbar.

**Link buttons** follow `Download HTML`, after a divider: one per entry of `links` in `/api/v1/config`, in the
operator's order, with the entry's label (02 §2.8). The UI fills the template with the call's trace ID, its pod
identity, and its window: `from_ms` is the call's start and `to_ms` is the start plus the duration. Each value is
encoded as `encodeURIComponent` does. A button opens its link in a new tab.

| Case | Button |
|------|--------|
| The deployment configures no links | Absent, along with the divider. |
| The call has a value for every placeholder of the template | Enabled. |
| The template uses a placeholder the call has no value for, typically `{trace_id}` on an untraced call | Shown disabled, with the reason in a tooltip. |

An exported copy has no backend to ask for the templates and shows no link buttons.

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
| Expand all | A button with a plus-square icon. Opens every node, level by level from the root, up to the 20,000-row budget (07 §5.4). Pass-through chains stay skipped. A tree past the budget opens only as far as the budget allows, and a message says so. |
| Collapse all | A button with a minus-square icon. Closes every node under the root, so the root's children stay in view. |
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

**Call Tree.** Virtualised over the flattened visible nodes; lazy expansion; degenerate-chain collapse; a size guard
against a pathological blob (07 §5.4).

**Flame graph.** The same merged model as blocks, laid out by a client-side transform
(`tree/transforms/flame.ts`): one block per node, placed under its parent, with its offset and width as shares of
the call. Children follow the model's order, from the parent's left edge, so the part of a block with nothing under
it is the method's self time.

| Element | Behavior |
|---------|----------|
| Block | 26 px high on a 28 px row. It shows `Class.method · duration` when it is at least 40 px wide; its tooltip and accessible name always carry the method, the duration, the share of the call, the self time, and the call count. |
| Color | By category, in the stronger variant of the badge color (§3.5). A block without a category takes the share bar's light color. The legend lists the categories on screen and `No category`. |
| Readout | A line above the graph with the numbers of the hovered or focused block, else of the selected one. |
| Selection | A click selects a block, and a second click clears it. The selection is shared with the Call Tree tab: the tree opens the way to the selected row, through closed nodes and skipped chains, and scrolls to it. Selecting adds no buttons. |
| Zoom | A double click, or Shift+Enter on a focused block, re-roots the graph on that block. A `Reset to the whole call` link returns. Shares stay shares of the whole call. |
| Culling | A block under one pixel wide is not drawn, and neither is anything under it. Its space stays reserved, so its siblings do not move. This bounds a row at one block per pixel. |

The flame graph does not skip pass-through chains: a chain is a stack of equally wide blocks, which reads at a
glance and costs one block per level. Blocks are DOM buttons, not a canvas, so focus, tooltips, and theming come
from the browser. Measured on synthetic trees at 1200 px: a 2,062-node tree draws 286 blocks, and a 20,000-node
tree, the row budget, draws 777. A canvas renderer is the fallback if a real tree proves this too slow.

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
| Calls | `/ui/calls` | `from`, `to` (Unix ms or a relative expression such as `now-1h`); `filter` (repeatable, one condition in its text form, §2.1); `order` (`start_desc`); `hide_system=1` when the switch is on |
| Pods Info | `/ui/pods` | The same parameters as Calls; it reads and writes only the `namespace`, `service`, and `pod` conditions |
| Call Tree | `/ui/tree/:pk` | `pk` in the path (`ns:svc:pod:restart:file:off:rec`); `ts_ms`, `retention_class` as query hints; `tab` (`tree`, `flame`, `hotspots`, `params`) |

The `cursor` is never in the URL — it is transient scroll state and expires (07 §3.2).

A calls URL holds the applied state in full, with the API's own parameter names:

```text
/ui/calls?from=now-1h&to=now&filter=duration>100ms&filter=namespace=prod-orders&order=start_desc&hide_system=1
```

A visit with no query string gets the last hour, `duration>500ms`, and `hide_system=1`, and the URL is written in
full from then on. With any query string there are no defaults: an absent `filter` means no filters, and an absent
`hide_system` means the switch is off. A `filter` value that does not parse is dropped.

**Old links.** A link without `filter` that carries the parameters of the old calls page (`service`, `pod`,
`duration_min_ms`, `duration_max_ms`, `error_only`, `retention_class`, `q`) is converted to conditions, with that
page's defaults: a lower duration bound of 500 ms unless `duration_min_ms` is given, and hiding system calls unless
`hide_system=false`. The old parameters are never written back. The old page took the union of its services and
pods, and the conditions take the intersection: a pod pinned inside a selected service narrows to that pod, as
before, but a pod of another service drops that service.

On the tree route, `tab` names the open fixed tab. The default, `tree`, is left out of the URL, and a missing or
unknown value opens Call Tree. A tab switch replaces the history entry, so Back leaves the page instead of stepping
through tabs. Derived tabs never enter the URL (§3.2).

## 7. Non-goals for v1

Deferred, with the reasons in 07 §8: the summary Dashboard, ranked-by-duration and ranked-by-SQL views,
the rich `$param` query language, heap-dumps, authentication, the tracing/logs wiring (the seam stays), and
dump analysis (thread dump, stackcollapse, DBMS_HPROF, JFR).
