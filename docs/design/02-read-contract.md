# 02 — Read contract

> Status: **draft**, awaiting review. Read API for both external clients (UI, automation, MCP) and internal query → collector fan-out. Fresh design — only the agent's TCP wire protocol (Section 1 of `01-write-contract.md`) is preserved; the legacy Java collector's external API is **not** a baseline.

> **2026-07-01 alignment — now in the body.** The read path is aligned with the hot-store + seal-pass model (hot reads from segments + the SQLite index, range-overlap discovery, PK time hints, eventual consistency); it is folded into §2–§5 below.

## 1. Scope

Two API surfaces:

- **External:** clients (UI, scripts, MCP) talk to the `query` service. JSON over HTTP.
- **Internal:** `query` fans out to `collector` replicas for the hot tier and reads parquet from **S3** for the cold tier. JSON over HTTP for collector; S3 SDK for cold.

The agent's TCP wire protocol is preserved — existing agents continue to ship to the new collector with no changes (see `01-write-contract.md` §1).

The Java collector's external API is **not** preserved. The new query service designs endpoints fresh; the UI is rewired in Stage 5 (`profiler-plan.md`).

No authentication for MVP. Add later (Keycloak/Bearer is the likely choice).

## 2. External query API

Base path: `/api/v1`. JSON request and response. Error bodies follow RFC 7807 (Problem Details).

### 2.1 Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/pods` | List `(namespace, service, pod, restart_time)` tuples with any data in a time range. |
| GET | `/pods/{pod-restart}/dictionary` | Per pod-restart dictionary (`method_id → string`, `param_id → string`). For advanced consumers using raw `/trace` (see §2.5). |
| GET | `/calls` | Page through Call records matching filter. |
| GET | `/calls/{pk}` | Fetch a single Call by PK with full metadata. |
| GET | `/calls/{pk}/trace` | Fetch the per-call trace blob (raw wire bytes; advanced consumers — see §2.5). |
| GET | `/calls/{pk}/tree` | Server-decoded call tree, MessagePack-encoded (canonical for UI / MCP / CLI — see §2.5). |
| GET | `/stats` | Aggregates of the calls a `/calls` query matches: counts, errors, sums, and percentiles per time bucket or per field value (§2.9). |

> **Not yet implemented.** `/pods/{pod-restart}/dictionary` and `/calls/{pk}` are specified here for advanced and raw-trace consumers, but the query service does not register them today; the MVP serves `/pods`, `/calls`, `/calls/{pk}/trace`, and `/calls/{pk}/tree`. `/stats` is specified in §2.9 and built in stages.

### 2.2 Primary key

Every Call has a 7-component PK:

```
PK = {
  pod_namespace:     string,
  pod_service:       string,
  pod_name:          string,
  restart_time_ms:   int64,
  trace_file_index:  int32,
  buffer_offset:     int32,
  record_index:      int32,
}
```

`trace_file_index`, `buffer_offset`, `record_index` are stored as three INT32 columns in parquet (decision in `01-write-contract.md` §5.3). `pod_*` and `restart_time_ms` are stored as separate columns.

URL serialization (path segments are colon-separated, percent-encoded):

```
<ns>:<svc>:<pod>:<restartMs>:<file>:<off>:<rec>
```

JSON bodies use the nested-object form.

A bare PK carries no time or retention class, so `/calls/{pk}/tree` and `/calls/{pk}/trace` cannot locate a cold file on their own. A client fetching a cold call passes the `ts_ms` and `retention_class` from the `/calls` response as query parameters of the same names (`?ts_ms=...&retention_class=...`; an opaque `call_ref` bundling them stays an option for later), so the file is found by range-overlap (§5.1) without a full scan. Without the `ts_ms` hint, a call outside the hot window answers `404` whose detail names the missing hint — the server never falls back to an unbounded scan.

### 2.3 GET /calls — filter and pagination

Query parameters:

| Param | Type | Notes |
|---|---|---|
| `from` | int64 (Unix ms) | Required. Inclusive lower bound on `ts_ms`. |
| `to` | int64 (Unix ms) | Required. Exclusive upper bound on `ts_ms`. |
| `filter` | string (repeatable) | Optional. One condition per value, such as `service=billing` or `duration>2s`; a call matches when it meets every condition (§2.3.3). |
| `pod` | string (repeatable) | Optional. `namespace/service/pod`. Shorthand for conditions (§2.3.3). |
| `method` | string | Optional substring match on the `method` column. Shorthand for a condition (§2.3.3). |
| `duration_min_ms` | int32 | Optional. Shorthand for `duration>=<n>ms`. |
| `duration_max_ms` | int32 | Optional. Shorthand for `duration<=<n>ms`; `0` means no upper bound. |
| `error_only` | bool | Optional. Maps to `error_flag = true`. Shorthand for `error=true`. |
| `retention_class` | string (repeatable) | Optional. `short_clean` / `normal_clean` / `long_clean` / `any_error` / `corrupted`. Shorthand for `retention_class in <values>`. |
| `order` | string | Optional. `<field>_<asc\|desc>`, default `start_desc` (§2.3.5). |
| `cursor` | string | Opaque cursor from previous page's `next_cursor`. |
| `limit` | int | Page size. Default 100, max 1000. |

Response:

```json
{
  "calls": [
    {
      "pk": {
        "pod_namespace": "default", "pod_service": "billing", "pod_name": "billing-7f8c",
        "restart_time_ms": 1714060800000,
        "trace_file_index": 5, "buffer_offset": 12340, "record_index": 0
      },
      "ts_ms": 1714060812345,
      "duration_ms": 1247,
      "method": "com.example.Service.handle",
      "thread_name": "http-nio-8080-exec-3",
      "cpu_time_ms": 873,
      "wait_time_ms": 12,
      "memory_used": 1048576,
      "queue_wait_ms": 15,
      "suspend_ms": 20,
      "child_calls": 42,
      "transactions": 2,
      "logs_generated": 2048,
      "logs_written": 512,
      "file_read": 100,
      "file_written": 200,
      "net_read": 300,
      "net_written": 400,
      "error_flag": false,
      "retention_class": "long_clean",
      "params": { "request.id": ["abc123"] },
      "trace_blob_size": 18432,
      "truncated_reason": null
    }
  ],
  "next_cursor": "opaque-string-or-null",
  "partial": false,
  "partial_reasons": []
}
```

Every metric column of `CallV2` (`01-write-contract.md` §5.2) is projected into the row: `queue_wait_ms`,
`suspend_ms`, `transactions`, `logs_generated`, `logs_written`, `file_read`, `file_written`, `net_read`, and
`net_written` ride alongside the original six (`08-ui-backend-requirements.md` R1). On the hot tier
`suspend_ms` is a provisional value — the call interval intersected with the pauses known at index time; the
seal pass re-derives it from the full `suspend.wal` (`01-write-contract.md` §5.1 step 4), which is one more
reason the §6.3 dedup prefers the cold copy.

`trace_blob_size` reports the blob's byte length, and is `0` when `truncated_reason != null` (the blob was dropped under pressure; see §4.6 of `01-write-contract.md`). On the cold list path the exact length is not available — `CallV2` (`01-write-contract.md` §5.2) carries no size column and the list projection does not read the blob — so the field is `null` there, and blob presence is `truncated_reason == null`; a client that needs the exact size fetches the blob via `/tree` or `/trace`. Adding a `trace_blob_size INT32` column to `CallV2` is the option if the list must carry the exact size — an additive change, backward-readable by column name (`01-write-contract.md` §5.2): rows sealed before the column read back as `null`, which degrades to today's behavior.

### 2.3.1 Cursor: ordering and stable pagination

`/calls` paginates by keyset (seek), not by offset. The cursor encodes a position in a total order that every tier shares, so a call that migrates from the hot tier to the cold tier between two page fetches keeps its place.

**Ordering.** Rows come back by `ts_ms` descending, then by PK ascending as the tiebreaker (`ts_ms` is not unique — many calls share a millisecond). The PK compares component by component: the `pod_*` string components byte-wise (not locale-aware), then `restart_time_ms` and the integer pointer components numerically. Both tiers must produce this same total order, so the query service applies one shared component-wise comparator to the hot rows and the cold-tier k-way merge alike. A hot source must not delegate ordering to a SQLite `ORDER BY` over a concatenated `pod_restart` string: the `/` separators and a text-compared `restart_time_ms` diverge from the component-wise order and silently break cross-tier merge and pagination. This is the default order, `start_desc`; §2.3.5 specifies the others.

**Why migration is not a special case.** `ts_ms` and the PK are immutable and identical in the SQLite call index and in the parquet row. Keyset pagination seeks with `WHERE (ts_ms, pk) < (cursor.ts_ms, cursor.pk)` against every source, so a migrated call is found by whichever tier now holds it, at the same position; the overlap window (§4.3) and PK dedup (§6) collapse the duplicate. Crossing the hot→cold boundary as pagination goes deeper is therefore transparent, and it rests on the zero-gap guarantee of §4.3: a flushed call is visible from at least one tier from the moment it leaves in-memory state.

**Cursor contents.** The cursor is an opaque, URL-safe base64 token carrying:

- a format version,
- the frozen query — `from`, `to`, every filter, and the ordering,
- the last-emitted position `(ts_ms, pk)`,
- an issue timestamp, for the TTL below.

The query is frozen at the first page so the window does not drift as wall-clock time and the hot window advance. On pages 2..N the client re-sends only the `cursor`; if it also re-sends filter parameters, they must match the frozen query or the request is rejected with `400`. Re-sent conditions (§2.3.3) match when they are the same set in canonical text form, in any order and from either the `filter` parameter or its legacy shorthand. Freezing `to` is what keeps the upper bound stable — otherwise each page re-evaluates `now` and the result set shifts under the reader.

**Fan-out and merge.** One global position is enough; no per-source continuation state is kept. For each page `query` re-issues the full fan-out (every hot replica plus the cold LIST), each source seeks past the cursor position and returns up to `limit` rows, and `query` runs a k-way merge, dedups by PK, then truncates to `limit`. Dedup runs before the truncation and before `next_cursor` is computed, so an overlap-window duplicate neither consumes a page slot nor strands the cursor between its two copies. Deep pagination costs one fan-out per page and re-scans cold parquet (no secondary index, §5.4); a stateful scroll cursor is deferred until profiling shows this is too slow.

**Consistency of a pagination session.** A page reflects a snapshot as of the position it reports, not a globally consistent snapshot of the whole window. Data written below an already-passed position — a late call that re-seals an older bucket into a patch file (`01-write-contract.md` §6.6) — is not surfaced in that pagination session. This is the eventual-consistency envelope of §4.3, made explicit for pagination: the profiler favors bounded, slightly stale results over holding a read snapshot open across many seconds.

**Termination.** `next_cursor` is `null` only when the seek position passes `from` and the window is exhausted. A page may come back empty in the middle of the range — its rows aged out of the hot tier and were deleted from the cold tier by a retention-class TTL between fetches — while a non-null `next_cursor` still points further down; the client keeps paging. An empty page is not an end-of-stream signal on its own.

**Cursor TTL.** A cursor is valid for `PROFILER_CURSOR_TTL` (default `15m`) from issue. An expired cursor is rejected with `400`, and the client restarts from page 1. Expiry, a cursor that does not decode, and a frozen-query mismatch all answer `400` with `code: cursor_rejected` (§8) — one code for the three conditions, because the client's reaction to all of them is the same page-1 restart. Every other `400` on `/calls` carries `invalid_request`, which is a caller bug to surface rather than to retry. The TTL bounds how far the frozen `to` can lag real time and covers a position that points into parquet already removed by a retention TTL. Signing the cursor (HMAC, to stop a client forging a position that forces an expensive scan) is deferred; internal validation of the frozen-query fingerprint is enough for the in-team MVP.

### 2.3.2 Wide-query guard

A `/calls` query over a wide window with no file-pruning filter is the one shape that cannot meet the read SLO. Sorted `ts_ms DESC`, its first page fills from the densest recent buckets — a 200-pod cluster lists ~3,000 objects per hour-class (§5.5), most of them `short_clean` — and paging deeper only widens the scan. For `short_clean` the query is also mostly empty: that class has a 2-day TTL (`01-write-contract.md` §6.4), so a week-wide short-call scan reads at most the last two days and returns nothing earlier.

`query` rejects such a query with `400` at validation rather than narrowing it silently. The result set then always matches what was asked, and the caller — not the server — chooses which axis to narrow on. The guard has two layers, both evaluated before any parquet file is opened.

**Narrowing filters.** A filter exempts a query from the guard only if it actually prunes the *set of files discovered*, not the rows read within them:

- `pod` — resolves to a pod-restart's file set. A `namespace`, `service`, or `pod` condition with `=` or `in` (§2.3.3) counts as `pod`;
- `retention_class` — selects key prefixes (§5.1), unless it names every class, which prunes nothing. Applies to a `retention_class` condition with `=` or `in`;
- `duration_min_ms` — prunes to the classes that can hold a call that long, by the tier table of `01-write-contract.md` §6.4: `≥ 1000` lists `long_clean`, `huge_clean`, and the error classes, which carry calls of any duration. A value below the first tier bound (for example `duration_min_ms=1`) prunes no class and does **not** exempt. The lower bound of a `duration` condition (`>`, `>=`, or `between`) counts the same way;
- `error_only` — prunes to `any_error` and `corrupted`. Applies to `error=true`.

The exemption check runs the same class derivation discovery uses, so a filter that would leave the LIST plan untouched never buys an exemption. `method` and `params` do **not** exempt: they filter rows inside already-listed files (§5.4), so they cut the result, not the scan. The same holds for every other condition of §2.3.3: `class`, `endpoint`, `trace_id`, `request_id`, an upper bound on `duration`, `error=false`, and every negation and regular expression.

**Layer 1 — span.** If `to - from > PROFILER_WIDE_RANGE_LIMIT` (default `6h`) and none of the narrowing filters above is present, reject with `400`. This layer needs no I/O, so it stops the pathological wide-open query before the discovery LIST that layer 2 depends on — a multi-day range is itself thousands of serial LIST round-trips (§5.5). A `pod` filter exempts the query from `PROFILER_WIDE_RANGE_LIMIT` but not from every bound: `pod` prunes rows inside already-listed files, not the discovery LIST prefixes (§5.1), so cold discovery still walks every hour prefix in the window. A pod-filtered `/calls` window is therefore capped at `PROFILER_MAX_PODS_RANGE` (§2.7) — the same span bound `/pods` uses — and is rejected here, before the LIST fans out across the interval, when it exceeds that cap.

**Layer 2 — estimated scan.** For a query that clears layer 1, the discovery LIST (§5.1) already returns, per candidate object, its size and — from the key — its `retention_class`. Summing these gives `(file_count, total_bytes)` for the whole scan with no extra request and no file opened. If `file_count > PROFILER_MAX_SCAN_FILES` or `total_bytes > PROFILER_MAX_SCAN_BYTES`, reject with `400` before reading. The two limits map to the two cost axes of §5.5: object count bounds LIST and GET round-trips, byte total bounds decode-and-scan volume. Both are needed — many tiny files pass a byte limit but not a file limit, and a few large files the reverse.

**What the byte limit actually bounds.** The sizes a LIST returns are on-object (ZSTD-compressed) bytes, so
`PROFILER_MAX_SCAN_BYTES` bounds what is fetched, not what is held: decoding multiplies the footprint several-fold
before rows are merged. The guard is also strictly per-request — N concurrent guard-passing queries can hold N
budgets of decoded data at once. The load campaign OOM-killed a 3 GiB query pod in 34 seconds exactly this way
(`load-testing-report.md` §7). What the guard cannot bound, the process-wide read memory budget does (§7.5): the
guard stays the cheap per-request pre-flight that rejects intrinsically-too-wide queries with narrowing advice
(`400`), while the budget bounds what the whole process materializes at runtime — concurrency and the decode
expansion included — and sheds load with `503` when concurrent demand exceeds it. Size
`PROFILER_MAX_SCAN_BYTES` for the widest single query the deployment wants to allow; size the pod from the budget
(§7.5), not from this guard.

The rejection body (§8) carries the estimate and a per-class byte breakdown, so the caller sees which axis dominates — usually `short_clean` — and which filter would cut it.

**Evaluated on every page.** The guard runs on page 1 against the parsed query and on pages 2..N against the query frozen in the cursor (§2.3.1). Until the cursor is HMAC-signed it is client-forgeable, so re-checking is the only thing that stops a hand-minted cursor from smuggling a wide query straight into cold discovery. Re-checking does not penalize honest pagination: a cursor `query` minted for a query that already cleared page 1 carries the same window, so the span layer re-passes, and a later page's cold window is bounded by the dynamic cutoff (§4.3) — its discovered file set is a subset of page 1's, so the cost estimate cannot grow past the page-1 verdict.

### 2.3.3 Filter conditions

A condition is a field, an operator, and a value. The `filter` parameter carries one condition per value, and a call
matches a query when it meets every condition. The UI's filter bar, its permalinks, the `profiler` CLI's `--filter`
flag, and the MCP tools carry the same conditions in the same text form, so a filter reads the same in all of them.
`/stats` takes the same conditions.

**Text form.**

```text
<field><op><value>          op: = != =~ !~ > >= < <=      duration>100ms, namespace=prod-orders
<field> in <v1>,<v2>        also "not in"                 service in billing,orders
<field> between <a>..<b>    duration only, inclusive      duration between 100ms..2s
```

- A field name matches `[A-Za-z_][A-Za-z0-9_.-]*`, so it holds no whitespace and no operator character.
- The value is the rest of the condition after the operator, with surrounding whitespace removed. Values are not
  quoted, and a value cannot begin or end with whitespace.
- Whitespace around a symbol operator is accepted and dropped. The canonical form has none, except that a value
  beginning with `=` or `~` keeps one space after the operator: `pod= ~x` is "pod equals `~x`", and `pod=~x` is a
  regular-expression match.
- In an `in` or `not in` list, `\,` is a literal comma and `\\` a literal backslash. Any other character stands for
  itself. A list ending in a lone backslash is invalid.
- In a URL, a condition is percent-encoded like any other query value: `filter=duration%3E100ms`.
- A duration is a number with the unit `ms` or `s`, and a bare number is seconds: `500ms`, `1.5s`, and `2` are valid.
  Whitespace inside a duration is ignored, and a duration is rounded to whole milliseconds. A `between` range may name
  its bounds in either order: `2s..100ms` is the same range as `100ms..2s`.
- `None` and `Any` are special values for fields a call can lack (params, `trace_id`, `request_id`, `endpoint`). They
  take their special meaning with `=` and `!=` only: `trace_id=None` matches a call without a trace ID, and
  `trace_id=Any` a call with one. With any other operator they are plain text, so `method=~Any` matches a method
  containing `Any`.

**Fields.**

| Field | Source | Operators |
|---|---|---|
| `duration` | `duration_ms` | `>`, `>=`, `<`, `<=`, `between` |
| `namespace`, `service`, `pod` | The PK's `pod_namespace`, `pod_service`, `pod_name` | `=`, `!=`, `=~`, `!~`, `in`, `not in` |
| `method` | `method` | `=`, `!=`, `=~`, `!~` |
| `class` | Derived from `method`, below | `=`, `!=`, `=~`, `!~`, `in`, `not in` |
| `endpoint` | Derived from the `web.method` and `web.url` params, below | `=`, `!=`, `=~`, `!~`, `in`, `not in` |
| `error` | `error_flag`; the value is `true` or `false` | `=` |
| `retention_class` | The object key's class (§5.1) | `=`, `!=`, `in`, `not in` |
| `trace_id` | The `trace.id`, `brave.trace_id`, and `X-B3-TraceId` params, below | `=`, `!=`, `=~` |
| `request_id` | The `x-request-id` param | `=`, `!=`, `=~` |
| Any other name | The call param of that name, such as `user.tenant` | `=`, `!=`, `=~`, `!~`, `in`, `not in` |

A name in the table always means the field. A call param that shares a name with a field, such as a param named
`duration`, cannot be filtered on.

`trace_id` reads the shared name the agent's tracer plugins write (`trace.id`), the name older Brave plugins write
(`brave.trace_id`), and the B3 propagation header (`X-B3-TraceId`). The values of all three count as values of one
param, so a trace ID the UI shows for a call always finds that call. Each name also stays reachable as a plain param.

`class` is the declaring class of the root method. Take the `method` text before its first `(`, then its last
whitespace-separated word, which is the qualified method name; the class is that name up to its last `.`. For
`void com.example.OrderService.place(long) (OrderService.java) [orders.jar]` the class is `com.example.OrderService`.
A qualified name without a `.`, such as the `#<id>` placeholder of a method missing from the dictionary, names no
class. Such a call lacks the field, like a call without a param: it meets only negative conditions on `class` and
falls into no `/stats` row. `None` and `Any` still do not apply to `class`, because the UI offers them only for
fields a call is expected to lack.

`endpoint` is `<web.method> <path>`, for example `GET /orders/{id}`. `<path>` is the path of the `web.url` value, with
the scheme, the authority, any query string, and any fragment removed; a URL without a path has the path `/`. Each
path segment that is all digits, a UUID, or 16 or more hex digits is replaced by `{id}`, which keeps the number of
distinct endpoints small enough to group by. A call without `web.url` has no endpoint. A call with `web.url` and no
`web.method` has the path alone. When either param has several values, the first one counts, as in the UI's call
title.

**Matching.**

- `=` compares the whole value, case-sensitively. `in` matches when any listed value would match with `=`.
- `=~` matches when the RE2 regular expression matches anywhere in the value (it is not anchored).
- `>`, `>=`, `<`, `<=`, and `between` compare durations in milliseconds.
- `!=`, `not in`, and `!~` are the negation of `=`, `in`, and `=~`. A negative condition on a field the call lacks
  therefore matches: `trace_id!=abc` matches a call without a trace ID.
- A param with several values matches a positive condition when any of its values matches, and a negative one when
  none of them does. A param recorded with no value counts as absent.

**Legacy parameters.** The parameters of §2.3 that predate `filter` are shorthand, and a query may mix them with
`filter`:

| Parameter | Conditions |
|---|---|
| `pod=<ns>/<svc>/<pod>` | One condition that the call's `(namespace, service, pod)` is one of the listed tuples. Repeating `pod` adds tuples to the same condition. The text form has no syntax for it. |
| `method=<text>` | `method=~<text>` with every regular-expression character of `<text>` escaped, which is a substring match |
| `duration_min_ms=<n>` | `duration>=<n>ms` |
| `duration_max_ms=<n>` | `duration<=<n>ms`; `duration_max_ms=0` adds no condition, as before `filter` existed |
| `error_only=true` | `error=true` |
| `retention_class=<c>` (repeatable) | `retention_class in <c1>,<c2>` |

**Errors.** A condition that does not parse, a field that does not take the operator, a value of the wrong type (a
duration that does not parse, an `error` value other than `true` or `false`, an unknown retention class), and `None` or
`Any` on a field every call has all answer `400` with `code: invalid_request` and a detail that quotes the condition.

### 2.3.4 Filter stages and capabilities

The filter model is specified in full above, and `query` implements it in two stages. A condition the running build
does not implement answers `400` with `code: invalid_request` and a detail that names the field and the operator,
such as "`!=` on `service` is not implemented". A client can tell such an answer from a typo only by the detail, so it
reads `filter_capabilities` instead of probing.

| Stage | Operators and fields |
|---|---|
| A | `duration` with `>`, `>=`, `<`, `<=`, `between`; `namespace`, `service`, `pod`, and `retention_class` with `=` and `in`; `method` with `=~`; `error` with `=`; `trace_id`, `request_id`, and params with `=` |
| B | Every other operator in the table of §2.3.3; the fields `class` and `endpoint`; the values `None` and `Any` |

`method` with `=~` is in stage A because it costs the same row scan as the legacy `method` substring match, and the
UI's filter bar turns a typed word into `method=~<word>`.

`GET /api/v1/config` reports what the build implements:

```json
{
  "filter_capabilities": {
    "fields": {
      "duration": [">", ">=", "<", "<=", "between"],
      "namespace": ["=", "in"],
      "service": ["=", "in"],
      "pod": ["=", "in"],
      "error": ["="],
      "retention_class": ["=", "in"],
      "trace_id": ["="],
      "request_id": ["="],
      "method": ["=~"]
    },
    "param_operators": ["="],
    "special_values": [],
    "orders": ["start_desc"],
    "group_by": []
  }
}
```

- `fields` maps each implemented field to its implemented operators. A field that is missing is not implemented.
- `param_operators` lists the operators that work on call params.
- `special_values` lists `None` and `Any` once they work.
- `orders` lists the `/calls` orders the build implements (§2.3.5), and `group_by` the `/stats` groupings (§2.9). A
  build without either holds only `start_desc` and no grouping, as the example shows.

A client that finds no `filter_capabilities`, from a build older than the filter model, assumes the legacy parameters
only.

### 2.3.5 Orders

`order=<field>_<direction>` sets the order of `/calls`. The direction is `asc` or `desc`, and the default is
`start_desc`, the order of §2.3.1.

| Field | Sort key |
|---|---|
| `start` | `ts_ms` |
| `duration` | `duration_ms` |
| `cpu` | `cpu_time_ms` |
| `queue` | `queue_wait_ms` |
| `calls` | `child_calls` |
| `net` | `net_read + net_written` |

Every order breaks ties by PK ascending, with the component-wise comparator of §2.3.1, so each order is a total order
that both tiers produce alike. The keyset of an order is `(sort key, pk)`: the cursor freezes the order with the rest
of the query and records the last-emitted `(sort key, pk)`. A re-sent `order` that differs from the frozen one answers
`400` with `code: cursor_rejected`, as any other contradicted filter does. The sort keys are immutable, like `ts_ms`,
so a call that migrates from the hot tier to the cold tier keeps its place in every order.

`query` implements orders in two stages, and an order the build does not implement answers `400` with
`code: invalid_request` and a detail that names it. `filter_capabilities.orders` (§2.3.4) lists the implemented ones.

| Stage | Orders |
|---|---|
| A | `start_desc`, `duration_desc` |
| B | Every other field and direction of the table above |

No retention class bounds `start`, `cpu`, `queue`, `calls`, or `net`, so an order on one of them reads every file of
the window, as `start_desc` does, and works only inside the wide-query guard (§2.3.2): it is no narrowing filter, and
the cost layer charges the whole discovery. Ascending or descending, the cold scan sorts each batch it reads, because
a file holds its rows in start order.

**`duration_desc` reads the duration bands from the top.** The clean retention classes are disjoint duration bands of
the tier table (`01-write-contract.md` §6.4): `huge_clean`, `long_clean`, `normal_clean`, and `short_clean`, from the
longest band down. The error classes, `any_error` and `corrupted`, hold calls of any duration. Besides the position
`(duration_ms, pk)`, a `duration_desc` cursor records `band`: the longest clean band the next page may still need. A
first page has no position, and its `band` is `huge_clean`. A page reads the clean bands from `band` down and reads the
error classes alongside every band:

1. Skip each clean band longer than `band`; its rows all precede the cursor. Skip each band a `duration` condition
   excludes, as discovery does (§2.3.2).
2. Read the next band and merge its rows past the position with the error-class rows past the position, by
   `(duration_ms DESC, pk ASC)`. A row is past the position when its `duration_ms` is lower, or equal with a greater
   PK; a position with no PK (below) passes only lower durations.
3. Once at least `limit` merged rows have a `duration_ms` at or above the band's lower bound, the page is the first
   `limit` of them, and the next cursor has the last row's position and this band as `band`. Every row of a shorter
   band is shorter, so no shorter band can change the page. Otherwise go back to step 2 with the next band.

A request for the slowest calls of a window therefore starts with `huge_clean` and the error classes, and opens a
shorter band only when the longer bands hold fewer than `limit` calls past the cursor. It needs no lower bound on
`duration`. The error classes have no duration index, so every page scans them in full; the guard
bounds that cost.

**The guard and `duration_desc`.** `order=duration_desc` counts as a narrowing filter for the span layer of §2.3.2,
because a first page opens `huge_clean` plus the error classes. The cost layer is evaluated per page,
before each band opens, against the files and bytes of the bands the page has opened so far plus the next one:

- If the first band of a page already exceeds `PROFILER_MAX_SCAN_FILES` or `PROFILER_MAX_SCAN_BYTES`, the page
  answers `400` with `code: query_too_wide`, and `by_class` names the classes in the estimate.
- If a later band would exceed the limits, the page stops before that band. It returns every merged row at or above
  the lower bound of the last band it read, which are final, and a `next_cursor` whose `band` is the band that did not
  fit and whose position is that lower bound with no PK: every row at or above it has been returned. Such a page can
  hold fewer than `limit` rows, even none; like an empty page (§2.3.1), it does not end the stream. The next page
  starts at the band that did not fit, so its first-band check charges that band and the error classes only. If they
  fit, paging goes on; if not, it answers `400` with the narrowing affordance, and the caller adds a lower bound on
  `duration` or narrows the window.

Every page therefore either returns a row or moves `band` to a shorter band, so paging always progresses.

`next_cursor` is `null` once every band the conditions allow and the error classes hold no row past the cursor. The
hot tier needs no bands: a hot source returns up to `limit` rows past the cursor in `(duration_ms DESC, pk ASC)` order,
and `query` merges them with the cold rows by the same comparator.

**`duration_asc` is the same walk from the bottom.** It reads the clean bands from `short_clean` up, with the error
classes alongside the first band, and its cursor records `band` as the shortest band the next page may still need; a
first page has `short_clean`. A band's frontier is its last duration, `upper − 1`, where `duration_desc` uses the
lower bound: a page ends once `limit` merged rows sit at or below the frontier of the band just read, and a page the
cost layer stops before a band returns the rows at or below the frontier of the last band it read, with a
`next_cursor` at that frontier with no PK. A position with no PK passes only longer calls. The walk saves no file
the way `duration_desc` does, because the shortest band, `short_clean`, is the largest class: `duration_asc` is no
narrowing filter for the span layer, and a lower bound on `duration` is what makes its first band cheap.

**Hot tier.** Every order reads the hot store per call-index partition: each partition returns up to `limit` rows past
the cursor in the order, with the whole tie group at the cut, and the partition runs merge by the shared comparator.
The partitions index `ts_ms` and `duration_ms` only. For `cpu`, `queue`, `calls`, and `net`, SQLite sorts the rows of
a partition that meet the pushed-down conditions on every page; a partition holds one time bucket of one replica, so
the cost is bounded, and an index per key would slow every insert on the write path for orders the UI rarely asks for.

### 2.4 Trace blob — lazy endpoint

`GET /api/v1/calls/{pk}/trace` returns the per-call blob as raw bytes.

- `Content-Type: application/octet-stream`
- `ETag`: a stable hash of the PK (blob is immutable per PK)
- `Cache-Control: public, max-age=31536000, immutable`
- `Accept-Ranges: bytes` — supports `Range:` for partial reads (useful when the UI streams the start of a long trace)
- `404` if `trace_blob = NULL` (see `truncated_reason` in the Call row)
- `416` in the §8 envelope, with `code: range_not_satisfiable`, for a `Range:` that does not parse or starts past the end of the blob; the latter also carries `Content-Range: bytes */<size>`
- `503 + partial` markers do not apply here: blob is either present or absent.

Reader semantics (chunk stream with tail/head noise; parsed against the per-pod-restart dictionary) — see `01-write-contract.md` §4.5.

### 2.5 Rendering the call tree

Two consumption paths:

- **`/calls/{pk}/tree` — canonical path** for UI, MCP, CLI. Server pre-aggregates the per-call blob into a tree, encodes as MessagePack with stable int-keyed maps and a version envelope. Self-contained (the response carries its own per-tree dictionary inline). Hand-written decoders in any language are ~50–80 LOC.
- **`/calls/{pk}/trace` + `/pods/{pod-restart}/dictionary` — advanced path** for consumers that want the raw wire format (third-party tooling re-using our Go decoder, full-fidelity offline analysis). Smaller payload, but more client code to maintain.

Per-node suspension (§2.5.3 fields 3–4) is attributed at tree build: each invocation's `[enter, exit]`
interval is intersected with the pod-restart's stop-the-world timeline
(`08-ui-backend-requirements.md` R7). On the hot tier the timeline comes from the replica's `suspend.wal`
mirror via the internal suspend endpoint (§3); on the cold tier from the row's own `suspend_json` column —
the pauses overlapping the call's blob span, inlined at seal (`01-write-contract.md` §3.6, §5.2). A NULL
column means zero suspension.

**The list column and the tree can disagree, by design.** `suspend_ms` on the `/calls` row (§2.3) is
computed on the *calls* time axis — the pause overlap with `[ts_ms, ts_ms + duration_ms]`
(`01-write-contract.md` §5.1 step 4) — while `suspend_json` and the per-node suspension above are selected
on the *blob* time axis, the call's own trace-event timer span (`01-write-contract.md` §3.6). The two axes
are independent epochs (calls-stream `ts_ms` vs. the trace timer), so a call can show `suspend_ms = 0` in
the `/calls` list while its `/tree` still reports non-zero suspension on one or more nodes, or vice versa.
This is a conscious compromise, not a bug: a client that needs the two numbers to agree should treat the
tree's per-node suspension as authoritative and the list column as a cheap, approximate hint for sorting
and filtering.

Big parameter values (`sql` / `xml`) are the one asymmetry between the two paths. The blob does not inline them — it holds `(rolling_seq, offset)` references into the value streams (§3, `01-write-contract.md` §4.4). `/tree` resolves each reference and inlines the value string in the returned tree, so its consumers need nothing else. On the hot tier the references resolve against the replica's value segments (via the internal values endpoint, §3); on the cold tier they resolve against the values the seal pass inlined into the row's `big_params_json` column (`01-write-contract.md` §4.4, §5.2) — the value segments themselves never reach S3. A reference that cannot be resolved (its segment was evicted before the seal, or the file predates the column) is marked explicitly in the tree (`unresolved`, §2.5.3) with the reference text in the value slot; a value is never dropped silently. The raw `/trace` blob keeps the references; the MVP does not expose the value streams over a separate external endpoint, so an advanced consumer resolves big params only against a full dump. Add an external `/calls/{pk}/values` endpoint if a raw-path consumer needs them.

The decision in MVP: ship `/tree` as the canonical contract. `/trace` + `/dictionary` remain as the secondary, lower-traffic interface — useful, but not the default.

#### 2.5.1 Why MessagePack with int-keyed maps

Schema-evolution requirement: backend + UI ship together today, but MCP and CLI are planned for Stage 4+. Future field additions must not break older clients of either kind.

To avoid a `.proto` schema toolchain and codegen step while still getting forward/backward compatibility, the response uses the **int-keyed map** convention — semantically equivalent to protobuf field tags, encoded in plain MessagePack:

- Every record (`Tree`, `Node`, `Param`) is a `Map<int, value>`, NOT a positional array.
- Field numbers are documented in this contract (§2.5.3) and are append-only.
- Removed fields keep their numbers reserved (never re-used) — same as protobuf.
- Top-level `v` (version) supports hard breaking changes if they are ever required.
- Unknown int keys MUST be silently ignored by decoders (= forward compat).

Trade-off vs protobuf: hand-written ~50–80 LOC decoders per consumer language; no codegen; convention enforced by code review of this contract document. Acceptable for an in-team API. The shape maps 1:1 to protobuf field tags, so migration later (e.g. if external partners appear) is mechanical.

#### 2.5.2 Response envelope

```
GET /api/v1/calls/{pk}/tree
Accept: application/x-msgpack

200 OK
Content-Type: application/x-msgpack
ETag: <validator>               # cold: <hash-of-PK>; hot: <hash-of-body>
Cache-Control: <cache-policy>   # cold: public, max-age=31536000, immutable; hot: no-cache
Content-Encoding: gzip          # transparent if client supports it

body — MessagePack-encoded Map<int, value>:
  {
    0: 1,                                      # v: version = 1
    1: ["com.example.Service.handle", "..."],  # methods: per-tree method dictionary
    2: ["request.id", "..."],                  # params:  per-tree param-key dictionary
    3: <Node>                                  # root: the root node
  }
```

The `methods` and `params` arrays carry only strings that this specific tree references — not the entire pod-restart dictionary. The response is self-contained; no separate dictionary fetch is required for this path.

The cache policy splits by tier (reports2 #5). A sealed cold tree is immutable per PK, so it keeps the PK-hash ETag and the one-year immutable cache. A live hot tree is still growing — its dictionary, big-parameter values, and suspend timeline change while the pod runs — so it returns a body-hash ETag with `Cache-Control: no-cache`; the client revalidates on every use and gets a 304 only while the tree is byte-for-byte unchanged.

#### 2.5.3 Field tag tables

`Tree` envelope (top level):

| # | Field | Type | Notes |
|---|---|---|---|
| 0 | `v` | int | Format version. Currently `1`. |
| 1 | `methods` | `[str]` | Per-tree method dictionary. |
| 2 | `params` | `[str]` | Per-tree param-key dictionary. |
| 3 | `root` | `Node` | Root node of the call tree. |

`Node` — **merged**: one node aggregates all sibling invocations of the same method under a parent.

| # | Field | Type | Required | Notes |
|---|---|---|---|---|
| 0 | `methodIdx` | int | yes | Index into top-level `methods`. |
| 1 | `durationMs` | int | yes | Total wall-clock, self + children. |
| 2 | `selfDurationMs` | int | yes | Time in this method only (`durationMs − Σ children.durationMs`). |
| 3 | `suspensionMs` | int | yes | Total suspension (self + children), attributed from the suspend timeline (§3, `08-ui-backend-requirements.md` R7). |
| 4 | `selfSuspensionMs` | int | yes | Suspension in this method only. |
| 5 | `executions` | int | yes | Total invocations aggregated into and below this node. |
| 6 | `selfExecutions` | int | yes | Invocations of this method directly under its parent. |
| 7 | `params` | `[Param]` | no | Omitted if the node has no params. |
| 8 | `children` | `[Node]` | no | Omitted for leaf nodes. |
| 9+ | reserved | — | — | Future additions (e.g. `cpuMs`, `memBytes`) use the next free numbers. |

> **v1 redefined (Stage 5, 2026-07-05).** The original v1 modeled a *raw* per-invocation tree
> (`enterMsRel` + a plain `durationMs`, no aggregation). No consumer shipped against it, so v1 is redefined
> here as the merged tree the UI needs (`08-ui-backend-requirements.md` R5–R7) rather than bumped to `v: 2`.
> `enterMsRel` and first/last-invocation offsets are dropped; raw per-invocation fidelity stays available via
> `/calls/{pk}/trace`.

`Param` — an aggregated mini-tree (`08-ui-backend-requirements.md` R11), not a flat value list: a node can
hold thousands of SQL texts and binds, so values fold into groups server-side.

| # | Field | Type | Required | Notes |
|---|---|---|---|---|
| 0 | `paramIdx` | int | yes | Index into top-level `params`. |
| 1 | reserved | — | — | The pre-R11 flat `values` list (see the registry below). |
| 2 | reserved | — | — | The pre-R11 `unresolved` index list (see the registry below). |
| 3 | `groups` | `[ParamGroup]` | yes | Value groups, ordered `durationMs` descending; the `::other` bucket, when present, is last. |

`ParamGroup`:

| # | Field | Type | Required | Notes |
|---|---|---|---|---|
| 0 | `value` | str | yes | The group's representative value: the first-seen full text; the literal `::other` for the overflow bucket; the reference text `<stream>:<seq>:<offset>` when unresolved. |
| 1 | `durationMs` | int | yes | Σ total duration of the invocations that carried a value of this group. Values co-occurring on one invocation each carry its full duration — sum groups of one param and the total can exceed the node's `durationMs`. |
| 2 | `executions` | int | yes | Number of invocations folded into this group. |
| 3 | `params` | `[Param]` | no | Nested params — binds under their SQL. Omitted when empty. |
| 4 | `unresolved` | bool | no | The value is an unresolved big-parameter reference (§2.5). Omitted when false. |

**Aggregation semantics** (ported from the Java `apps/agent/parsers/` `Hotspot` / `TreeBuilderTrace`, deviations noted):

- **Group key.** Values group per param by the normalized signature when the param is SQL-shaped —
  it arrived as `PARAM_BIG_DEDUP` (the deduplicated big-value stream carries SQL by construction,
  `01-write-contract.md` §4.4) or its key word is `binds` — and by the exact value otherwise. The
  normalization is the old UI's `signatures.sql` (`apps/agent/profiler-ui/src/profiler.mjs:3469`): drop commas, strip
  single-quoted literals (`''` escapes included) and digits, abbreviate every word to its first character,
  strip whitespace. *Deviation:* the Java aggregation keyed a group by an invocation's whole value-set; the
  per-value key is what makes the signature axis work, and one invocation's duration is attributed to a
  given group at most once either way.
- **Attribution.** Each invocation adds its own total duration to every distinct group its values fall
  into, and 1 to that group's `executions` — the Java `tag.totalTime += invocation total; count += 1`.
- **Top-N and `::other`.** A container holds at most 256 groups (the Java `Hotspot.MAX_PARAMS` default; a
  container is a node's top-level params jointly, or one group's nested params). Overflow evicts the
  current smallest-`durationMs` group into its param's `::other` bucket, which sums the evicted durations
  and executions, never evicts, and does not count against the cap. An evicted group's nested params are
  folded away — `::other` keeps totals only. *Deviation:* eviction picks the true current minimum (the
  Java priority queue could act on a stale ordering).
- **Binds nesting.** Within one invocation, `binds` values nest under that invocation's most recent
  `PARAM_BIG_DEDUP` group (the SQL they bind); a `binds` value with no preceding SQL in its invocation
  stays a top-level param.

**Reserved-number registry.** When a field is removed in a future version, its number is added below and never re-used.

| Record | # | Was | Removed |
|---|---|---|---|
| `Param` | 1 | `values [str]` — the pre-R11 flat value list | 2026-07-05, replaced by `groups` (R11) |
| `Param` | 2 | `unresolved [int]` — indexes into `values` | 2026-07-05, replaced by the per-group `unresolved` flag |

#### 2.5.4 Versioning rules

- **Additive changes (new fields).** New int key appended at the next free number. Old clients skip unknown keys; new clients see new data. **No `v` bump.**
- **Field removal.** Server stops emitting; number moves to the reserved registry above. **No `v` bump.**
- **Breaking changes** (rename, type change, restructure of the tree shape). Bump `v` to `2`. Server emits `v: 1` when client sends `Accept-Version: 1` (request header), `v: 2` otherwise. Old `v: 1` support kept for 6+ months past the `v: 2` launch, then retired with a release note.

#### 2.5.5 Compression

The wire payload goes through standard HTTP `Content-Encoding: gzip` if the client advertises `Accept-Encoding: gzip`. UI does by default; MCP/CLI are expected to as well. Saves another 30–50% on large trees. Server-side gzip is cheap (`compress/gzip`); no separate dictionary needed since the per-tree dictionary already shares strings inside this response.

### 2.6 Dictionary endpoint

`GET /api/v1/pods/{pod-restart}/dictionary` returns the per-pod-restart dictionary that the blob references.

Path serialization for the pod-restart key: `<ns>:<svc>:<pod>:<restartMs>` (same convention as the Call PK, minus the call coordinates).

Response:

```json
{
  "version": 4231,
  "methods": ["com.example.Service.handle", "com.example.Service.tx", "..."],
  "params":  ["request.id", "user.tenant", "..."]
}
```

- `methods[i]` and `params[i]` resolve `method_id = i` and `param_id = i` references inside the blob.
- `version` is a monotonic counter, incremented each time the dictionary grows during a live pod-restart. ETag is `(pod-restart, version)`.

> **TODO (dictionary shape):** this endpoint models `methods` and `params` as two independent id spaces (`method_id = i` into `methods`, `param_id = i` into `params`), but the agent wire uses a single shared id space. The collector writes the full word list into both arrays, so a reader resolves correctly against either. A future revision should collapse this to one `words` array indexed by id, with `method_id` / `param_id` indexing it.
- The endpoint serves **live** pod-restarts only (TCP connection still open): `query` forwards the request to the collector replica hosting the pod-restart (via internal endpoint, §3), where the dictionary lives on local PV + RAM and may still grow. Clients revalidate with `If-None-Match`; on a no-change response 304 is returned. On growth, the full word list is returned (small enough that delta encoding is not worth the complexity).
- A **closed** pod-restart has no dictionary object: every sealed row carries the subset its own blob references in `dict_words_json` (`01-write-contract.md` §3.6, §5.2), and the cold `/tree` path resolves from the row alone. An advanced consumer of the raw `/trace` path (§2.5) resolves a cold blob against the same column.

### 2.7 Pods and stats

`GET /api/v1/pods?from=...&to=...` returns the set of `(namespace, service, pod, restart_time)` tuples that have any Call rows in the time range. The set is the union of two sources: live pod-restarts from the hot tier (`/internal/v1/pods` on each replica, §3) and closed pod-restarts from the cold pod manifests. Cold discovery LISTs `pods/v1/<yyyy>/<mm>/<dd>/` for each day the range spans and reads each small JSON manifest (`01-write-contract.md` §3.6); it does not open parquet files. The `<podRestartHash>` in a parquet key is a one-way hash, so the manifest is the only cold source of the readable identity tuple. The response is an envelope `{ pods, partial, partial_reasons }` — the same partial-failure shape as `/calls` (§7.4), since the pods fan-out can also partially fail. Each `pods` entry is `{ namespace, service, pod, restart_time_ms, time_min_ms, time_max_ms }`, and the hot and cold sources union on the entry shape without reshaping.

`GET /api/v1/stats` is specified in §2.9.

### 2.9 GET /stats

`GET /api/v1/stats` aggregates the calls that match a `/calls` query. It serves the UI's histogram, its "N calls ·
M errors" readout, and its value suggestions, and the agent-access operations `hotspots`, `timeline`, and `compare`.

Query parameters:

| Param | Type | Notes |
|---|---|---|
| `from`, `to`, `filter`, and the legacy shorthand | | As for `/calls` (§2.3, §2.3.3). |
| `group_by` | string | Required. `time`, or a field of §2.3.3. |
| `buckets` | int | `group_by=time` only. Default 60, max 1000. |
| `limit` | int | Field grouping only. Default 20, max 1000. |
| `order` | string | Field grouping only. `count_desc` (default) or `duration_sum_desc`. |

Response:

```json
{
  "group_by": "service",
  "rows": [
    {
      "key": "billing",
      "count": 1200, "errors": 14,
      "duration_ms_sum": 384000, "p50_ms": 120, "p95_ms": 950, "p99_ms": 2100,
      "cpu_time_ms_sum": 201000, "wait_time_ms_sum": 9000, "queue_wait_ms_sum": 1500, "suspend_ms_sum": 300
    }
  ],
  "other": null,
  "distinct": 1,
  "totals": { "count": 1200, "errors": 14 },
  "approximate": false,
  "partial": false,
  "partial_reasons": []
}
```

**Rows.** Every row has the same members: the key, the call count, the error count, the sum and the 50th, 95th, and
99th percentiles of `duration_ms`, and the sums of `cpu_time_ms`, `wait_time_ms`, `queue_wait_ms`, and `suspend_ms`.
A row with no calls has `null` percentiles.

**`group_by=time`.** The window splits into equal buckets of `bucket_ms = max(1, ceil((to - from) / buckets))`
milliseconds, and the response adds a `bucket_ms` member. Bucket `i` covers
`[from + i·bucket_ms, from + (i+1)·bucket_ms)`, the last bucket ends at `to` and may be shorter, and a call falls into
the bucket that holds its `ts_ms`. The key is
the bucket's start in Unix ms. Every bucket is a row, in time order, including the empty ones, so a client draws the
histogram without filling gaps. `other` is `null` and `distinct` is the number of buckets.

**`group_by=<field>`.** One row per value of the field, the top `limit` by `order`, with ties broken by the key
ascending. `count_desc` ranks by call count, and `duration_sum_desc` by the sum of `duration_ms`, which is the "where
does the time go" ranking of `hotspots`. `other` is a row without a key that aggregates every value past the top
`limit`, or `null` when there is none. `distinct` is the number of distinct values. A key is the field's value as
§2.3.3 defines it, so `<field>=<key>` is a condition that selects the row's calls. A call that lacks the field (a
param it does not carry) falls into no row; it still counts in the totals.

**Totals.** `totals` counts the calls and the errors under the conditions, across every row and `other`.

**Percentiles.** Each group keeps a DDSketch with a relative accuracy of 1%: logarithmic bins of ratio
`γ = 1.01 / 0.99`, plus one bin for `duration_ms = 0`. A reported percentile is within 1% of the exact value of that
rank, before rounding to whole milliseconds. The rank of the `q` percentile over `n` calls is `floor(q · (n − 1))`, in
ascending order of duration. Sketches merge by adding bin counts, so the hot replicas and the cold scan each build
their own, and `query` merges them without loss.

**Sources and duplicates.** `/stats` has no cursor and no row-level merge, so it cannot dedup by PK the way `/calls`
does (§6). It splits the window at `t_lo`, the earliest `hot_window_oldest_ms` of the healthy replicas, clamped to
`[from, to]`:

- **Below `t_lo`, cold only.** No healthy replica serves a call older than its own `hot_window_oldest_ms`, and a
  replica drops a call-index partition only after every row of its bucket is uploaded (§3), so the cold scan holds
  every call in `[from, t_lo)` and aggregates it alone.
- **From `t_lo` on, hot first, then the cold remainder.** Each replica aggregates the rows it holds in `[t_lo, to)`
  and also returns the set of their PK hashes: 64 bits of `sha256` over the URL form of the PK (§2.2). The cold scan
  reads `[t_lo, to)` too, and counts a row only when its PK hash is in no replica's set. A call that is in both tiers
  is counted once, from hot; a call that the cold tier holds alone, such as one from a replica that has already
  dropped it, is counted from cold.

This holds for any mix of replica windows: a new or empty replica reports `hot_window_oldest_ms = now` (§4.3), which
moves no boundary, because `t_lo` is the earliest window rather than the latest. A window that ends before `t_lo` is
cold only and reads no hash sets. A hash collision makes the cold scan skip a call that no replica holds, with a
probability near `n² / 2⁶⁵` for `n` hot calls, which is about 3·10⁻⁶ at ten million. The hash sets are charged to the
read memory budget (§7.5) at 8 bytes per call. When the budget cannot hold them, `query` drops the hot aggregates and
proceeds as for a degraded hot tier, and `partial_reasons` holds `read_budget_exhausted`. The hot tier is degraded
when discovery fails, when it finds no replica, or when no replica answers its health probe (§4.3); the cold scan
then aggregates the whole window without a hash check and the response is `partial`. A replica that fails its probe
or its `/internal/v1/stats` call while others answer is left out: `t_lo` comes from the replicas that answered the
probe, the calls only the failed replica holds are missing, and the response is `partial`. Leaving it out counts no
call twice, because the cold scan checks only the hash sets it received. Each replica aggregates its own rows through
`/internal/v1/stats` (§3) and returns mergeable partial aggregates.

Compaction leaves its output and its inputs side by side until the delete grace has passed (`01-write-contract.md`
§6.6), so the cold scan removes that overlap by lineage, not by PK. Before it aggregates, it reads the footer of each
discovered compacted object (its key carries the `maintain` replica token) and skips every discovered object that a
footer's `profiler.compaction_inputs` lists, including a compacted object that a later compaction took as input. This
costs one footer read per discovered compacted object before the scan starts. A call that two replicas both hold after
a replica transition (§6.2) still counts twice; that case is rare, and removing it would need a PK set as large as the
window.

A call in both tiers is counted from its hot copy, unlike `/calls`, which prefers the cold copy (§6.3). The hot index
holds `error` and `suspend_ms` as of the call's arrival, and the sealed row can differ, so for calls younger than the
hot retention the `/stats` error count and suspension sum can differ slightly from what `/calls` shows.

**Retention.** `/stats` counts the calls the store still holds. The clean classes have shorter TTLs than the error
classes (`01-write-contract.md` §6.4), so a window older than two days holds no `short_clean` calls, and its counts,
error rates, and percentiles describe the longer calls only. A client that compares two windows (`compare`) keeps
both inside the shortest TTL that matters to it, or adds `duration>=100ms` to both.

**Bounded memory.** A source keeps at most `PROFILER_STATS_MAX_GROUPS` (default 10,000) groups. Past that, it tracks
the groups with Space-Saving, weighted by the `order` metric: the heaviest groups stay, a new value replaces the
lightest one, and the response sets `approximate: true`. The new value inherits the `order` metric of the value it
replaces (the call count for `count_desc`, the `duration_ms` sum for `duration_sum_desc`), as Space-Saving requires.
That metric of an approximate row can then exceed the exact value, by at most the window's total of it divided by
`PROFILER_STATS_MAX_GROUPS`; the row's other members cover the calls since the value last entered the summary.
`distinct` is then a lower bound. A `time` grouping holds at most 1,000 buckets and is never bounded this way.
Groupings by a high-cardinality param, such as a request ID, are the expected case; the stage A groupings rarely reach
the limit.

**Guard, budget, and errors.** The wide-query guard (§2.3.2) applies with the same layers, the same narrowing
filters, and no exemption for an order: an aggregate reads every file of the window, so its cost is the full
estimate. The read memory budget (§7.5) charges the cold scan's batches and every source's groups and sketches. A
denial of any of these answers `503` as on `/calls` (§7.5); only the hash sets above degrade to `partial`.
`partial` and `partial_reasons` follow §7.4. A missing or unknown `group_by`, `buckets` or `limit` out of range, an
`order` other than the two above, and a parameter that does not apply to the grouping all answer `400` with
`code: invalid_request`.

**Stages.** `query` implements the groupings in two stages. A grouping the build does not implement answers `400`
with `code: invalid_request` and a detail that names it, and `filter_capabilities.group_by` (§2.3.4) lists the
implemented ones.

| Stage | Groupings |
|---|---|
| A | `time`, `method`, `namespace`, `service`, `pod` |
| B | Call params, the derived fields `class` and `endpoint`, `error`, `retention_class`, and `param_name` |

`group_by=param_name` counts, for each call-param name in the window, the calls that carry it; the UI's filter bar
offers those names as fields. It takes `limit` and `order` like a field grouping.

## 3. Internal hot-read API on collector

Base path: `/internal/v1`. Same JSON shapes as `/api/v1`. Aggregation is done in `query`, not in collector, except that a replica builds the mergeable partial aggregates of `/internal/v1/stats`.

| Method | Path | Purpose |
|---|---|---|
| GET | `/internal/v1/pods` | Pods/restarts this replica holds data for. Used by `query` for targeted fan-out. |
| GET | `/internal/v1/pods/{pod-restart}/dictionary` | Same shape as `/api/v1/pods/{pod-restart}/dictionary` (§2.6). For live pod-restarts hosted by this replica. |
| GET | `/internal/v1/pods/{pod-restart}/suspend` | The pod-restart's stop-the-world timeline from the replica's `suspend.wal` mirror: `{ "events": [{ "end_ms": ..., "duration_ms": ... }] }` — the same event shape the sealed rows inline as `suspend_json` (`01-write-contract.md` §3.6, §5.2), so a consumer parses one format on either tier. `query` intersects it with node work intervals for the per-node suspension of `/tree` (§2.5.3, `08-ui-backend-requirements.md` R7). |
| GET | `/internal/v1/pods/{pod-restart}/values` | Batched big-parameter values from this replica's `sql` / `xml` segments: `?ref=<stream>:<seq>:<offset>` (repeatable) → `{ "values": { "<ref>": "<value>", ... } }`. A reference that does not resolve is absent, and `query` marks it `unresolved` in the tree (§2.5.3). Internal only — the external API never exposes the value streams (§2.5). |
| GET | `/internal/v1/calls` | Same params as `/api/v1/calls`, with the keyset in place of `cursor`: the position's sort key in `after_ts_ms` for the `start` orders, in `after_duration_ms` for the `duration` orders, and in `after_key` for the others (`net_read + net_written` for `net`), with `after_pk`. Only a duration order leaves `after_pk` out, for a position with no PK (§2.3.5). Returns only rows this replica holds, and echoes the order it sorted by as `order`. A replica without the echo predates `order` and returns its newest rows, so for any other order `query` counts it as a failed source and marks the page partial (§7.4) instead of merging rows in the wrong order. A replica that knows `order` but not the one asked for answers `400`, which counts the same way. |
| GET | `/internal/v1/calls/{pk}` | Single-row fetch from this replica. |
| GET | `/internal/v1/stats` | Same params as `/api/v1/stats`, over the rows this replica holds. Returns every group up to `PROFILER_STATS_MAX_GROUPS` with its DDSketch bins rather than percentiles, no `other` row, and the PK hashes of the rows it aggregated, so `query` can merge the replicas and the cold scan, skipping cold rows a replica already counted, before it ranks and cuts to `limit` (§2.9). Three more parameters serve the fan-out: `bucket_origin_ms` and `bucket_ms` place the calls of `[t_lo, to)` on the buckets of the external window, and `max_groups` passes `query`'s `PROFILER_STATS_MAX_GROUPS`. The hashes travel as `pk_hashes`, the base64 of their 8-byte big-endian forms. |
| GET | `/internal/v1/calls/{pk}/trace` | Blob from this replica. |
| GET | `/internal/v1/health/hot-window` | `{ "hot_window_oldest_ms": ..., "hot_window_now_ms": ... }`. Reports the earliest `ts_ms` this replica still serves. Lets `query` compute the cold cutoff dynamically (§4.3). |

Sources this replica reads from when serving `/internal/v1/*`:

1. The SQLite call index — filter, sort, and paginate `/internal/v1/calls` over it; it holds one row per call (pointer + filter columns). It is partitioned by time bucket into `calls-<bucket>.sqlite` files (`01-write-contract.md` §8); the replica ATTACHes the partitions overlapping the query range.
2. The gzip trace segments in `trace/*.gz` — `/internal/v1/calls/{pk}/trace` decompresses the segment(s) covering the call (located via the SQLite segment catalog) and slices the blob.
3. The gzip value segments in `sql/*.gz` and `xml/*.gz` — the blob carries `(rolling_seq, offset)` references into them (`PARAM_BIG_DEDUP` → `sql`, `PARAM_BIG` → `xml`; `01-write-contract.md` §4.4). The values endpoint reads them for the `/calls/{pk}/tree` rendering in `query` (§2.5); the raw `/calls/{pk}/trace` blob keeps the references and an advanced consumer resolves them itself.
Neither open nor sealed local parquet is a query source. An open parquet file has no footer and is not randomly readable; a sealed one is redundant with the SQLite index. A call-index partition is dropped only after every `parquet_local` row of its bucket is uploaded and `hot_retention` has elapsed, and drops run oldest-first behind a contiguity barrier — a stuck bucket pins itself and every newer bucket — so the index always covers what the local parquet holds (`03-lifecycle.md` §3.9). Reading it would only duplicate rows the PK-dedup then collapses. The replica never reads from S3 to serve `/internal/v1/*`; S3 is `query`'s job via the cold path.

> **Known gap.** The hot call-index carries no truncation or blob-size data, so hot `/internal/v1/calls` rows always serialize `truncated_reason` and `trace_blob_size` as `null`. A call truncated under pressure reads as intact from the hot tier until its cold parquet row, which does carry them, becomes authoritative.

## 4. Hot / cold model

### 4.1 Tiers

- **Hot tier** = collector replicas. Each holds data for its assigned pod-restarts (sticky TCP) in: the SQLite call index and the `trace` / `sql` / `xml` segments (un-sealed calls), and already-sealed parquet files retained on the local PV for `hot_retention` past seal.
- **Cold tier** = S3. Authoritative copy of everything flushed.

### 4.2 Hot retention on PV

`PROFILER_HOT_RETENTION` (default `15m`) is how long each collector keeps flushed parquet files locally past their flush. Must satisfy `hot_retention ≥ seal_interval + overlap_margin`.

This is the standard pattern (Prometheus head block + WAL, VictoriaMetrics in-memory + on-disk, Loki ingester + object storage): the hot tier intentionally overlaps with cold for a grace window. Queries cover both tiers, dedup by PK, and obtain a consistent view with no "gap risk" between flush completion and cold visibility.

Local parquet lifecycle:

1. Parquet writer closes a file when its time bucket ends (`01-write-contract.md` §6.1).
2. File is uploaded to S3 (idempotent key, §7 of write contract).
3. On `200 OK`, segment refcounts are decremented (`01-write-contract.md` §6.2). **The local file is NOT deleted yet.**
4. A janitor goroutine on the replica deletes the local file when `now > flush_ts + hot_retention`.

### 4.3 Overlap and cutoff

Given a query for `[from, to]`:

- **Hot fan-out:** every collector replica is queried for `[max(from, replica.hot_window_oldest_ms), to]`.
- **Cold LIST:** S3 read for `[from, coldTo]`. The cutoff is dynamic, not a static `now - hot_retention`: `coldTo = min(to, max over healthy replicas of (hot_window_oldest_ms + overlap_margin))`. The *max* — the youngest hot window — is what zero-gap needs, since anything below it lives only in cold. `query` reads each replica's window from `/internal/v1/health/hot-window` (§3); an empty replica reports `oldest = now`. Any degraded hot state (discovery uninitialized, resolution failed, zero replicas, or a failed health probe) widens `coldTo` to the full `to`, so the guarantee never rests on an unreachable replica.
- **Overlap window** = `[coldTo - overlap_margin, coldTo]`, the band both tiers serve.
- After merge, dedup by PK (§6) collapses any overlap. A window that ends inside every replica's hot coverage skips the cold LIST entirely.

`overlap_margin` defaults to one `seal_interval` (5 min). Tunable via `PROFILER_OVERLAP_MARGIN`.

Result: every flushed Call is visible from at least one tier from the moment it leaves in-memory state, and from BOTH tiers for `overlap_margin` after that. Zero-gap guarantee, bounded duplication cost.

Result consistency is eventual within seconds: a just-arrived, not-yet-sealed call may be briefly invisible to `/calls` until its bucket seals. Stable cursor ordering across the hot→cold migration is specified in §2.3.1 — keyset pagination on the tier-independent `(ts_ms, pk)` order makes migration transparent, so a call keeps its page position wherever it lives. `partial` handling (§7.4) is unchanged.

## 5. S3 LIST-based discovery

### 5.1 Time range → S3 prefixes

Path layout (`01-write-contract.md` §7):

```
s3://<bucket>/parquet/v1/<retentionClass>/<yyyy>/<mm>/<dd>/<hh>/<replica>-<podRestartHash>-<timeBucketStart>-<timeMin>-<timeMax>-<seq>.parquet
```

For range `[t1, t2]`:

1. For each retention class in the filter (default: all 5).
2. Walk the hour list between `floor(t1, 1h)` and `ceil(t2, 1h)`.
3. LIST each `<retentionClass>/<yyyy>/<mm>/<dd>/<hh>/` prefix in parallel.
4. Parse `<timeMin>` and `<timeMax>` from each object key (`01-write-contract.md` §7) and keep every file whose `[timeMin, timeMax]` overlaps `[t1, t2)` (`timeMin < t2` and `timeMax ≥ t1`). Both bounds ride in the key, and `ListObjectsV2` returns the key with every listed object, so overlap is decided straight from the LIST: no parquet footer read and no per-object HEAD (`ListObjectsV2` returns only the key, size, and ETag, not user metadata). The seal pass writes each file's true `min(ts_ms)` / `max(ts_ms)` into these fields (`01-write-contract.md` §6.2), so the set of opened files is exact at file granularity. A `ts_ms ∈ [t1, t2)` filter still runs when a file is read: a sparse file can span the window without holding a row inside it.

The same LIST result powers the wide-query guard (§2.3.2): each entry's size and its key-encoded `retention_class` sum to `(file_count, total_bytes)` per class before any file is opened, which is the estimate the guard's cost layer gates on.

Late arrivals need no special handling. A late call re-seals into a patch file under the same `<timeBucketStart>` but with its own `<timeMin>` / `<timeMax>` over the late rows (`01-write-contract.md` §6.6), so the overlap test finds it like any other file. A long-running call is filed by its start, so its `ts_ms` — and the hour prefix that holds it — always fall inside the walk of steps 2–3; there is no earlier bucket to widen for.

Discovery tolerates compaction. A `maintain` compaction (`01-write-contract.md` §6.6) may delete a listed object between the LIST and the read; discovery treats a `404` on a listed key as an empty result, not an error. The write-side delete-grace (`01-write-contract.md` §6.6) keeps the pre-compaction inputs readable long enough that a query which listed them still reads them, so this backstop only fires for a read that outlives the grace.

### 5.2 Parallelism

Up to `PROFILER_S3_LIST_CONCURRENCY` (default `16`) parallel LIST calls per query. Tens to hundreds of LISTs for multi-day ranges.

### 5.3 Manifest deferred

Start with LIST by prefix; add an S3 manifest file only if LIST profiles slow at scale.

### 5.4 Secondary index deferred

No secondary index in the MVP. `method` substring, `params`, and `/stats` filters scan the candidate parquet column data over the requested range; a full scan is accepted. Add an index only if profiling shows it is needed.

### 5.5 LIST scaling

Discovery has two independent cost axes. The date hierarchy (steps 2–3) bounds how many objects a LIST *enumerates*; the `<timeMin>` / `<timeMax>` overlap test (step 4) bounds how many of those a query *opens*. Folders govern the first, the key range the second. The wide-query guard (§2.3.2) gates on the same two axes: `PROFILER_MAX_SCAN_FILES` bounds enumeration, `PROFILER_MAX_SCAN_BYTES` the opened volume.

**Objects under one hour prefix**, for one retention class:

```
objects(1 hour, 1 class) ≈ (60 / bucket_minutes) × active_pod_restarts × (1 + patches_per_bucket)
```

With the 5-minute default that is `12 × P × f`: `P` is the pod-restarts that wrote in the hour (≈ the running pod count plus restart churn), and `f ≈ 1.3` covers late-data patch files (`01-write-contract.md` §6.6) and size-split `<seq>` files (`01-write-contract.md` §6.1). A 200-pod cluster lists ~3,000 objects per hour-class; a 2,000-pod cluster ~30,000.

**The hour, not the year.** `ListObjectsV2` on a prefix costs `O(keys under the prefix)`, not `O(bucket size)`. A "last hour" query lists one `<yyyy>/<mm>/<dd>/<hh>/` prefix per class; other days sit under other prefixes and never enter the scan.

**Pagination is sequential within a prefix.** A prefix returns at most 1,000 keys per page, and each page needs the prior page's continuation token, so a 30,000-object prefix is 30 round-trips in series. `query` parallelizes across prefixes (§5.2), never within one — a fat hour prefix serializes, and that, not the object count itself, is the latency metric to watch.

The pod-restart factor `P` is cut at its source by cross-pod-restart compaction, part of the `maintain` job (`01-write-contract.md` §6.6): merging the small per-`(bucket, retention_class)` files across pod-restarts shrinks the object count itself, not just the LIST latency.

Two further levers, applied only when LIST profiling shows a real bottleneck (do not pre-optimize the key):

1. **Finer path granularity** — add a 5-minute segment (`.../<hh>/<HHMM>/`). A "last hour" query then lists 12 shallow prefixes in parallel instead of one deep one, cutting the serial page count without changing the object count.
2. **Manifest** — replace the per-hour LIST with a single `GET` of a manifest object that lists the hour's files (§5.3). The endgame for very large clusters: thousands of enumerated keys become one read.

## 6. Deduplication

### 6.1 PK as dedup key

PK (§2.2) is unique per Call across all time and all replicas.

### 6.2 When duplicates appear

- **Overlap window (expected).** Same Call visible from both hot (collector local parquet) and cold (S3) during `hot_retention - seal_interval ≤ age < hot_retention`. This is the normal case.
- **Replica transition (rare).** During scale-out or pod restart, sticky TCP may temporarily land an agent on two replicas in quick succession. Each writes its own copy of the affected Calls. Both copies have the same PK; dedup collapses to one.
- **Upload retry.** The S3 PUT object key is deterministic (`01-write-contract.md` §7), so retries overwrite the same key — no S3-side duplicates.

### 6.3 Tiebreaker

When merge sees the same PK from multiple sources, prefer **cold (S3)** over hot. Both copies are byte-identical (same parquet file uploaded once), so the choice is for determinism, not correctness. If only hot has it, use hot.

### 6.4 Why `query` always dedups

Even when sticky TCP routes one agent to one replica, `query` never relies on that for correctness. Replica failover, network partitions, or transient routing flaps can violate the "one source" assumption. Dedup by PK costs ~O(rows) hashing on merge and protects against all these cases.

## 7. Fan-out

### 7.1 Discovery

`query` reads `COLLECTOR_HEADLESS_SVC` env (e.g. `collector-headless.profiler.svc.cluster.local`). `net.LookupHost` returns one A record per Ready collector pod. Re-resolved on every external request (OS DNS cache typically capped at the CoreDNS TTL, ~30s).

### 7.2 Parallel requests

For each hot fan-out, `query` issues N parallel GETs. Per-request timeout `PROFILER_FANOUT_TIMEOUT` (default `2s`). Replicas exceeding timeout produce a `partial_reasons` entry but do not fail the whole query.

### 7.3 Replica targeting (optimization)

For queries that filter by `pod=...`, `query` calls `/internal/v1/pods` on each replica first to learn which pods that replica holds, then skips replicas with no relevant data. Optional optimization for large clusters; off by default.

### 7.4 Partial results

If at least one replica or S3 LIST fails:

- `partial: true` in the response.
- `partial_reasons: [...]` lists what failed (replica IP, S3 LIST prefix, exception summary).
- The client shows results with an explicit "data may be incomplete" indicator.

A profiler is most useful when at least partial data is shown — failing the whole query because one replica is slow defeats the purpose.

**Per-request scan budget (still deferred).** Layer 2 of the wide-query guard (§2.3.2) estimates scan cost before
reading, but the estimate is by file size: it overshoots a projection-only read and cannot see a pathological row
distribution. A per-request fail-soft backstop — stop mid-execution past a hard byte or deadline cap and return the
rows gathered so far with `partial: true` and `partial_reasons: [budget_exceeded]` — remains deferred, and the
`budget_exceeded` reason stays reserved for it. The load campaign fired this entry's revisit trigger with a harder
finding: concurrent guard-passing queries OOM the pod, which no per-request cap can bound. That need is now met by
the process-wide read memory budget (§7.5), which deliberately rejects with an atomic `503` instead of the partial
form — see §7.5 for why a budget-truncated page cannot carry a cursor safely.

### 7.5 Process-wide read memory budget and admission

The read path shares one process-wide byte budget (`PROFILER_READ_MEMORY_BUDGET`) that every reader charges BEFORE
materializing decoded data and releases when the data is handed off or dropped. It closes the two holes the §2.3.2
guard cannot: the unbounded compressed→decoded expansion factor, and N concurrent guard-passing requests holding N
per-request budgets at once — the OOM shape of `load-testing-report.md` §7. The guard and the budget divide the
work: the guard is a cheap per-request pre-flight in compressed LIST units, fail-closed `400` with narrowing advice
("this query is intrinsically too wide — narrow it"); the budget is runtime admission control in materialized
units ("the process cannot afford this right now — wait or narrow").

**Units of charge.** The budget counts bytes the readers are about to materialize, never `runtime.MemStats`:

- **Cold list scan and the point fetch's PK scan** read in batches of a light projection. A batch of K rows is
  charged from the footer before decoding: `perRow = max(Σ uncompressed size of the projected columns / rows,
  sizeof(row struct) + container overhead)`, `K = clamp(target batch bytes / perRow, min, max)` (fat rows shrink
  the batch instead of inflating its charge), times a safety factor. The struct floor matters: dictionary-encoded
  short strings make the uncompressed size tiny while the reader still builds one Go struct per row. After the
  read, the charge is reconciled to the actual per-row accounting footprint — up (a `Grow`, counted by
  `profiler_query_read_budget_batch_overruns_total`) or down.
- **The point fetch's surgical phase** (below) charges one parquet page per heavy column: priced from the offset
  index and the chunk metadata before it is decoded, dictionary page included, reconciled to the decoded page's
  size after. A page is indivisible — `SeekToRow` decodes the whole page holding the target row — so one page, not
  one file, is what must fit the budget.
- **Hot fan-out bodies** (`/internal/v1/calls` pages, trace blobs) are charged by `Content-Length` before the read;
  a chunked body reserves fixed-size steps BEFORE each read, under hard caps that reject a runaway response
  outright (a declared length past the cap is refused on the headers alone).

Retained state is owned by one request-scoped **page lease** (or point lease for `/trace` and `/tree`): survivors
of each batch are deep-copied, their exact footprint moves into the request lease, and the temporary batch or page
charge is released. The retained accumulator is capped by the incremental merge at the page limit, so a request
holds one batch or page in flight plus its capped page — never a whole file, never a whole scan.

**Two-phase point fetch.** `/trace` and `/tree` never materialize a candidate file: phase 1 streams a PK-only
projection through the charged batch reader (blob columns are not read at all) and remembers the match as
(row group, row offset); phase 2 seeks each needed heavy column to that position and decodes exactly one charged
page per column. The scan phases open the file without the page index; the surgical phase opens it with the index —
that is what lets it price and locate the page before decoding. An object deleted between the phases answers
not-found, the same semantics as a delete after the LIST (§5.1). Point-fetch memory is therefore independent of
file size: a compacted object far larger than the whole budget still serves, page by page.

**Queueing.** Admissions (`Acquire`) form a strict FIFO: a wide request is not starved by a stream of small ones.
Resizes (`Grow`) of already-held leases bypass the FIFO and are served with priority over the admission head —
a grower blocks everyone behind its held capacity, so finishing it drains fastest. The fairness guarantee covers
admissions only: a burst of grows can delay new acquires. Two grows (or a grow and a queued acquire) can mutually
block on an exhausted budget; the bounded wait (`PROFILER_READ_BUDGET_WAIT`) resolves that as a denial for the
loser rather than a deadlock, at the accepted cost of an occasional `503` while the budget is formally sufficient.

**Denial semantics: one atomic `503`.** Any budget denial inside `/calls`, `/trace`, or `/tree` fails the whole
request with `503` and a `Retry-After` header; rows already gathered are discarded and the client's cursor does not
move, so retrying the same page with the same cursor later is legal and cheap. A partial page was rejected
deliberately: a page emitted after unread files would advance the cursor past rows the denied sources never
produced — silently and irreversibly, since §2.3.1 keeps no per-source state — and `next_cursor = null` would fake
the §2.3.1 termination signal. The `partial` vocabulary stays reserved for source *failures* (§7.4), not for load
shedding. The problem body uses the guard's dialect — `suggested_filters`, and `estimated_files` /
`estimated_bytes` / `by_class` when discovery already ran — because narrowing genuinely shrinks the request's
appetite. Two denial reasons are distinguished in the detail and in
`profiler_query_read_budget_denials_total{reason}`: `exhausted` (transient contention; retry as advised) and
`never_fits` (structural: a single charge exceeds the whole budget — the budget is undersized for the data, not
the process overloaded).

**What the model honestly is.** For the light projections the budget is a hard bound to within one batch. For
blob-bearing pages and for the decoded-row reconcile it is a calibrated estimate trued up after materialization: a
skewed page or a dictionary-expanded batch can transiently exceed its pre-charge, which the reconcile then records
(`batch_overruns_total`). Outside the ledger entirely: the pods manifests and dictionaries (small JSON), the JSON
encoding of the response, the parquet reader's transient decompression buffers, and the `calltree` build peak
(proportional to a blob the ledger does hold). The budget is therefore not an absolute RSS ceiling; the sizing
rule below carries a factor for exactly these gaps, and the overrun counter shows when the estimates drift. A
write-time cap on blob and JSON column values would turn the page estimates into hard bounds; that is a
write-contract follow-up, not part of this mechanism.

**Sizing.** `budget ≤ (pod memory limit − baseline RSS) / overhead factor`, with the overhead factor ≈ 2 covering
the out-of-ledger items above. The default (512 MB) fits the default 2 Gi query pod with a ~256 MiB baseline.
Structural fit is bounded by the write side: sealed files split at `PROFILER_PARQUET_MAX_SIZE` (64 MB compressed,
`01-write-contract.md` §9) and compacted objects at `PROFILER_COMPACTION_MAX_BYTES` (96 MiB compressed), and only
single batches and single pages of them must fit — so `never_fits` at the default budget indicates a
misconfiguration, not expected operation.

## 8. Error responses

RFC 7807 Problem Details for actual errors (parameter validation, internal, downstream failures that produce zero data).

Every failure of `/api/v1` and `/internal/v1` answers in this envelope, with `Content-Type: application/problem+json` — including the failures no handler produces itself: a route miss, a wrong method, and an unexpected internal error. A client parses one error schema and never has to fall back on a status code alone.

| HTTP | Condition |
|---|---|
| 400 | Query parameter validation failed. |
| 400 | Wide query over `PROFILER_WIDE_RANGE_LIMIT` with no narrowing filter (§2.3.2, span layer). |
| 400 | Estimated scan over `PROFILER_MAX_SCAN_FILES` or `PROFILER_MAX_SCAN_BYTES` (§2.3.2, cost layer). |
| 400 | `/pods` window, or a pod-filtered `/calls` window, over `PROFILER_MAX_PODS_RANGE` (§2.7, §2.3.2). |
| 404 | PK not found, or `trace_blob = NULL` (blob endpoint). |
| 404 | No route matches the path. The embedded SPA serves its client-side routes but never an unmatched `/api/v1` path. |
| 405 | The path matches no route for the request method. With the SPA embedded, a non-GET request to an unmatched path lands here rather than on the route-miss row above, because the SPA catch-all is registered for `GET` alone. |
| 416 | The `Range:` of a blob request does not parse, or starts past the end of the blob (§2.4). |
| 500 | An unexpected internal failure. The detail is deliberately generic — the cause is in the server log, not in the response, so bucket names, object keys, and driver messages stay out of a client's hands. |
| 503 | `query` itself is not Ready (e.g., DNS discovery uninitialized). While the API handler is unmounted this covers every non-probe path, the SPA shell included; the `/health/ready` and `/health/live` probes keep the state body of `03-lifecycle.md` §4. |
| 503 | Read memory budget denied the request (§7.5) — atomic, with `Retry-After`; the body carries the guard-dialect members below and a detail naming the reason (`exhausted` vs `never_fits`). |
| 504 | Every *attempted* source failed and none succeeded (`succeeded == 0 && failed > 0`). A source legitimately skipped — cold below the cutoff, or a replica whose hot window misses the range — counts as neither, so a hot-only query still answers with S3 down, and a cold-only query still answers with every replica unreachable. |

Partial results (some sources failed but some succeeded) are NOT errors — `partial: true` in the body. See §7.4.

**Branch on `code`, not on prose.** `type` is always `about:blank`, and `title` and `detail` are written for a human — either may be reworded in any release. The `code` member names the condition, and is the only member of the envelope a client should switch on. It is optional in the sense that a response from an older build carries none; a client that finds no `code` falls back to the status.

| `code` | HTTP | Condition |
|---|---|---|
| `invalid_request` | 400 | A caller bug: a malformed, missing, or out-of-range parameter. Surface it — retrying the identical request gets the identical answer. |
| `cursor_rejected` | 400 | The cursor expired, did not decode, or was contradicted by re-sent filters (§2.3.1). Recoverable: a paging client restarts from page 1 rather than showing an error. |
| `query_too_wide` | 400 | A wide-query guard rejection (§2.3.2), span or cost layer; the guard extension members below carry the narrowing affordance. |
| `read_budget_exhausted` | 503 | A read memory budget denial (§7.5), with `Retry-After`. |
| `not_ready` | 503 | The service has not mounted its API handler yet (`03-lifecycle.md` §2). |
| `call_not_found` | 404 | No tier holds the call. |
| `trace_unavailable` | 404 | The call exists, but its trace blob does not — dropped at seal, or unassemblable from the hot segments. |
| `pod_restart_not_found` | 404 | `/internal/v1` only: the addressed replica hosts no such pod-restart. Distinct from `not_found` so a rolled pod-restart does not look like an endpoint this build lacks. |
| `no_source_available` | 504 | Every attempted source failed and none succeeded. |
| `not_found` | 404 | No route matches the path. |
| `method_not_allowed` | 405 | The path matches no route for this method. |
| `range_not_satisfiable` | 416 | The blob cannot serve the request's `Range:` (§2.4). The blob is immutable, so the same range never becomes valid: drop the header or correct it. |
| `internal_error` | 500 | An unexpected server-side failure. |

The two wide-query rejections (§2.3.2) extend the Problem Details body so a client can render a guided prompt instead of a bare error:

- `suggested_filters` — the narrowing filters that would admit the query (`pod`, `retention_class`, `duration_min_ms`, `error_only`). A build with the filter model (§2.3.3) also suggests `namespace` and `service`;
- `estimated_files` / `estimated_bytes` — the scan the query would have cost (cost layer only);
- `by_class` — `estimated_bytes` split by `retention_class`, so the UI can point at the dominant class.

The span-layer rejection omits the estimate members — it fires before the LIST. The §7.5 budget rejection reuses
the same members (minus the estimate when it fires before discovery), so one client affordance renders both.
Stage 5 UI renders these as a "narrow your query" affordance (`profiler-plan.md`).

## 9. Configuration

### Collector

| Env | Default | Description |
|---|---|---|
| `PROFILER_HOT_RETENTION` | `15m` | Local parquet retention past flush (§4.2). |
| `PROFILER_INTERNAL_API_PORT` | `8081` | Bind for `/internal/v1/*`. |

### Query

| Env | Default | Description |
|---|---|---|
| `COLLECTOR_HEADLESS_SVC` | — | DNS name for collector replica discovery (§7.1). |
| `PROFILER_OVERLAP_MARGIN` | `5m` | Hot/cold overlap window size (§4.3). |
| `PROFILER_FANOUT_TIMEOUT` | `2s` | Per-replica hot read timeout (§7.2). |
| `PROFILER_S3_LIST_CONCURRENCY` | `16` | Parallel S3 LIST cap (§5.2). |
| `PROFILER_CURSOR_TTL` | `15m` | Validity of a `/calls` pagination cursor (§2.3.1). |
| `PROFILER_STATS_MAX_GROUPS` | `10000` | Groups one `/stats` source keeps before it switches to an approximate top-N (§2.9). |
| `PROFILER_WIDE_RANGE_LIMIT` | `6h` | Span above which `/calls` requires a narrowing filter (§2.3.2). |
| `PROFILER_MAX_PODS_RANGE` | `8784h` (366 d) | Span above which `/pods` is rejected with `400`; `/pods` lists one S3 prefix per UTC day and has no narrowing filter (§2.7). Also caps a pod-filtered `/calls` window, which a `pod` filter exempts from `PROFILER_WIDE_RANGE_LIMIT` but not from the discovery-LIST fan-out (§2.3.2); lower it below the default if that fan-out — one LIST per hour prefix per class across the window — is the cost you want to bound. |
| `PROFILER_MAX_SCAN_FILES` | `10000` | Candidate-object ceiling for a `/calls` scan; over it, `400` (§2.3.2). |
| `PROFILER_MAX_SCAN_BYTES` | `2GB` | Estimated-scan-byte ceiling for a `/calls` scan; over it, `400`. Counts compressed object bytes, per request; size it for the widest single query the deployment allows — the pod is sized from the §7.5 budget, not from this guard. |
| `PROFILER_READ_MEMORY_BUDGET` | `512MB` | Process-wide read memory budget (§7.5): every reader charges materialized bytes against it. Size ≤ (pod limit − baseline) / overhead factor (≈2). |
| `PROFILER_READ_BUDGET_WAIT` | `5s` | Bounded queue wait on a budget charge (§7.5); past it the request answers `503` + `Retry-After`. A negative value is rejected at startup. |
| `PROFILER_EXTERNAL_API_PORT` | `8080` | Bind for `/api/v1/*` and the UI. |
| `PROFILER_METRICS_PORT` | `8081` | Bind for `/metrics` and `/debug/pprof`, on a listener the ingress does not publish (`04-storage-layout.md` §12). |
| `S3_ENDPOINT` / `S3_BUCKET` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` | — | Same as in `01-write-contract.md` §9. |

## 10. What this contract does NOT cover

- **Streaming / tail endpoint** for real-time observation — out of scope for MVP, defer to a future iteration once the batch model is stable.
- **Authentication and authorization** — out of scope for MVP. The likely choice (Keycloak/Bearer) will be a thin layer in front of `/api/v1/*`. It is planned for `/api/v1/*` and for the MCP server of the agent-access plan together; until then an MCP server stays inside the cluster.
- **Diagnostic dumps endpoint** — already served by `dumps-collector`, not re-implemented here.
- **UI specifics** — UI consumes `/api/v1/*` as documented; UI-internal data flow is Stage 5.

## 11. Review checklist

- [x] Endpoint paths and parameter naming conventions — accepted.
- [x] `partial: true` response shape — accepted. Client handles partial response and shows a warning indicator instead of failing the whole query.
- [x] Default `hot_retention` (`15m`) and `overlap_margin` (`5m`) — accepted.
- [x] Trace blob caching policy (immutable + 1y) — accepted.
- [x] `cutoff=strict` escape hatch — dropped from MVP; can be added later if a concrete consumer appears.
- [x] PK URL serialization with `:` separator — accepted; k8s pod/service names cannot contain `:`.
- [x] Server-decoded `/tree` endpoint — included as canonical; MessagePack with int-keyed maps and a `v` version envelope (§2.5).
- [x] Cursor / stable pagination (§2.3.1) — keyset on `(ts_ms DESC, pk ASC)`, frozen-query cursor, single global position, dedup-before-limit, TTL — accepted.
- [x] Dictionary cold-path lifecycle — final snapshot uploaded to S3 on pod-restart close; see `01-write-contract.md` §3.6.
- [x] Optional `/internal/v1/pods` targeting (§7.3) — implemented in collector in Stage 1b; left dormant in `query` until cluster size makes it worthwhile.
- [x] Wide-query guard (§2.3.2) — fail-closed `400`, two layers (span + post-LIST estimate); narrowing filters `{pod, retention_class, duration_min_ms, error_only}`; scan-budget backstop and stats manifest deferred — accepted.
