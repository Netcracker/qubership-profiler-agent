package mcpserver

// Instructions is what the server tells a client in its initialize result:
// a short form of the workflow and the query rules of the CLI skill
// (agent-packages/qubership-profiler-cli-usage). Change the two together.
const Instructions = `The Qubership profiler records every root call of a profiled JVM (an HTTP request, a message handler, a scheduled job) with its call tree. These tools read that data. They only read.

Why was a request slow:
1. profiler_list_services shows the namespaces, services, and pods with data. Use those names in filters.
2. profiler_find_calls finds the call. With a request or trace ID, filter on request_id or trace_id. With a time and a service instead, bracket the time with from and to, pass service, and keep slow calls with {"field": "duration", "op": ">", "value": "1s"}. request_id reads the x-request-id header only; a service may keep its ID in a call parameter of its own, so a filter on that parameter's name may find the call when request_id does not. A request that crossed several services has one call per service with the same trace_id; explain the slowest one that is not mostly waiting on the next service.
3. profiler_explain_call with the call's ref shows the hot path, the methods with the most self time, and the slowest SQL.
4. Go deeper only where the time is: pass node with a #node of the hot path to re-root there. A lower threshold or a larger max_nodes shows more. The notes say what was cut and which input shows it.

What is slow in a service:
1. profiler_hotspots with service groups the calls by root method and ranks them by total time. A large total with a small mean is called often; a large mean or p95 is slow per call. group_by pod shows whether one pod is worse; order count_desc ranks by call count.
2. Its sampled part lists the inner methods with the most self time and the slowest SQL across the slowest calls.
3. Explain one of the sampled calls it lists to see the path to the hot method. To study one group, profiler_find_calls with {"field": "method", "op": "=~", "value": "<part of its name>"} and order duration_desc lists its slowest calls.

What changed:
1. profiler_timeline with service shows calls, errors, and p50, p95, and p99 per time bucket. Find the first bucket where p95 or errors jumps.
2. profiler_compare_windows with from and to set at the change compares it with the window of the same length just before. baseline_shift 1d compares with the same hours a day before; baseline_from and baseline_to set the baseline outright.
3. Groups marked slower rose in mean or p95 by 20% or more. A group with more calls and no slower mark is load, not a regression. New and vanished groups have no call in one window; groups outside the top N or unknown lack figures for one window and are neither.
4. In the sampled part, the inner method or statement with the largest positive change is the first suspect. Explain a listed call to see where it sits.

Reading the results:
- CPU close to the duration means the call was computing; a large wait means it was blocked on I/O, a lock, or a pool. Queue wait is time queued before the call started.
- GC suspension is time in stop-the-world pauses of the whole JVM, not the call's own code.
- A node's total includes its children; its self time is spent in the method itself. Large self time is where the time went.
- Executions on a fast node under a slow parent mean a loop or an N+1 query.
- A call marked "no tree" was too big for the agent's buffers; it cannot be explained.
- A Partial line means a data source failed and the result may miss calls. Retry before concluding that a call does not exist.
- An Approximate line means the server kept only the heaviest groups; a group's figures may be overcounted.
- Hotspots and compare have two layers. The Exact tables count every call under the filters. The part headed "Sampled, not exact" reads the trees of the slowest calls only (20 for hotspots, 10 per window for compare), so it describes the slow tail, not the typical call. "in 19" counts the sampled calls that ran a method: 19 of 20 is a pattern, 1 of 20 an outlier. Confirm a sampled finding by explaining two or three of the listed calls before you report it.

Rules for queries:
- Pass a service or namespace whenever you know it.
- Keep the window at 6 hours or less unless a condition narrows the data: duration of 1s or more, error = true, or a service or pod. A request or trace ID does not narrow the scan, so bracket the time when you know it.
- Hotspots, timeline, and compare count every call of their windows, so a service or a short window matters more for them.
- Keep both windows of a compare within the last two days, or add a duration >= 100ms condition: fast successful calls age out, so an older baseline holds only the slower calls.
- "the query is too wide to scan" lists the conditions that would make it fit. Add one or shorten the window; the same query fails again.
- "the profiler is busy" means the server is short of read memory. Narrow the query or retry after a minute.

Limits: no thread dumps, heap dumps, or GC logs. Fast successful calls are kept for a short time (two days by default); slow and failed calls are kept longer. Filters and the exact statistics apply to root calls only: a method inside a call is found with profiler_explain_call or the sampled part of hotspots, not with a filter.`
