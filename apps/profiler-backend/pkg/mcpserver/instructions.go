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

What is slow in a service: profiler_find_calls with service and a duration condition, then explain a few of the slowest calls and compare their hot paths.

Reading the results:
- CPU close to the duration means the call was computing; a large wait means it was blocked on I/O, a lock, or a pool. Queue wait is time queued before the call started.
- GC suspension is time in stop-the-world pauses of the whole JVM, not the call's own code.
- A node's total includes its children; its self time is spent in the method itself. Large self time is where the time went.
- Executions on a fast node under a slow parent mean a loop or an N+1 query.
- A call marked "no tree" was too big for the agent's buffers; it cannot be explained.
- A Partial line means a data source failed and the result may miss calls. Retry before concluding that a call does not exist.

Rules for queries:
- Pass a service or namespace whenever you know it.
- Keep the window at 6 hours or less unless a condition narrows the data: duration of 1s or more, error = true, or a service or pod. A request or trace ID does not narrow the scan, so bracket the time when you know it.
- "the query is too wide to scan" lists the conditions that would make it fit. Add one or shorten the window; the same query fails again.
- "the profiler is busy" means the server is short of read memory. Narrow the query or retry after a minute.

Limits: no thread dumps, heap dumps, or GC logs. Fast successful calls are kept for a short time (two days by default); slow and failed calls are kept longer. Filters apply to root calls only: a method inside a call is found with profiler_explain_call, not with a filter.`
