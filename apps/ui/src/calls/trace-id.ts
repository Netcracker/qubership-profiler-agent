import type { CallJSON } from '../api/types';

/**
 * The param names the agent records a trace ID under, in order of preference:
 * `trace.id` from the Jaeger and OpenTelemetry plugins, `brave.trace_id` from
 * the Brave plugin, and the incoming `X-B3-TraceId` header that the servlet
 * instrumentation records.
 */
const TRACE_ID_PARAMS = ['trace.id', 'brave.trace_id', 'X-B3-TraceId'] as const;

/**
 * The trace ID of a calls row, or null for "not traced". A row carries no `trace_id` member (02 §2.3); the ID comes
 * from the call's own params, under the names the tracing plugins record it with. A tracer that records the ID on a
 * frame below the call's root leaves the row without it, although the call tree still finds it (`findTraceId`).
 */
export function callTraceId(call: CallJSON): string | null {
  for (const name of TRACE_ID_PARAMS) {
    const value = call.params[name]?.find((v) => v !== '');
    if (value !== undefined) return value;
  }
  return null;
}
