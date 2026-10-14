import type { CallJSON } from '../api/types';
import { TRACE_ID_PARAMS } from '../tree/links';

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
