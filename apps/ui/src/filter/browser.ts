import type { CallJSON } from '../api/types';
import { callTraceId } from '../calls/trace-id';
import type { Condition, Operator } from './condition';

// Conditions the calls page applies to the loaded rows in the browser, because /calls cannot filter by them yet
// (redesign plan §4.2.1). Such a condition is not sent; it narrows the rows the server returned. Task 3.6 sends them
// as `filter` parameters instead and deletes this module.

/** The tooltip of a token that filters the loaded rows only. */
export const BROWSER_TRACE_NOTE =
  "The backend cannot filter by trace_id yet, so this filters the loaded rows in the browser by each call's trace ID.";

/** True for `trace_id = <id>`, the click on a row's trace ID. */
export function isBrowserCondition(c: Condition): boolean {
  return c.field === 'trace_id' && c.op === '=' && c.values.length === 1;
}

/** Whether the filter bar may offer a field or an operator because the browser applies it. */
export function browserSupports(field: string, op?: Operator): boolean {
  return field === 'trace_id' && (op === undefined || op === '=');
}

/** The tooltip of a browser-side token, or null for a condition the server applies or skips. */
export function browserNote(c: Condition): string | null {
  return isBrowserCondition(c) ? BROWSER_TRACE_NOTE : null;
}

/** The rows that match every browser-side condition. */
export function filterInBrowser(rows: CallJSON[], conditions: readonly Condition[]): CallJSON[] {
  const traceIds = conditions.filter(isBrowserCondition).map((c) => c.values[0]!);
  if (traceIds.length === 0) return rows;
  return rows.filter((call) => {
    const id = callTraceId(call);
    return id !== null && traceIds.every((t) => t === id);
  });
}
