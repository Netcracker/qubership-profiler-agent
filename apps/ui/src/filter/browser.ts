import type { CallJSON } from '../api/types';
import { callTraceId } from '../calls/trace-id';
import { ANY, NONE } from './condition';
import type { Condition, Operator } from './condition';

// Conditions the calls page applies to the loaded rows in the browser, because a backend without the filter model
// cannot filter by them (redesign plan §4.2.1). Such a condition is not sent; it narrows the rows the server
// returned. With the filter model, they go out as `filter` parameters (calls/query-plan.ts).

/** The tooltip of a token that filters the loaded rows only. */
export const BROWSER_TRACE_NOTE =
  "The backend cannot filter by trace_id yet, so this filters the loaded rows in the browser by each call's trace ID.";

/** True for `trace_id = <id>`, the click on a row's trace ID, and for `trace_id = None` or `trace_id = Any`. */
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

/** Whether a trace ID, null for an untraced call, matches the value of `trace_id =`: `None`, `Any`, or an ID. */
function traceMatches(id: string | null, value: string): boolean {
  if (value === NONE) return id === null;
  if (value === ANY) return id !== null;
  return id === value;
}

/** The rows that match every browser-side condition. */
export function filterInBrowser(rows: CallJSON[], conditions: readonly Condition[]): CallJSON[] {
  const values = conditions.filter(isBrowserCondition).map((c) => c.values[0]!);
  if (values.length === 0) return rows;
  return rows.filter((call) => {
    const id = callTraceId(call);
    return values.every((v) => traceMatches(id, v));
  });
}
