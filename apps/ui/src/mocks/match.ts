import type { CallJSON } from '../api/types';
import { parseDurationMs, parseDurationRange } from '../calls/duration-filter';
import { ANY, NONE } from '../filter/condition';
import type { Condition } from '../filter/condition';

// The mock's matcher for the filter conditions of 02 §2.3.3: what each field reads from a call, and how each operator
// compares. The UI parses a condition with its own grammar; this module only decides whether a call matches it.

/** The params that hold a trace ID; their values all count as `trace_id` values (02 §2.3.3). */
export const TRACE_ID_PARAMS = ['trace.id', 'brave.trace_id', 'X-B3-TraceId'];

/** The non-empty values of a param; a param recorded with no value counts as absent. */
function paramValues(call: CallJSON, name: string): string[] {
  // Own keys only: a name such as `constructor` must not read Object.prototype.
  return (Object.hasOwn(call.params, name) ? call.params[name]! : []).filter((v) => v !== '');
}

/** The declaring class of the root method, or null for a name without a `.` (02 §2.3.3). */
export function callClass(method: string): string | null {
  const head = method.split('(')[0]!.trim();
  const qualified = head.split(/\s+/).at(-1) ?? '';
  const dot = qualified.lastIndexOf('.');
  return dot <= 0 ? null : qualified.slice(0, dot);
}

const ID_SEGMENT = /^(\d+|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{16,})$/i;

/** `<web.method> <path>` with ID segments replaced by `{id}`, or null without `web.url` (02 §2.3.3). */
export function callEndpoint(call: CallJSON): string | null {
  const url = paramValues(call, 'web.url')[0];
  if (url === undefined) return null;
  let path: string;
  try {
    path = new URL(url, 'http://host').pathname;
  } catch {
    return null;
  }
  const normalized = path
    .split('/')
    .map((segment) => (ID_SEGMENT.test(segment) ? '{id}' : segment))
    .join('/');
  const method = paramValues(call, 'web.method')[0];
  return method === undefined ? normalized || '/' : `${method} ${normalized || '/'}`;
}

/** The values a call has for a field; an empty list means the call lacks the field. */
export function fieldValues(call: CallJSON, field: string): string[] {
  switch (field) {
    case 'namespace':
      return [call.pk.pod_namespace];
    case 'service':
      return [call.pk.pod_service];
    case 'pod':
      return [call.pk.pod_name];
    case 'method':
      return [call.method];
    case 'class': {
      const c = callClass(call.method);
      return c === null ? [] : [c];
    }
    case 'endpoint': {
      const e = callEndpoint(call);
      return e === null ? [] : [e];
    }
    case 'error':
      return [String(call.error_flag)];
    case 'retention_class':
      return [call.retention_class];
    case 'trace_id':
      return [...new Set(TRACE_ID_PARAMS.flatMap((name) => paramValues(call, name)))];
    case 'request_id':
      return paramValues(call, 'x-request-id');
    default:
      return paramValues(call, field);
  }
}

function matchesDuration(call: CallJSON, c: Condition): boolean {
  const d = call.duration_ms;
  const value = c.values[0]!;
  if (c.op === 'between') {
    const range = parseDurationRange(value)!;
    return d >= range.minMs && d <= range.maxMs;
  }
  const ms = parseDurationMs(value)!;
  if (c.op === '>') return d > ms;
  if (c.op === '>=') return d >= ms;
  if (c.op === '<') return d < ms;
  return d <= ms;
}

/** Whether a call meets a condition. The condition is valid: the handler has parsed and checked it. */
export function matchCondition(call: CallJSON, c: Condition): boolean {
  if (c.field === 'duration') return matchesDuration(call, c);
  const values = fieldValues(call, c.field);
  const value = c.values[0]!;
  if ((c.op === '=' || c.op === '!=') && (value === NONE || value === ANY)) {
    const present = values.length > 0;
    const positive = value === ANY ? present : !present;
    return c.op === '=' ? positive : !positive;
  }
  switch (c.op) {
    case '=':
      return values.includes(value);
    case '!=':
      return !values.includes(value);
    case 'in':
      return values.some((v) => c.values.includes(v));
    case 'not in':
      return !values.some((v) => c.values.includes(v));
    case '=~':
      return values.some((v) => new RegExp(value).test(v));
    case '!~':
      return !values.some((v) => new RegExp(value).test(v));
    default:
      return false;
  }
}
