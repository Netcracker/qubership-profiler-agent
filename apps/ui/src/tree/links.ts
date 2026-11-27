import type { ConfigLink } from '../api/types';
import type { TreeModel, TreeNode } from './model';

// Link-outs from one call to a tracing or logging system (02 §2.8). The
// operator configures URL templates, `/config` serves them, and this module
// fills in the placeholders. The server never sees an expanded URL.

/** The placeholders a template may use (02 §2.8). */
export const LINK_PLACEHOLDERS = ['trace_id', 'from_ms', 'to_ms', 'namespace', 'service', 'pod'] as const;

export type LinkPlaceholder = (typeof LINK_PLACEHOLDERS)[number];

/** What this call has for each placeholder; null when it has no value. */
export type LinkValues = Record<LinkPlaceholder, string | null>;

/** `missing`: the template uses placeholders this call has no value for. */
export type ExpandedLink = { kind: 'ready'; url: string } | { kind: 'missing'; placeholders: LinkPlaceholder[] };

const PLACEHOLDER = /\{([a-z_]+)\}/g;

function isPlaceholder(name: string): name is LinkPlaceholder {
  return (LINK_PLACEHOLDERS as readonly string[]).includes(name);
}

/**
 * Fills a URL template. Each value is percent-encoded the way
 * `encodeURIComponent` does, so a pod name or a trace ID cannot add a query
 * parameter or change the path. Braces that name no known placeholder are part
 * of the URL and are left as they are, as 02 §2.8 requires: a LogQL stream
 * selector such as `{app="x"}` or a regexp escape such as `\x{a0}` keeps them.
 */
export function expandLink(template: string, values: LinkValues): ExpandedLink {
  const missing = new Set<LinkPlaceholder>();
  const url = template.replace(PLACEHOLDER, (whole, name: string) => {
    if (!isPlaceholder(name)) return whole;
    const value = values[name];
    if (value === null) {
      missing.add(name);
      return whole;
    }
    return encodeURIComponent(value);
  });
  return missing.size > 0 ? { kind: 'missing', placeholders: [...missing] } : { kind: 'ready', url };
}

/**
 * How far the link window reaches past each end of the call (02 §2.8). A log
 * pipeline timestamps a line apart from the clock the agent reads, so a window
 * of the call's exact bounds can miss the call's own log lines.
 */
export const LINK_WINDOW_MARGIN_MS = 1000;

/**
 * The `{from_ms}` and `{to_ms}` values of a call that starts at `startMs` and
 * runs for `durationMs`, or nulls where the start is unknown.
 */
export function linkWindow(startMs: number | null, durationMs: number | null): Pick<LinkValues, 'from_ms' | 'to_ms'> {
  return {
    from_ms: startMs === null ? null : String(startMs - LINK_WINDOW_MARGIN_MS),
    to_ms: startMs === null || durationMs === null ? null : String(startMs + durationMs + LINK_WINDOW_MARGIN_MS),
  };
}

/**
 * The characters a trace ID may have to fill `{trace_id}` (02 §2.8). The value
 * comes from a request header of the profiled application, and the system a
 * link opens decodes it again, so a `"` in it could change a query such as
 * `trace_id:"{trace_id}"`. The set admits hexadecimal trace IDs and UUIDs.
 */
const LINKABLE_TRACE_ID = /^[0-9A-Za-z_-]+$/;

/** Why a link is unavailable, for the tooltip of its disabled button. */
export function missingReason(placeholders: readonly LinkPlaceholder[]): string {
  if (placeholders.includes('trace_id')) return 'Unavailable: this call carries no trace ID';
  if (placeholders.includes('from_ms') || placeholders.includes('to_ms')) {
    return 'Unavailable: the start time of this call is unknown';
  }
  return `Unavailable: this call has no value for ${placeholders.map((p) => `{${p}}`).join(', ')}`;
}

/**
 * The param names the agent records a trace ID under, in order of preference:
 * `trace.id` from the Jaeger and OpenTelemetry plugins, `brave.trace_id` from
 * the Brave plugin, and the incoming `X-B3-TraceId` header that the servlet
 * instrumentation records.
 */
export const TRACE_ID_PARAMS = ['trace.id', 'brave.trace_id', 'X-B3-TraceId'] as const;

function traceIdOf(node: TreeNode, keyIdxs: readonly number[]): string | null {
  for (const keyIdx of keyIdxs) {
    const param = node.params.find((p) => p.paramIdx === keyIdx);
    const value = param?.groups.find((g) => g.value !== '::other' && g.value !== '')?.value;
    if (value !== undefined) return value;
  }
  return null;
}

/**
 * The call's trace ID, or null for a call that was not traced. The root's
 * params come first. A tracer that starts its span a few frames down records
 * the ID on that frame, so the search then goes on through the tree, depth
 * first in the order of the `/tree` response, and takes the first ID it meets
 * (02 §2.8). The model sorts children by duration for display, so the search
 * follows the node ids instead: buildTreeModel numbers the nodes in pre-order
 * of the response, and the first node in that order has the smallest id.
 */
export function findTraceId(model: TreeModel): string | null {
  const keyIdxs = TRACE_ID_PARAMS.map((name) => model.paramKeys.indexOf(name)).filter((idx) => idx >= 0);
  if (keyIdxs.length === 0) return null;
  let first: { id: number; traceId: string } | null = null;
  const stack: TreeNode[] = [model.root];
  while (stack.length > 0) {
    const node = stack.pop()!;
    if (first !== null && node.id > first.id) continue;
    const found = traceIdOf(node, keyIdxs);
    if (found !== null) {
      first = { id: node.id, traceId: found };
      continue;
    }
    stack.push(...node.children);
  }
  return first === null ? null : first.traceId;
}

/** A link button of the tree toolbar: where it goes, or why it cannot. */
export interface CallLink {
  label: string;
  kind: ConfigLink['kind'];
  /** null when the link is unavailable; `reason` then says why. */
  url: string | null;
  reason: string | null;
}

/**
 * Expands every configured link for one call, in the operator's order. A trace
 * ID with a character outside `[0-9A-Za-z_-]` counts as missing (02 §2.8), so
 * a link that uses it is disabled.
 */
export function callLinks(links: readonly ConfigLink[], values: LinkValues): CallLink[] {
  const unlinkableTraceId = values.trace_id !== null && !LINKABLE_TRACE_ID.test(values.trace_id);
  const usable = unlinkableTraceId ? { ...values, trace_id: null } : values;
  return links.map((link) => {
    const expanded = expandLink(link.url_template, usable);
    if (expanded.kind === 'missing') {
      const reason =
        unlinkableTraceId && expanded.placeholders.includes('trace_id')
          ? 'Unavailable: the trace ID of this call has characters other than A-Z, a-z, 0-9, _ and -'
          : missingReason(expanded.placeholders);
      return { label: link.label, kind: link.kind, url: null, reason };
    }
    // The server rejects any other scheme at startup (02 §2.8). The check is
    // repeated here because the result becomes an href: a `javascript:` URL
    // from a misbehaving server must not turn into a link.
    if (!/^https?:\/\//i.test(expanded.url)) {
      return { label: link.label, kind: link.kind, url: null, reason: 'Unavailable: the link is not an http or https URL' };
    }
    return { label: link.label, kind: link.kind, url: expanded.url, reason: null };
  });
}
