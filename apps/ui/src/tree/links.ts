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
 * parameter or change the path. Braces that name no known placeholder are left
 * as they are: the server rejects such a template at startup, so one that gets
 * here comes from a newer server, and guessing at it would be worse.
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
 * first, and takes the first ID it meets.
 */
export function findTraceId(model: TreeModel): string | null {
  const keyIdxs = TRACE_ID_PARAMS.map((name) => model.paramKeys.indexOf(name)).filter((idx) => idx >= 0);
  if (keyIdxs.length === 0) return null;
  const stack: TreeNode[] = [model.root];
  while (stack.length > 0) {
    const node = stack.pop()!;
    const found = traceIdOf(node, keyIdxs);
    if (found !== null) return found;
    for (let i = node.children.length - 1; i >= 0; i--) stack.push(node.children[i]!);
  }
  return null;
}

/** A link button of the tree toolbar: where it goes, or why it cannot. */
export interface CallLink {
  label: string;
  kind: ConfigLink['kind'];
  /** null when the link is unavailable; `reason` then says why. */
  url: string | null;
  reason: string | null;
}

/** Expands every configured link for one call, in the operator's order. */
export function callLinks(links: readonly ConfigLink[], values: LinkValues): CallLink[] {
  return links.map((link) => {
    const expanded = expandLink(link.url_template, values);
    if (expanded.kind === 'missing') {
      return { label: link.label, kind: link.kind, url: null, reason: missingReason(expanded.placeholders) };
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
