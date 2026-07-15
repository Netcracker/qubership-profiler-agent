import { getBinary, getJson } from './client';
import { pkToPath } from './pk';
import type { CallPK, CallsResponse, PodsResponse, RetentionClass } from './types';
import { TREE_WIRE_VERSION, decodeTree } from '../msgpack/decode';
import type { TreeWire } from '../msgpack/tree-wire';

/** First-page /calls filter (02 §2.3). Pages 2..N send only the cursor. */
export interface CallsFilter {
  fromMs: number;
  toMs: number;
  /** `namespace/service/pod` tuples. */
  pods?: readonly string[];
  /** Substring match on the method column. */
  method?: string;
  durationMinMs?: number;
  durationMaxMs?: number;
  errorOnly?: boolean;
  retentionClasses?: readonly RetentionClass[];
  limit?: number;
}

export function fetchCallsFirstPage(filter: CallsFilter, signal?: AbortSignal): Promise<CallsResponse> {
  return getJson<CallsResponse>(
    '/api/v1/calls',
    {
      from: filter.fromMs,
      to: filter.toMs,
      pod: filter.pods,
      method: filter.method || undefined,
      duration_min_ms: filter.durationMinMs || undefined,
      duration_max_ms: filter.durationMaxMs || undefined,
      error_only: filter.errorOnly || undefined,
      retention_class: filter.retentionClasses,
      limit: filter.limit,
    },
    signal,
  );
}

/**
 * Follow-up page. The cursor carries the frozen query (02 §2.3.1); re-sending
 * filters alongside it risks a mismatch 400, so only the cursor travels.
 */
export function fetchCallsNextPage(cursor: string, signal?: AbortSignal): Promise<CallsResponse> {
  return getJson<CallsResponse>('/api/v1/calls', { cursor }, signal);
}

export function fetchPods(fromMs: number, toMs: number, signal?: AbortSignal): Promise<PodsResponse> {
  return getJson<PodsResponse>('/api/v1/pods', { from: fromMs, to: toMs }, signal);
}

/**
 * Cold-lookup hints (02 §2.2): a bare PK cannot locate a call outside the hot
 * window, so ts_ms and retention_class from the /calls row ride along on
 * every /tree request.
 */
export interface TreeHints {
  tsMs?: number;
  retentionClass?: RetentionClass;
}

export async function fetchTree(pk: CallPK, hints: TreeHints, signal?: AbortSignal): Promise<TreeWire> {
  const bytes = await getBinary(
    `/api/v1/calls/${encodeURIComponent(pkToPath(pk))}/tree`,
    'application/x-msgpack',
    { ts_ms: hints.tsMs, retention_class: hints.retentionClass },
    signal,
    // Pin the wire version so a future breaking v2 keeps serving v1 to this UI
    // over the compatibility window (02 §2.5.4) instead of breaking it silently.
    { 'Accept-Version': String(TREE_WIRE_VERSION) },
  );
  return decodeTree(bytes);
}
