import { HttpResponse, delay, http } from 'msw';

import { parsePkPath } from '../api/pk';
import { isRetentionClass } from '../api/types';
import type { ProblemDetails } from '../api/types';
import { encodeTree } from '../msgpack/encode';
import { HOT_WINDOW_MS, callsPage, findCall, podsInRange, treeForCall } from './synthetic';
import type { CallsQueryShape, Position } from './synthetic';

// MSW handlers that mirror the query service's external behaviour: same
// validation order, same RFC 7807 titles and details (api.go, guard.go,
// cursor.go, tree.go). The mock is the contract — when the real service
// disagrees with it, that is a backend bug to report, not a mock to patch.

export const WIDE_RANGE_LIMIT_MS = 6 * 60 * 60 * 1000;
export const CURSOR_TTL_MS = 15 * 60 * 1000;
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

function problem(p: ProblemDetails): Response {
  return HttpResponse.json(p, {
    status: p.status,
    headers: { 'Content-Type': 'application/problem+json' },
  });
}

const badRequest = (detail: string): Response =>
  problem({ type: 'about:blank', title: 'invalid request', status: 400, detail });

// Human pacing in the browser; instant under vitest.
async function pace(): Promise<void> {
  if (import.meta.env.MODE !== 'test') await delay(120 + Math.random() * 200);
}

// --- Window and filter parsing (mirrors model.ParseWindow / ParseCallsQuery) ---

function parseWindow(sp: URLSearchParams): { fromMs: number; toMs: number } | string {
  const fromRaw = sp.get('from');
  const toRaw = sp.get('to');
  if (fromRaw === null || fromRaw === '' || toRaw === null || toRaw === '') {
    return 'from and to are required (Unix ms)';
  }
  const fromMs = Number(fromRaw);
  if (!Number.isSafeInteger(fromMs)) return 'from must be Unix ms';
  const toMs = Number(toRaw);
  if (!Number.isSafeInteger(toMs)) return 'to must be Unix ms';
  if (toMs <= fromMs) return 'to must be greater than from';
  return { fromMs, toMs };
}

function parseCallsQuery(sp: URLSearchParams): CallsQueryShape | string {
  const window = parseWindow(sp);
  if (typeof window === 'string') return window;
  const q: CallsQueryShape = {
    ...window,
    pods: sp.getAll('pod'),
    method: sp.get('method') ?? '',
    durationMinMs: 0,
    durationMaxMs: 0,
    errorOnly: false,
    retentionClasses: sp.getAll('retention_class'),
  };
  for (const p of q.pods) {
    if (p.split('/').length !== 3) return `pod must be <namespace>/<service>/<pod>: ${p}`;
  }
  for (const c of q.retentionClasses) {
    if (!isRetentionClass(c)) return `unknown retention_class: ${c}`;
  }
  for (const name of ['duration_min_ms', 'duration_max_ms'] as const) {
    const raw = sp.get(name);
    if (raw !== null && raw !== '') {
      const v = Number(raw);
      if (!Number.isInteger(v) || v < 0) return `${name} must be a non-negative integer`;
      if (name === 'duration_min_ms') q.durationMinMs = v;
      else q.durationMaxMs = v;
    }
  }
  const errorOnly = sp.get('error_only');
  if (errorOnly !== null && errorOnly !== '') {
    if (errorOnly !== 'true' && errorOnly !== 'false') return 'error_only must be a boolean';
    q.errorOnly = errorOnly === 'true';
  }
  return q;
}

function parseLimit(sp: URLSearchParams): number | string {
  const raw = sp.get('limit');
  if (raw === null || raw === '') return DEFAULT_LIMIT;
  const v = Number(raw);
  if (!Number.isInteger(v) || v <= 0) return 'limit must be a positive integer';
  if (v > MAX_LIMIT) return `limit must not exceed ${MAX_LIMIT}`;
  return v;
}

// --- Wide-query guard, span layer (guard.go) ---

function hasNarrowingFilter(q: CallsQueryShape): boolean {
  return q.pods.length > 0 || q.retentionClasses.length > 0 || q.durationMinMs > 0 || q.errorOnly;
}

function suggestedFilters(q: CallsQueryShape): string[] {
  const out: string[] = [];
  if (q.pods.length === 0) out.push('pod');
  if (q.retentionClasses.length === 0) out.push('retention_class');
  if (q.durationMinMs <= 0) out.push('duration_min_ms');
  if (!q.errorOnly) out.push('error_only');
  return out;
}

function guardSpan(q: CallsQueryShape): Response | null {
  const spanMs = q.toMs - q.fromMs;
  if (spanMs <= WIDE_RANGE_LIMIT_MS || hasNarrowingFilter(q)) return null;
  return problem({
    type: 'about:blank',
    title: 'query too wide',
    status: 400,
    detail: `time span ${formatGoDuration(spanMs)} exceeds PROFILER_WIDE_RANGE_LIMIT 6h0m0s and no file-pruning filter is present`,
    suggested_filters: suggestedFilters(q),
  });
}

/** Rough Go time.Duration format: "26h0m0s". */
function formatGoDuration(ms: number): string {
  const h = Math.floor(ms / 3600000);
  const m = Math.floor((ms % 3600000) / 60000);
  const s = Math.floor((ms % 60000) / 1000);
  return `${h}h${m}m${s}s`;
}

// --- Cursor (cursor.go): opaque URL-safe base64 of the frozen query + position ---

interface CursorToken {
  v: number;
  q: CallsQueryShape;
  pos: Position;
  iat: number;
}

function base64UrlEncode(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function base64UrlDecode(s: string): string {
  const padded = s.replace(/-/g, '+').replace(/_/g, '/');
  const bin = atob(padded);
  const bytes = Uint8Array.from(bin, (ch) => ch.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

/** Exported so tests can mint aged or malformed cursors. */
export function encodeMockCursor(token: CursorToken): string {
  return base64UrlEncode(JSON.stringify(token));
}

function decodeMockCursor(raw: string, nowMs: number): CursorToken | string {
  let json: string;
  try {
    json = base64UrlDecode(raw);
  } catch {
    return 'cursor is not URL-safe base64';
  }
  let token: CursorToken;
  try {
    token = JSON.parse(json) as CursorToken;
  } catch {
    return 'cursor does not decode';
  }
  if (token.v !== 1) return `cursor version ${token.v} is not supported`;
  const age = nowMs - token.iat;
  if (age > CURSOR_TTL_MS) {
    return `cursor expired: issued ${Math.round(age / 1000)}s ago, TTL 15m0s`;
  }
  return token;
}

/** Mirrors frozenQueryMismatch: re-sent filters must equal the frozen query. */
function frozenQueryMismatch(frozen: CallsQueryShape, sp: URLSearchParams): string {
  const resent: CallsQueryShape = { ...frozen };
  if (sp.get('from') !== null) resent.fromMs = Number(sp.get('from'));
  if (sp.get('to') !== null) resent.toMs = Number(sp.get('to'));
  if (sp.getAll('pod').length > 0) resent.pods = sp.getAll('pod');
  if (sp.get('method') !== null) resent.method = sp.get('method') ?? '';
  if (sp.getAll('retention_class').length > 0) resent.retentionClasses = sp.getAll('retention_class');
  if (sp.get('duration_min_ms') !== null) resent.durationMinMs = Number(sp.get('duration_min_ms'));
  if (sp.get('duration_max_ms') !== null) resent.durationMaxMs = Number(sp.get('duration_max_ms'));
  if (sp.get('error_only') !== null) resent.errorOnly = sp.get('error_only') === 'true';
  if (JSON.stringify(resent) !== JSON.stringify(frozen)) {
    return 're-sent filters do not match the query frozen in the cursor; restart from page 1';
  }
  return '';
}

// --- Handlers ---

export const handlers = [
  http.get('/api/v1/config', async () => {
    await pace();
    return HttpResponse.json({ dumps_collector_url: 'https://dumps-collector-petclinic.example.com' });
  }),

  http.get('/api/v1/pods', async ({ request }) => {
    await pace();
    const sp = new URL(request.url).searchParams;
    const window = parseWindow(sp);
    if (typeof window === 'string') return badRequest(window);
    return HttpResponse.json({
      pods: podsInRange(window.fromMs, window.toMs, Date.now()),
      partial: false,
      partial_reasons: [],
    });
  }),

  http.get('/api/v1/calls', async ({ request }) => {
    await pace();
    const sp = new URL(request.url).searchParams;
    const nowMs = Date.now();

    let q: CallsQueryShape;
    let after: Position | null = null;
    const cursorRaw = sp.get('cursor');
    if (cursorRaw === null || cursorRaw === '') {
      const parsed = parseCallsQuery(sp);
      if (typeof parsed === 'string') return badRequest(parsed);
      q = parsed;
      const rejection = guardSpan(q);
      if (rejection !== null) return rejection;
    } else {
      const token = decodeMockCursor(cursorRaw, nowMs);
      if (typeof token === 'string') return badRequest(token);
      const mismatch = frozenQueryMismatch(token.q, sp);
      if (mismatch !== '') return badRequest(mismatch);
      q = token.q;
      after = token.pos;
    }

    const limit = parseLimit(sp);
    if (typeof limit === 'string') return badRequest(limit);

    const { calls, nextPos } = callsPage(q, after, limit, nowMs);
    return HttpResponse.json({
      calls,
      next_cursor: nextPos === null ? null : encodeMockCursor({ v: 1, q, pos: nextPos, iat: nowMs }),
      partial: false,
      partial_reasons: [],
    });
  }),

  http.get('/api/v1/calls/:pk/tree', async ({ request, params }) => {
    await pace();
    const sp = new URL(request.url).searchParams;
    let pk;
    try {
      pk = parsePkPath(decodeURIComponent(String(params['pk'])));
    } catch (e) {
      return badRequest(e instanceof Error ? e.message : String(e));
    }
    const tsRaw = sp.get('ts_ms');
    if (tsRaw !== null && tsRaw !== '' && !Number.isSafeInteger(Number(tsRaw))) {
      return badRequest('ts_ms must be Unix ms');
    }
    for (const c of sp.getAll('retention_class')) {
      if (!isRetentionClass(c)) return badRequest(`unknown retention_class: ${c}`);
    }
    const hasTs = tsRaw !== null && tsRaw !== '';
    const nowMs = Date.now();
    const pkPath = String(params['pk']);

    const tsMs = pk.restart_time_ms + pk.buffer_offset;
    const isHot = nowMs - tsMs <= HOT_WINDOW_MS;
    const call = findCall(pk, nowMs);
    if (call === null || (!isHot && !hasTs)) {
      // Mirrors pointProblem in tree.go: an honest 404 whose detail explains
      // the §2.2 hint when it could have changed the answer.
      let detail = `no tier holds call ${decodeURIComponent(pkPath)}`;
      if (!hasTs) {
        detail += '; a call outside the hot window needs the ts_ms (and retention_class) hints from its /calls row (02 §2.2)';
      }
      return problem({ type: 'about:blank', title: 'call not found', status: 404, detail });
    }
    if (call.truncated_reason !== null) {
      return problem({
        type: 'about:blank',
        title: 'trace blob unavailable',
        status: 404,
        detail: `the blob of ${decodeURIComponent(pkPath)} was dropped at seal: truncated_reason = ${call.truncated_reason}`,
      });
    }
    const body = encodeTree(treeForCall(call));
    return new HttpResponse(body.buffer as ArrayBuffer, {
      headers: {
        'Content-Type': 'application/x-msgpack',
        'Cache-Control': 'public, max-age=31536000, immutable',
      },
    });
  }),
];
