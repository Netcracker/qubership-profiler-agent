import type { ProblemDetails } from './types';

// Thin typed fetch over /api/v1. Chosen over RTK Query deliberately:
// - data loads only on an explicit Apply (09 §2.2), so a declarative cache
//   layer has nothing to manage;
// - the keyset cursor freezes the query server-side (02 §2.3.1) — page
//   accumulation and TTL restart are custom logic under either client;
// - /tree is binary MessagePack with immutable HTTP caching, which the
//   browser cache already handles;
// - the bundle ships inside the query binary (07 §6), so fewer runtime
//   dependencies matter.

/** Non-2xx response, carrying the RFC 7807 body when one was parseable. */
export class ApiError extends Error {
  readonly status: number;
  readonly problem: ProblemDetails | null;

  constructor(status: number, problem: ProblemDetails | null, fallback: string) {
    super(problem?.detail ?? problem?.title ?? fallback);
    this.name = 'ApiError';
    this.status = status;
    this.problem = problem;
  }
}

// Classify errors by the problem `code` (02 §8). A build older than the code member sends none; for it, the helpers
// fall back on the `title` and `detail` prose that build wrote.

/** True when the problem carries `code`, or carries no code and `legacy` matches the older build's prose. */
function hasCode(e: unknown, status: number, code: string, legacy: (p: ProblemDetails) => boolean): e is ApiError {
  if (!(e instanceof ApiError) || e.status !== status || e.problem === null) return false;
  return e.problem.code !== undefined ? e.problem.code === code : legacy(e.problem);
}

/** Wide-query guard rejection (02 §2.3.2). */
export function isWideQueryRejection(e: unknown): e is ApiError {
  return hasCode(e, 400, 'query_too_wide', (p) => p.title === 'query too wide');
}

/**
 * Cursor rejection: expired, malformed, or contradicted by re-sent filters (02 §2.3.1). The client restarts from page
 * one for all of them.
 */
export function isCursorRejection(e: unknown): e is ApiError {
  return hasCode(e, 400, 'cursor_rejected', (p) => (p.detail ?? '').toLowerCase().includes('cursor'));
}

/**
 * A request the backend rejected as malformed (02 §8), such as a filter it does not implement (02 §2.3.4). Retrying
 * the same request gets the same answer.
 */
export function isInvalidRequest(e: unknown): e is ApiError {
  return hasCode(e, 400, 'invalid_request', (p) => p.title === 'invalid request');
}

export type QueryParamValue = string | number | boolean | undefined;
export type QueryParams = Record<string, QueryParamValue | readonly (string | number)[]>;

/** Exported so callers can size a request before sending it (PR 708 review #8). */
export function buildUrl(path: string, params?: QueryParams): string {
  const sp = new URLSearchParams();
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value === undefined) continue;
    if (Array.isArray(value)) {
      for (const v of value) sp.append(key, String(v));
    } else {
      sp.set(key, String(value));
    }
  }
  const qs = sp.toString();
  return qs === '' ? path : `${path}?${qs}`;
}

async function toApiError(res: Response): Promise<ApiError> {
  let problem: ProblemDetails | null = null;
  const contentType = res.headers.get('content-type') ?? '';
  if (contentType.includes('json')) {
    try {
      problem = (await res.json()) as ProblemDetails;
    } catch {
      problem = null;
    }
  }
  return new ApiError(res.status, problem, `${res.status} ${res.statusText}`);
}

export async function getJson<T>(path: string, params?: QueryParams, signal?: AbortSignal): Promise<T> {
  const res = await fetch(buildUrl(path, params), {
    headers: { Accept: 'application/json' },
    signal: signal ?? null,
  });
  if (!res.ok) throw await toApiError(res);
  return (await res.json()) as T;
}

export async function getBinary(
  path: string,
  accept: string,
  params?: QueryParams,
  signal?: AbortSignal,
  headers?: Record<string, string>,
): Promise<Uint8Array> {
  const res = await fetch(buildUrl(path, params), {
    headers: { Accept: accept, ...headers },
    signal: signal ?? null,
  });
  if (!res.ok) throw await toApiError(res);
  return new Uint8Array(await res.arrayBuffer());
}
