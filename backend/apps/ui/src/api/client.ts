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

/** Wide-query guard rejection (02 §2.3.2); api.go titles it "query too wide". */
export function isWideQueryRejection(e: unknown): e is ApiError {
  return e instanceof ApiError && e.status === 400 && e.problem?.title === 'query too wide';
}

/**
 * Cursor rejection — expired, malformed, or frozen-query mismatch (02 §2.3.1).
 * The backend reports every cursor failure as a 400 whose detail names the
 * cursor (decodeCursor / frozenQueryMismatch in backend/libs/query); the
 * client's reaction is the same for all of them: restart from page one.
 */
export function isCursorRejection(e: unknown): e is ApiError {
  return (
    e instanceof ApiError &&
    e.status === 400 &&
    (e.problem?.detail ?? '').toLowerCase().includes('cursor')
  );
}

export type QueryParamValue = string | number | boolean | undefined;
export type QueryParams = Record<string, QueryParamValue | readonly (string | number)[]>;

function buildUrl(path: string, params?: QueryParams): string {
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
): Promise<Uint8Array> {
  const res = await fetch(buildUrl(path, params), {
    headers: { Accept: accept },
    signal: signal ?? null,
  });
  if (!res.ok) throw await toApiError(res);
  return new Uint8Array(await res.arrayBuffer());
}
