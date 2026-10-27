import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { ApiError } from '../api/client';
import { fetchCallsFirstPage, fetchCallsNextPage, fetchConfig } from '../api/endpoints';
import { MOCK_FILTER_CAPABILITIES } from './handlers';
import { server } from './node';

// The mock's /calls takes the `filter` parameter of 02 §2.3.3 and freezes it in the cursor like every other filter.

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const NOW = Date.now();
const WINDOW = { fromMs: NOW - 10 * 60 * 1000, toMs: NOW, limit: 50 };

async function rejection(p: Promise<unknown>): Promise<ApiError> {
  try {
    await p;
  } catch (e) {
    if (e instanceof ApiError) return e;
    throw e;
  }
  throw new Error('expected a rejection');
}

describe('mock /calls filter', () => {
  it('reports the whole filter model in /config', async () => {
    const config = await fetchConfig();
    expect(config.filter_capabilities).toEqual(MOCK_FILTER_CAPABILITIES);
  });

  it('returns only calls that meet every condition', async () => {
    const page = await fetchCallsFirstPage({ ...WINDOW, filters: ['duration>=1s', 'error=false'] });
    expect(page.calls.length).toBeGreaterThan(0);
    expect(page.calls.every((c) => c.duration_ms >= 1000 && !c.error_flag)).toBe(true);
  });

  it('keeps the conditions on later pages', async () => {
    const first = await fetchCallsFirstPage({ ...WINDOW, limit: 5, filters: ['service!=billing'] });
    expect(first.next_cursor).not.toBeNull();
    const next = await fetchCallsNextPage(first.next_cursor!);
    expect([...first.calls, ...next.calls].every((c) => c.pk.pod_service !== 'billing')).toBe(true);
  });

  it('answers invalid_request for a condition that does not parse', async () => {
    const e = await rejection(fetchCallsFirstPage({ ...WINDOW, filters: ['duration=1s'] }));
    expect(e.status).toBe(400);
    expect(e.problem?.code).toBe('invalid_request');
    expect(e.problem?.detail).toContain('"duration=1s"');
  });

  it('answers invalid_request for a regular expression that does not compile', async () => {
    const e = await rejection(fetchCallsFirstPage({ ...WINDOW, filters: ['method=~('] }));
    expect(e.problem?.code).toBe('invalid_request');
  });

  it('counts a namespace condition as narrowing for the wide-query guard', async () => {
    const wide = { fromMs: NOW - 7 * 60 * 60 * 1000, toMs: NOW, limit: 5 };
    const e = await rejection(fetchCallsFirstPage(wide));
    expect(e.problem?.code).toBe('query_too_wide');
    const page = await fetchCallsFirstPage({ ...wide, filters: ['namespace in payments,orders'] });
    expect(page.partial).toBe(false);
  });

  it('still suggests pod filters when the namespace condition does not narrow', async () => {
    const wide = { fromMs: NOW - 7 * 60 * 60 * 1000, toMs: NOW, limit: 5 };
    const e = await rejection(fetchCallsFirstPage({ ...wide, filters: ['namespace!=payments'] }));
    expect(e.problem?.suggested_filters).toContain('pod');
    const e2 = await rejection(fetchCallsFirstPage({ ...wide, filters: ['duration<1s'] }));
    expect(e2.problem?.suggested_filters).toEqual(['pod', 'namespace', 'service', 'retention_class', 'duration_min_ms', 'error_only']);
  });
});
