import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { ApiError } from '../api/client';
import { fetchCallsFirstPage, fetchCallsNextPage } from '../api/endpoints';
import type { CallJSON } from '../api/types';
import { server } from './node';

// The mock sorts /calls by every `order` of 02 §2.3.5, with the keyset that pages each order.

beforeAll(() => server.listen({ onUnhandledFrame: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const NOW = Date.now();
const WINDOW = { fromMs: NOW - 10 * 60 * 1000, toMs: NOW, limit: 50 };

function expectSorted(rows: readonly CallJSON[], key: (c: CallJSON) => number, asc: boolean): void {
  for (let i = 1; i < rows.length; i++) {
    const [a, b] = [key(rows[i - 1]!), key(rows[i]!)];
    if (asc) expect(a).toBeLessThanOrEqual(b);
    else expect(a).toBeGreaterThanOrEqual(b);
  }
}

describe('mock /calls order', () => {
  it('sorts by duration, longest first, across pages', async () => {
    const first = await fetchCallsFirstPage({ ...WINDOW, order: 'duration_desc' });
    expect(first.calls).toHaveLength(50);
    expect(first.next_cursor).not.toBeNull();
    const second = await fetchCallsNextPage(first.next_cursor!);
    const rows = [...first.calls, ...second.calls];
    expectSorted(rows, (c) => c.duration_ms, false);
    expect(new Set(rows.map((c) => JSON.stringify(c.pk))).size).toBe(rows.length);
  });

  it.each([
    ['start_asc', (c: CallJSON) => c.ts_ms, true],
    ['cpu_desc', (c: CallJSON) => c.cpu_time_ms, false],
    ['queue_asc', (c: CallJSON) => c.queue_wait_ms, true],
    ['calls_desc', (c: CallJSON) => c.child_calls, false],
    ['net_asc', (c: CallJSON) => c.net_read + c.net_written, true],
  ] as const)('sorts by %s', async (order, key, asc) => {
    const page = await fetchCallsFirstPage({ ...WINDOW, order });
    expect(page.calls.length).toBeGreaterThan(1);
    expectSorted(page.calls, key, asc);
  });

  it('rejects an order outside the contract', async () => {
    await expect(fetchCallsFirstPage({ ...WINDOW, order: 'pod_desc' })).rejects.toBeInstanceOf(ApiError);
  });
});
