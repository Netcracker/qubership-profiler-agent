import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { ApiError } from '../api/client';
import { fetchCallsFirstPage, fetchCallsNextPage, fetchStats } from '../api/endpoints';
import type { CallJSON } from '../api/types';
import { server } from './node';
import { fieldStats, percentile, statsRow, timeStats } from './stats';

// The mock's /stats (02 §2.9): time buckets, field groupings, totals, and the parameter checks.

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const NOW = Date.now();
const WINDOW = { fromMs: NOW - 10 * 60 * 1000, toMs: NOW };

function call(tsMs: number, durationMs: number, over: Partial<CallJSON> = {}): CallJSON {
  return {
    pk: { pod_namespace: 'ns', pod_service: 'svc', pod_name: 'pod', restart_time_ms: 0, trace_file_index: 0, buffer_offset: tsMs, record_index: 0 },
    ts_ms: tsMs,
    duration_ms: durationMs,
    method: 'void a.B.c()',
    thread_name: 't',
    cpu_time_ms: 1,
    wait_time_ms: 2,
    memory_used: 0,
    queue_wait_ms: 3,
    suspend_ms: 4,
    child_calls: 0,
    transactions: 0,
    logs_generated: 0,
    logs_written: 0,
    file_read: 0,
    file_written: 0,
    net_read: 0,
    net_written: 0,
    error_flag: false,
    retention_class: 'normal_clean',
    params: {},
    trace_blob_size: 0,
    truncated_reason: null,
    ...over,
  };
}

async function rejection(p: Promise<unknown>): Promise<ApiError> {
  try {
    await p;
  } catch (e) {
    if (e instanceof ApiError) return e;
    throw e;
  }
  throw new Error('expected a rejection');
}

/** Every call of the query, through /calls paging. */
async function allCalls(filters: string[]): Promise<CallJSON[]> {
  let page = await fetchCallsFirstPage({ ...WINDOW, filters, limit: 1000 });
  const out = [...page.calls];
  while (page.next_cursor !== null) {
    page = await fetchCallsNextPage(page.next_cursor);
    out.push(...page.calls);
  }
  return out;
}

describe('percentile', () => {
  it('takes the rank floor(q · (n − 1)) in ascending order', () => {
    expect(percentile([10, 20, 30, 40], 0.5)).toBe(20);
    expect(percentile([10, 20, 30, 40], 0.99)).toBe(30);
    expect(percentile([7], 0.95)).toBe(7);
    expect(percentile([], 0.5)).toBeNull();
  });
});

describe('timeStats', () => {
  it('puts a call in the bucket that holds its start, with every bucket a row', () => {
    const calls = [call(1000, 5), call(1999, 5), call(2000, 5, { error_flag: true })];
    const s = timeStats(calls, 1000, 4000, 3);
    expect(s.bucket_ms).toBe(1000);
    expect(s.rows.map((r) => [r.key, r.count, r.errors])).toEqual([
      ['1000', 2, 0],
      ['2000', 1, 1],
      ['3000', 0, 0],
    ]);
    expect(s.rows[2]!.p50_ms).toBeNull();
    expect(s.distinct).toBe(3);
    expect(s.totals).toEqual({ count: 3, errors: 1 });
  });

  it('makes the last bucket shorter when the window does not divide evenly', () => {
    const s = timeStats([call(1000 + 9, 1)], 1000, 1010, 3);
    expect(s.bucket_ms).toBe(4);
    expect(s.rows.map((r) => r.key)).toEqual(['1000', '1004', '1008']);
    expect(s.rows[2]!.count).toBe(1);
  });
});

describe('fieldStats', () => {
  const calls = [
    call(1, 100, { params: { tier: ['gold'] } }),
    call(2, 300, { params: { tier: ['gold', 'silver'] } }),
    call(3, 900, { params: { tier: ['bronze'] } }),
    call(4, 50),
  ];

  it('ranks by count, breaks ties by key, and puts the rest in other', () => {
    const s = fieldStats(calls, 'tier', 2, 'count_desc');
    expect(s.rows.map((r) => [r.key, r.count])).toEqual([
      ['gold', 2],
      ['bronze', 1],
    ]);
    expect(s.other?.count).toBe(1);
    expect(s.distinct).toBe(3);
    // A call without the param counts in the totals only.
    expect(s.totals.count).toBe(4);
  });

  it('adds up the omitted groups, so a call in two of them counts twice in other', () => {
    const multi = [
      call(1, 100, { params: { tag: ['a'] } }),
      call(2, 100, { params: { tag: ['a'] } }),
      call(3, 10, { params: { tag: ['b', 'c'] } }),
    ];
    const s = fieldStats(multi, 'tag', 1, 'count_desc');
    expect(s.rows.map((r) => r.key)).toEqual(['a']);
    expect(s.other).toMatchObject({ count: 2, duration_ms_sum: 20 });
  });

  it('ranks by the duration sum', () => {
    const s = fieldStats(calls, 'tier', 1, 'duration_sum_desc');
    expect(s.rows.map((r) => [r.key, r.duration_ms_sum])).toEqual([['bronze', 900]]);
  });

  it('counts the calls that carry each param name', () => {
    const s = fieldStats(calls, 'param_name', 20, 'count_desc');
    expect(s.rows).toEqual([expect.objectContaining({ key: 'tier', count: 3 })]);
    expect(s.other).toBeNull();
  });

  it('sums the metrics of a row', () => {
    expect(statsRow(calls.slice(0, 2), 'k')).toEqual(
      expect.objectContaining({ key: 'k', count: 2, duration_ms_sum: 400, cpu_time_ms_sum: 2, suspend_ms_sum: 8 }),
    );
  });
});

describe('mock /stats', () => {
  it('counts the same calls as /calls for the same query', async () => {
    const filters = ['duration>=1s'];
    const [stats, calls] = await Promise.all([fetchStats({ ...WINDOW, filters, groupBy: 'time' }), allCalls(filters)]);
    expect(stats.rows).toHaveLength(60);
    expect(stats.totals.count).toBe(calls.length);
    expect(stats.rows.reduce((sum, r) => sum + r.count, 0)).toBe(calls.length);
    expect(stats.totals.errors).toBe(calls.filter((c) => c.error_flag).length);
  });

  it('groups by a field with a limit', async () => {
    const stats = await fetchStats({ ...WINDOW, groupBy: 'service', limit: 2 });
    expect(stats.rows.length).toBeLessThanOrEqual(2);
    expect(stats.distinct).toBeGreaterThanOrEqual(stats.rows.length);
  });

  it.each([
    [{ groupBy: '' }, 'group_by is required'],
    [{ groupBy: 'duration' }, 'unknown group_by'],
    [{ groupBy: 'time', limit: 5 }, 'limit does not apply'],
    [{ groupBy: 'service', buckets: 5 }, 'buckets does not apply'],
    [{ groupBy: 'time', buckets: 1001 }, 'buckets must be an integer'],
  ])('answers invalid_request for %o', async (q, detail) => {
    const e = await rejection(fetchStats({ ...WINDOW, ...q }));
    expect(e.problem?.code).toBe('invalid_request');
    expect(e.problem?.detail).toContain(detail);
  });

  it('answers the wide-query guard like /calls', async () => {
    const e = await rejection(fetchStats({ fromMs: NOW - 7 * 60 * 60 * 1000, toMs: NOW, groupBy: 'time' }));
    expect(e.problem?.code).toBe('query_too_wide');
  });
});
