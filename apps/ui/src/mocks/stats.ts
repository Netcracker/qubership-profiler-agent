import type { CallJSON, StatsAggregate, StatsResponse, StatsRow } from '../api/types';
import { fieldValues } from './match';

// The mock's GET /stats aggregation (02 §2.9). It sees every call of the window, so it counts exactly: percentiles
// are exact ranks rather than DDSketch estimates, and no grouping is ever approximate.

/** The groupings that are not fields: time buckets, and the names of the call params. */
export const TIME = 'time';
export const PARAM_NAME = 'param_name';

/** The percentile of rank `floor(q · (n − 1))` in ascending order (02 §2.9), or null for no calls. */
export function percentile(sortedAsc: readonly number[], q: number): number | null {
  if (sortedAsc.length === 0) return null;
  return sortedAsc[Math.floor(q * (sortedAsc.length - 1))]!;
}

/** The aggregates of a group of calls. */
export function statsAggregate(calls: readonly CallJSON[]): StatsAggregate {
  const durations = calls.map((c) => c.duration_ms).sort((a, b) => a - b);
  const sum = (f: (c: CallJSON) => number): number => calls.reduce((acc, c) => acc + f(c), 0);
  return {
    count: calls.length,
    errors: calls.filter((c) => c.error_flag).length,
    duration_ms_sum: sum((c) => c.duration_ms),
    p50_ms: percentile(durations, 0.5),
    p95_ms: percentile(durations, 0.95),
    p99_ms: percentile(durations, 0.99),
    cpu_time_ms_sum: sum((c) => c.cpu_time_ms),
    wait_time_ms_sum: sum((c) => c.wait_time_ms),
    queue_wait_ms_sum: sum((c) => c.queue_wait_ms),
    suspend_ms_sum: sum((c) => c.suspend_ms),
  };
}

/** One row over a group of calls. */
export function statsRow(calls: readonly CallJSON[], key: string): StatsRow {
  return { key, ...statsAggregate(calls) };
}

function totals(calls: readonly CallJSON[]): StatsResponse['totals'] {
  return { count: calls.length, errors: calls.filter((c) => c.error_flag).length };
}

const EXACT = { approximate: false, partial: false, partial_reasons: [] };

/**
 * `group_by=time`: `bucket_ms = max(1, ceil((to − from) / buckets))`, every bucket a row in time order, the last one
 * ending at `to` and possibly shorter. The key is the bucket's start in Unix ms.
 */
export function timeStats(calls: readonly CallJSON[], fromMs: number, toMs: number, buckets: number): StatsResponse {
  const bucketMs = Math.max(1, Math.ceil((toMs - fromMs) / buckets));
  const n = Math.ceil((toMs - fromMs) / bucketMs);
  const groups: CallJSON[][] = Array.from({ length: n }, () => []);
  for (const c of calls) groups[Math.floor((c.ts_ms - fromMs) / bucketMs)]!.push(c);
  return {
    group_by: TIME,
    bucket_ms: bucketMs,
    rows: groups.map((g, i) => statsRow(g, String(fromMs + i * bucketMs))),
    other: null,
    distinct: n,
    totals: totals(calls),
    ...EXACT,
  };
}

/** The keys a call falls under: one per distinct value of the field, or one per param name it carries. */
function keysOf(call: CallJSON, groupBy: string): string[] {
  if (groupBy === PARAM_NAME) return Object.keys(call.params).filter((name) => call.params[name]!.some((v) => v !== ''));
  return fieldValues(call, groupBy).filter((v) => v !== '');
}

/**
 * `group_by=<field>`: one row per value, the top `limit` by `order` with ties broken by the key ascending, and the
 * rest in `other`. A call with several values counts once under each, so rows can add up to more than the totals.
 */
export function fieldStats(
  calls: readonly CallJSON[],
  groupBy: string,
  limit: number,
  order: 'count_desc' | 'duration_sum_desc',
): StatsResponse {
  const groups = new Map<string, CallJSON[]>();
  for (const c of calls) {
    for (const key of new Set(keysOf(c, groupBy))) {
      const g = groups.get(key);
      if (g === undefined) groups.set(key, [c]);
      else g.push(c);
    }
  }
  const metric = (row: StatsRow): number => (order === 'count_desc' ? row.count : row.duration_ms_sum);
  const rows = [...groups.entries()].map(([key, g]) => statsRow(g, key));
  rows.sort((a, b) => metric(b) - metric(a) || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  const rest = rows.slice(limit);
  // The omitted groups add up as the backend merges them (libs/query/stats/merge.go), so a call in two of them counts
  // twice, as it does in the rows.
  const restCalls = rest.flatMap((r) => groups.get(r.key)!);
  return {
    group_by: groupBy,
    rows: rows.slice(0, limit),
    other: rest.length === 0 ? null : statsAggregate(restCalls),
    distinct: rows.length,
    totals: totals(calls),
    ...EXACT,
  };
}
