import { describe, expect, it } from 'vitest';

import type { CallJSON } from '../api/types';
import {
  CALLS_ORDERS,
  DEFAULT_ORDER,
  LEGACY_ORDERS,
  ORDER_FIELDS,
  makeOrder,
  orderDirection,
  orderField,
  orderLabel,
  serverCanOrder,
  sortCalls,
} from './sort-order';

/** A call whose every sortable metric is `n`, so each order sorts the same way. */
function call(n: number): CallJSON {
  return {
    pk: {
      pod_namespace: 'ns',
      pod_service: 'svc',
      pod_name: 'pod',
      restart_time_ms: 0,
      trace_file_index: 0,
      buffer_offset: n,
      record_index: n,
    },
    ts_ms: n,
    duration_ms: n,
    method: 'm',
    thread_name: 't',
    cpu_time_ms: n,
    wait_time_ms: 0,
    memory_used: 0,
    queue_wait_ms: n,
    suspend_ms: 0,
    child_calls: n,
    transactions: 0,
    logs_generated: 0,
    logs_written: 0,
    file_read: 0,
    file_written: 0,
    net_read: n,
    net_written: n,
    error_flag: false,
    retention_class: 'normal_clean',
    params: {},
    trace_blob_size: null,
    truncated_reason: null,
  };
}

describe('sort orders', () => {
  it('lists the six fields of the contract in both directions', () => {
    expect(ORDER_FIELDS.map((f) => f.field)).toEqual(['start', 'duration', 'cpu', 'queue', 'calls', 'net']);
    expect(CALLS_ORDERS).toHaveLength(12);
    expect(orderField('duration_asc')).toBe('duration');
    expect(orderDirection('duration_asc')).toBe('asc');
    expect(makeOrder('net', 'desc')).toBe('net_desc');
    expect(orderLabel('queue_desc')).toBe('Queue wait');
  });

  it.each(CALLS_ORDERS)('sorts the loaded rows by %s', (order) => {
    const sorted = sortCalls([call(2), call(3), call(1)], order).map((c) => c.ts_ms);
    expect(sorted).toEqual(orderDirection(order) === 'asc' ? [1, 2, 3] : [3, 2, 1]);
  });

  it('keeps the server order for ties', () => {
    const a = { ...call(1), duration_ms: 5 };
    const b = { ...call(2), duration_ms: 5 };
    expect(sortCalls([b, a], 'duration_desc')).toEqual([b, a]);
  });
});

describe('serverCanOrder', () => {
  it('sends only start time, newest first, to a backend that predates order', () => {
    for (const order of CALLS_ORDERS) expect(serverCanOrder(order, LEGACY_ORDERS)).toBe(order === DEFAULT_ORDER);
  });

  it('takes the set of served orders as a parameter', () => {
    expect(serverCanOrder('duration_desc', new Set(['start_desc', 'duration_desc']))).toBe(true);
    expect(serverCanOrder('duration_asc', new Set(['start_desc', 'duration_desc']))).toBe(false);
  });
});
