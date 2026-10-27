import { cleanup, render } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import type { CallJSON } from '../api/types';
import { buildCallColumns, shortTraceId } from './columns';

afterEach(cleanup);

const BASE: CallJSON = {
  pk: {
    pod_namespace: 'synthetic-ns',
    pod_service: 'orders',
    pod_name: 'orders-7f9c-1',
    restart_time_ms: 1_700_000_000_000,
    trace_file_index: 0,
    buffer_offset: 100,
    record_index: 1,
  },
  ts_ms: 1_700_000_100_000,
  duration_ms: 250,
  method: 'void com.example.OrderService.place()',
  thread_name: 'http-nio-8080-exec-1',
  cpu_time_ms: 40,
  wait_time_ms: 0,
  memory_used: 2048,
  queue_wait_ms: 3,
  suspend_ms: 0,
  child_calls: 12,
  transactions: 0,
  logs_generated: 0,
  logs_written: 0,
  file_read: 0,
  file_written: 0,
  net_read: 1024,
  net_written: 512,
  error_flag: false,
  retention_class: 'normal_clean',
  params: {},
  trace_blob_size: null,
  truncated_reason: null,
};

const COLUMNS = new Map(buildCallColumns({}).map((d) => [d.key, d]));

function column(key: string) {
  const def = COLUMNS.get(key);
  if (def === undefined) throw new Error(`no column ${key}`);
  return def;
}

const cellText = (key: string, c: CallJSON): string => render(<>{column(key).render(c)}</>).container.textContent ?? '';

describe('buildCallColumns', () => {
  // Each pair differs in the column's metric only; the second call has the larger value.
  it.each<[string, Partial<CallJSON>, Partial<CallJSON>]>([
    ['start', { ts_ms: 1 }, { ts_ms: 2 }],
    ['duration', { duration_ms: 10 }, { duration_ms: 20 }],
    ['cpu', { cpu_time_ms: 1 }, { cpu_time_ms: 2 }],
    ['queue', { queue_wait_ms: 1 }, { queue_wait_ms: 2 }],
    ['calls', { child_calls: 1 }, { child_calls: 2 }],
    ['net_io', { net_read: 100, net_written: 100 }, { net_read: 0, net_written: 300 }],
    ['suspend', { suspend_ms: 1 }, { suspend_ms: 2 }],
    ['transactions', { transactions: 1 }, { transactions: 2 }],
    ['disk_io', { file_read: 100, file_written: 100 }, { file_read: 300, file_written: 0 }],
    ['memory', { memory_used: 1 }, { memory_used: 2 }],
  ])('sorts the %s column by its value', (key, small, large) => {
    const { compare } = column(key);
    const a = { ...BASE, ...small };
    const b = { ...BASE, ...large };
    expect(compare!(a, b)).toBeLessThan(0);
    expect(compare!(b, a)).toBeGreaterThan(0);
    expect(compare!(a, a)).toBe(0);
  });

  it('sorts the pod column by pod name', () => {
    const a = { ...BASE, pk: { ...BASE.pk, pod_name: 'billing-a' } };
    expect(column('pod').compare!(a, BASE)).toBeLessThan(0);
  });

  it.each<[string, Partial<CallJSON>, string]>([
    ['suspend', { suspend_ms: 1500 }, '1.5s'],
    ['transactions', { transactions: 1234 }, '1,234'],
    ['disk_io', { file_read: 1024, file_written: 1024 }, '2.0 KiB'],
    ['memory', { memory_used: 3 * 1024 * 1024 }, '3.0 MiB'],
    ['net_io', { net_read: 512, net_written: 512 }, '1.0 KiB'],
  ])('renders the hidden-by-default %s column as formatted text', (key, fields, text) => {
    expect(cellText(key, { ...BASE, ...fields })).toBe(text);
  });

  it('hides only the extra metric columns by default', () => {
    const hidden = buildCallColumns({}).filter((d) => d.defaultHidden === true).map((d) => d.key);
    expect(hidden).toEqual(['suspend', 'transactions', 'disk_io', 'memory']);
  });
});

describe('shortTraceId', () => {
  it('cuts an ID longer than 12 characters and keeps a shorter one whole', () => {
    expect(shortTraceId('4bf92f3577b34da6a3ce929d0e0e4736')).toBe('4bf92f3577b3…');
    expect(shortTraceId('a1b2c3d4e5f6')).toBe('a1b2c3d4e5f6');
  });
});
