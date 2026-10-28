import { describe, expect, it } from 'vitest';

import type { PodEntry } from '../api/types';
import { groupPods } from './group-pods';
import { podValueSource } from './pod-values';

const NOW = 1_800_000_000_000;

function entry(namespace: string, service: string, pod: string, restartAgoMs = 1000): PodEntry {
  return { namespace, service, pod, restart_time_ms: NOW - restartAgoMs, time_min_ms: NOW - restartAgoMs, time_max_ms: NOW };
}

describe('podValueSource', () => {
  const source = podValueSource(
    groupPods(
      [
        entry('payments', 'billing', 'billing-b'),
        entry('orders', 'checkout', 'shared-1'),
        entry('payments', 'billing', 'billing-a'),
        // The same pod name in two namespaces and a second restart of one pod are listed once.
        entry('payments', 'checkout', 'shared-1'),
        entry('payments', 'billing', 'billing-a', 5000),
      ],
      NOW,
    ),
  );
  const values = (field: string) => source(field).map((s) => s.value);

  it('lists each namespace, service, and pod once, sorted', () => {
    expect(values('namespace')).toEqual(['orders', 'payments']);
    expect(values('service')).toEqual(['billing', 'checkout']);
    expect(values('pod')).toEqual(['billing-a', 'billing-b', 'shared-1']);
  });

  it('suggests nothing for other fields', () => {
    expect(source('trace_id')).toEqual([]);
  });

  it.each(['constructor', 'toString', '__proto__'])('suggests nothing for a call parameter named %s', (field) => {
    expect(source(field)).toEqual([]);
  });

  it('suggests nothing before the /pods data arrives', () => {
    const empty = podValueSource(null);
    expect(empty('namespace')).toEqual([]);
    expect(empty('pod')).toEqual([]);
  });
});
