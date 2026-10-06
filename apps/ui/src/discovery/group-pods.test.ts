import { describe, expect, it } from 'vitest';

import type { PodEntry } from '../api/types';
import { LIVE_THRESHOLD_MS, expandSelection, groupPods } from './group-pods';

const NOW = 1_800_000_000_000;

function entry(namespace: string, service: string, pod: string, restart: number, maxMs: number): PodEntry {
  return {
    namespace,
    service,
    pod,
    restart_time_ms: restart,
    time_min_ms: restart,
    time_max_ms: maxMs,
  };
}

describe('groupPods', () => {
  it('groups the flat list into namespace → service → pod with restart counts', () => {
    const grouped = groupPods(
      [
        entry('orders', 'checkout', 'checkout-a', 1000, NOW - 10_000),
        entry('orders', 'checkout', 'checkout-a', 2000, NOW),
        entry('orders', 'checkout', 'checkout-b', 1500, NOW - LIVE_THRESHOLD_MS * 2),
        entry('payments', 'billing', 'billing-a', 500, NOW),
      ],
      NOW,
    );
    expect(grouped.map((n) => n.namespace)).toEqual(['orders', 'payments']);
    const checkout = grouped[0]!.services[0]!;
    expect(checkout.key).toBe('orders/checkout');
    expect(checkout.restartCount).toBe(3);
    expect(checkout.pods.map((p) => p.pod)).toEqual(['checkout-a', 'checkout-b']);
    // checkout-a's newest restart reaches "now" → live; checkout-b is stale.
    expect(checkout.pods[0]!.live).toBe(true);
    expect(checkout.pods[1]!.live).toBe(false);
    expect(checkout.pods[0]!.restarts.map((r) => r.restart_time_ms)).toEqual([1000, 2000]);
  });
});

describe('expandSelection', () => {
  const grouped = groupPods(
    [
      entry('orders', 'checkout', 'checkout-a', 1000, NOW),
      entry('orders', 'checkout', 'checkout-b', 1000, NOW),
      entry('payments', 'billing', 'billing-a', 1000, NOW),
    ],
    NOW,
  );

  it('expands a selected service into all of its pods', () => {
    expect(expandSelection(grouped, ['orders/checkout'], [])!.sort()).toEqual([
      'orders/checkout/checkout-a',
      'orders/checkout/checkout-b',
    ]);
  });

  it('unions service expansion with individually selected pods', () => {
    expect(expandSelection(grouped, ['orders/checkout'], ['payments/billing/billing-a'])!.sort()).toEqual([
      'orders/checkout/checkout-a',
      'orders/checkout/checkout-b',
      'payments/billing/billing-a',
    ]);
  });

  it('needs no /pods data when only pods are selected', () => {
    expect(expandSelection(null, [], ['a/b/c'])).toEqual(['a/b/c']);
  });

  it('reports "not ready" while services need the missing /pods data', () => {
    expect(expandSelection(null, ['orders/checkout'], [])).toBeNull();
  });
});
