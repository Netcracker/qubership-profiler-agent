import { describe, expect, it } from 'vitest';

import type { PodEntry } from '../api/types';
import { groupPods } from '../discovery/group-pods';
import type { Condition } from './condition';
import { parseCondition } from './grammar';
import { UNSUPPORTED_REASON, translateConditions, unsupportedReason } from './translate';

const NOW = 1_800_000_000_000;

function entry(namespace: string, service: string, pod: string): PodEntry {
  return { namespace, service, pod, restart_time_ms: NOW - 1000, time_min_ms: NOW - 1000, time_max_ms: NOW };
}

const NAMESPACES = groupPods(
  [
    entry('orders', 'checkout', 'checkout-a'),
    entry('orders', 'checkout', 'checkout-b'),
    entry('orders', 'cart', 'cart-a'),
    entry('payments', 'billing', 'billing-a'),
  ],
  NOW,
);

function conds(...texts: string[]): Condition[] {
  return texts.map((t) => {
    const r = parseCondition(t);
    if (!r.ok) throw new Error(r.error);
    return r.condition;
  });
}

const translate = (...texts: string[]) => translateConditions(conds(...texts), NAMESPACES);

describe('translateConditions', () => {
  it('sends no parameters and skips nothing for no conditions', () => {
    expect(translate()).toEqual({ params: {}, skipped: [], noPods: false, contradiction: false });
  });

  it('expands namespace, service, and pod to pod tuples and intersects them', () => {
    expect(translate('namespace=orders').params?.pods).toEqual(['orders/cart/cart-a', 'orders/checkout/checkout-a', 'orders/checkout/checkout-b']);
    expect(translate('namespace=orders', 'service in checkout,billing').params?.pods).toEqual([
      'orders/checkout/checkout-a',
      'orders/checkout/checkout-b',
    ]);
    expect(translate('service=checkout', 'pod=checkout-b').params?.pods).toEqual(['orders/checkout/checkout-b']);
  });

  it('reports no pods when the identity conditions match none', () => {
    const t = translate('namespace=payments', 'service=checkout');
    expect(t.params?.pods).toEqual([]);
    expect(t.noPods).toBe(true);
  });

  it('waits for the /pods data before it expands identity conditions', () => {
    expect(translateConditions(conds('service=checkout'), null).params).toBeNull();
    expect(translateConditions(conds('duration>1s'), null).params).toEqual({ durationMinMs: 1001 });
  });

  it.each([
    ['duration>100ms', { durationMinMs: 101 }],
    ['duration>=100ms', { durationMinMs: 100 }],
    ['duration<2s', { durationMaxMs: 1999 }],
    ['duration<=2s', { durationMaxMs: 2000 }],
    ['duration between 100ms..2s', { durationMinMs: 100, durationMaxMs: 2000 }],
  ])('sends %s as duration bounds', (text, params) => {
    expect(translate(text).params).toEqual(params);
  });

  it('intersects several duration bounds', () => {
    expect(translate('duration>=100ms', 'duration<=5s', 'duration between 1s..10s').params).toEqual({
      durationMinMs: 1000,
      durationMaxMs: 5000,
    });
    expect(translate('duration>5s', 'duration<1s').contradiction).toBe(true);
  });

  it('skips an upper bound /calls cannot express', () => {
    expect(translate('duration<1ms').skipped).toHaveLength(1);
  });

  it('sends error=true as error_only', () => {
    expect(translate('error=true').params).toEqual({ errorOnly: true });
    expect(translate('error=false').skipped.map((s) => s.condition.values[0])).toEqual(['false']);
  });

  it('sends retention_class and intersects several conditions', () => {
    expect(translate('retention_class in long_clean,any_error').params).toEqual({ retentionClasses: ['long_clean', 'any_error'] });
    expect(translate('retention_class in long_clean,any_error', 'retention_class=any_error').params).toEqual({
      retentionClasses: ['any_error'],
    });
    expect(translate('retention_class=long_clean', 'retention_class=any_error').contradiction).toBe(true);
  });

  it('sends a plain method word as the substring match', () => {
    expect(translate('method=~OrderService\\.place').params).toEqual({ method: 'OrderService.place' });
    expect(translate('method=~Order.*').skipped).toHaveLength(1);
    const two = translate('method=~Order', 'method=~Tax');
    expect(two.params).toEqual({ method: 'Order' });
    expect(two.skipped.map((s) => s.condition.values[0])).toEqual(['Tax']);
  });

  // The grammar rejects these, so they reach the translation only from a caller that builds conditions by hand.
  it.each<[Condition, RegExp]>([
    [{ field: 'pod', op: '=', values: ['Any'] }, /cannot match pod = None or Any/],
    [{ field: 'retention_class', op: 'in', values: ['long_clean', 'hot'] }, /does not support this filter/],
    [{ field: 'method', op: '=~', values: [] }, /needs a value/],
    [{ field: 'duration', op: '>', values: ['fast'] }, /duration bound cannot be sent/],
    [{ field: 'duration', op: 'between', values: ['2s'] }, /duration bound cannot be sent/],
  ])('skips the unchecked condition %j and sends nothing for it', (condition, reason) => {
    const t = translateConditions([condition], NAMESPACES);
    expect(t.params).toEqual({});
    expect(t.skipped).toEqual([{ condition, reason: expect.stringMatching(reason) }]);
  });

  it.each(['pod!=checkout-a', 'service not in cart', 'namespace=~ord', 'method!~health', 'trace_id=abc', 'user.tenant=acme', 'endpoint=None'])(
    'skips %s with a reason',
    (text) => {
      const t = translate(text);
      expect(t.params).toEqual({});
      expect(t.skipped).toHaveLength(1);
      expect(t.skipped[0]!.reason).not.toBe('');
    },
  );
});

describe('translateConditions with the selection of an old link', () => {
  // The old page took the union: every pod of orders/checkout, and payments/billing/billing-a.
  const selection = { services: ['orders/checkout'], pods: ['payments/billing/billing-a'] };

  it('sends the pods of the selected services and the selected pods', () => {
    const t = translateConditions([], NAMESPACES, selection);
    expect([...t.params!.pods!].sort()).toEqual(['orders/checkout/checkout-a', 'orders/checkout/checkout-b', 'payments/billing/billing-a']);
  });

  it('selects across namespaces without the pairs the old link left out', () => {
    const t = translateConditions([], NAMESPACES, { services: ['orders/cart', 'payments/billing'], pods: [] });
    expect([...t.params!.pods!].sort()).toEqual(['orders/cart/cart-a', 'payments/billing/billing-a']);
  });

  it('intersects the selection with the namespace, service, and pod conditions', () => {
    const t = translateConditions(conds('namespace=orders'), NAMESPACES, selection);
    expect([...t.params!.pods!].sort()).toEqual(['orders/checkout/checkout-a', 'orders/checkout/checkout-b']);
    expect(translateConditions(conds('pod=cart-a'), NAMESPACES, selection).noPods).toBe(true);
  });

  it('waits for /pods only when the selection names a service', () => {
    expect(translateConditions([], null, selection).params).toBeNull();
    expect(translateConditions([], null, { services: [], pods: ['a/b/c'] }).params).toEqual({ pods: ['a/b/c'] });
  });
});

describe('unsupportedReason', () => {
  it('enables the fields and operators of the translation table only', () => {
    expect(unsupportedReason('pod')).toBeNull();
    expect(unsupportedReason('pod', 'in')).toBeNull();
    expect(unsupportedReason('pod', '!=')).toBe(UNSUPPORTED_REASON);
    expect(unsupportedReason('method', '=~')).toBeNull();
    expect(unsupportedReason('method', '=')).toBe(UNSUPPORTED_REASON);
    expect(unsupportedReason('trace_id')).toBe(UNSUPPORTED_REASON);
  });

  it.each(['constructor', 'toString', '__proto__'])('disables a call parameter named %s', (field) => {
    expect(unsupportedReason(field)).toBe(UNSUPPORTED_REASON);
    expect(unsupportedReason(field, '=')).toBe(UNSUPPORTED_REASON);
    const t = translate(`${field}=value`);
    expect(t.params).toEqual({});
    expect(t.skipped.map((s) => s.reason)).toEqual([UNSUPPORTED_REASON]);
  });
});
