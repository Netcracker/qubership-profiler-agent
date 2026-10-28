import { describe, expect, it } from 'vitest';

import { convertLegacyParams, readPodSelection, writePodSelection } from './legacy';

const convert = (query: string) => {
  const { conditions, selection } = convertLegacyParams(new URLSearchParams(query));
  expect(selection).toBeNull();
  return conditions;
};
const keep = (query: string) => convertLegacyParams(new URLSearchParams(query));
const NO_MIN = 'duration_min_ms=0';

describe('conditionsFromLegacyParams', () => {
  it('turns a service into namespace and service conditions', () => {
    expect(convert(`service=payments/billing&${NO_MIN}`)).toEqual([
      { field: 'namespace', op: '=', values: ['payments'] },
      { field: 'service', op: '=', values: ['billing'] },
    ]);
  });

  it('turns several services into in conditions', () => {
    expect(convert(`service=payments/billing&service=payments/ledger&${NO_MIN}`)).toEqual([
      { field: 'namespace', op: '=', values: ['payments'] },
      { field: 'service', op: 'in', values: ['billing', 'ledger'] },
    ]);
  });

  it('turns a pod into a pod condition with its namespace and service', () => {
    expect(convert(`pod=payments/billing/billing-7f9c&${NO_MIN}`)).toEqual([
      { field: 'namespace', op: '=', values: ['payments'] },
      { field: 'service', op: '=', values: ['billing'] },
      { field: 'pod', op: '=', values: ['billing-7f9c'] },
    ]);
  });

  it('turns one service in two namespaces into namespace and service conditions', () => {
    expect(convert(`service=prod/orders&service=stage/orders&${NO_MIN}`)).toEqual([
      { field: 'namespace', op: 'in', values: ['prod', 'stage'] },
      { field: 'service', op: '=', values: ['orders'] },
    ]);
  });

  it('drops a pod of a selected service, which the service already holds', () => {
    expect(convert(`service=payments/billing&pod=payments/billing/billing-7f9c&${NO_MIN}`)).toEqual([
      { field: 'namespace', op: '=', values: ['payments'] },
      { field: 'service', op: '=', values: ['billing'] },
    ]);
  });

  it('keeps services from two namespaces as a selection', () => {
    const { conditions, selection } = keep(`service=prod/orders&service=stage/billing&${NO_MIN}`);
    expect(conditions).toEqual([]);
    expect(selection).toEqual({ services: ['prod/orders', 'stage/billing'], pods: [] });
  });

  it('keeps a service and a pod of another service as a selection', () => {
    const { conditions, selection } = keep(`service=prod/orders&pod=prod/billing/billing-0&error_only=true&${NO_MIN}`);
    expect(conditions).toEqual([{ field: 'error', op: '=', values: ['true'] }]);
    expect(selection).toEqual({ services: ['prod/orders'], pods: ['prod/billing/billing-0'] });
  });

  it('keeps pods of two services as a selection, and drops the pods of selected services from it', () => {
    expect(keep(`pod=prod/orders/orders-0&pod=prod/billing/billing-0&${NO_MIN}`).selection).toEqual({
      services: [],
      pods: ['prod/orders/orders-0', 'prod/billing/billing-0'],
    });
    expect(keep(`service=prod/orders&service=stage/billing&pod=prod/orders/orders-0&${NO_MIN}`).selection).toEqual({
      services: ['prod/orders', 'stage/billing'],
      pods: [],
    });
  });

  it('turns the same pod name in two namespaces into conditions', () => {
    expect(convert(`pod=prod/orders/orders-0&pod=stage/orders/orders-0&${NO_MIN}`)).toEqual([
      { field: 'namespace', op: 'in', values: ['prod', 'stage'] },
      { field: 'service', op: '=', values: ['orders'] },
      { field: 'pod', op: '=', values: ['orders-0'] },
    ]);
  });

  it('applies the old 500 ms default when duration_min_ms is absent', () => {
    expect(convert('')).toEqual([{ field: 'duration', op: '>=', values: ['500ms'] }]);
  });

  it.each(['abc', '-5', '1.5', ''])('applies the 500 ms default for duration_min_ms=%j', (raw) => {
    expect(convert(`duration_min_ms=${raw}`)).toEqual([{ field: 'duration', op: '>=', values: ['500ms'] }]);
  });

  it('turns duration_min_ms into a lower bound', () => {
    expect(convert('duration_min_ms=3000')).toEqual([{ field: 'duration', op: '>=', values: ['3s'] }]);
  });

  it('turns duration_max_ms into an upper bound', () => {
    expect(convert('duration_min_ms=0&duration_max_ms=100')).toEqual([{ field: 'duration', op: '<=', values: ['100ms'] }]);
  });

  it('turns both duration bounds into a range', () => {
    expect(convert('duration_min_ms=100&duration_max_ms=2000')).toEqual([
      { field: 'duration', op: 'between', values: ['100ms..2s'] },
    ]);
  });

  it('turns error_only into error=true', () => {
    expect(convert(`error_only=true&${NO_MIN}`)).toEqual([{ field: 'error', op: '=', values: ['true'] }]);
    expect(convert(`error_only=false&${NO_MIN}`)).toEqual([]);
  });

  it('turns retention_class into a retention_class condition and drops unknown classes', () => {
    expect(convert(`retention_class=long_clean&retention_class=any_error&retention_class=hot&${NO_MIN}`)).toEqual([
      { field: 'retention_class', op: 'in', values: ['long_clean', 'any_error'] },
    ]);
  });

  it('turns q into a quoted method match', () => {
    expect(convert(`q=Order.place&${NO_MIN}`)).toEqual([{ field: 'method', op: '=~', values: ['Order\\.place'] }]);
  });

  it('reads and writes a selection, ignoring malformed and repeated entries', () => {
    const sp = new URLSearchParams('service=a/x&service=a/x&service=bad&pod=a/y/1&pod=a/y');
    const selection = readPodSelection(sp);
    expect(selection).toEqual({ services: ['a/x'], pods: ['a/y/1'] });
    expect(readPodSelection(new URLSearchParams('service=bad'))).toBeNull();
    const out = new URLSearchParams();
    writePodSelection(out, selection);
    expect(out.toString()).toBe('service=a%2Fx&pod=a%2Fy%2F1');
    writePodSelection(out, null);
    expect(out.toString()).toBe('service=a%2Fx&pod=a%2Fy%2F1');
  });
});
