import { describe, expect, it } from 'vitest';

import { groupPods } from '../discovery/group-pods';
import { capabilitiesOf } from '../filter/capabilities';
import type { Condition } from '../filter/condition';
import { MOCK_FILTER_CAPABILITIES } from '../mocks/handlers';
import { planQuery } from './query-plan';

const FULL = capabilitiesOf(MOCK_FILTER_CAPABILITIES);
const STAGE_A = capabilitiesOf({ ...MOCK_FILTER_CAPABILITIES, fields: { service: ['='] }, special_values: [], orders: ['start_desc'] });

const service: Condition = { field: 'service', op: '=', values: ['billing'] };
const notService: Condition = { field: 'service', op: '!=', values: ['billing'] };
const trace: Condition = { field: 'trace_id', op: '=', values: ['abc'] };

const NAMESPACES = groupPods([
  { namespace: 'payments', service: 'billing', pod: 'billing-1', restart_time_ms: 1, time_min_ms: 0, time_max_ms: 0 },
], 0);

describe('planQuery', () => {
  it('holds the request back while /config loads', () => {
    const plan = planQuery([service], 'start_desc', undefined, null);
    expect(plan.params).toBeNull();
    expect(plan.waitingForPods).toBe(false);
  });

  it('sends every condition as a filter with the filter model', () => {
    const plan = planQuery([service, notService, trace], 'cpu_desc', FULL, null);
    expect(plan.params).toEqual({ filters: ['service=billing', 'service!=billing', 'trace_id=abc'], order: 'cpu_desc' });
    expect(plan.skipped).toEqual([]);
    expect(plan.browser).toEqual([]);
    expect(plan.serverOrdered).toBe(true);
    expect(plan.expandsPods).toBe(false);
  });

  it('leaves out the default order and an order the build lacks', () => {
    expect(planQuery([], 'start_desc', FULL, null).params).toEqual({ filters: [] });
    const plan = planQuery([], 'cpu_desc', STAGE_A, null);
    expect(plan.params).toEqual({ filters: [] });
    expect(plan.serverOrdered).toBe(false);
  });

  it('skips a condition the build lacks, with the reason', () => {
    const plan = planQuery([service, notService], 'start_desc', STAGE_A, null);
    expect(plan.params).toEqual({ filters: ['service=billing'] });
    expect(plan.skipped).toEqual([{ condition: notService, reason: 'This backend does not support != on service.' }]);
  });

  it('translates to pod tuples without the filter model', () => {
    const plan = planQuery([service, trace], 'start_desc', null, NAMESPACES);
    expect(plan.params).toEqual({ pods: ['payments/billing/billing-1'] });
    expect(plan.browser).toEqual([trace]);
    expect(plan.skipped).toEqual([]);
    expect(plan.expandsPods).toBe(true);
    expect(plan.serverOrdered).toBe(true);
  });

  it('waits for /pods without the filter model', () => {
    const plan = planQuery([service], 'start_desc', null, null);
    expect(plan.params).toBeNull();
    expect(plan.waitingForPods).toBe(true);
  });

  it('names why no call can match without the filter model', () => {
    const other: Condition = { field: 'service', op: '=', values: ['orders'] };
    expect(planQuery([other], 'start_desc', null, NAMESPACES).noMatch).toBe(
      'No pod in this window matches the namespace, service, and pod filters.',
    );
    const apart: Condition[] = [
      { field: 'duration', op: '>', values: ['2s'] },
      { field: 'duration', op: '<', values: ['1s'] },
    ];
    const plan = planQuery(apart, 'start_desc', null, NAMESPACES);
    expect(plan.noMatch).toBe('The filters exclude each other, so no call can match.');
    expect(plan.params).toBeNull();
  });

  it('sorts in the browser for any order but the default without the filter model', () => {
    expect(planQuery([], 'duration_desc', null, null).serverOrdered).toBe(false);
  });
});
