import { describe, expect, it } from 'vitest';

import type { FilterCapabilities } from '../api/types';
import { capabilitiesOf } from './capabilities';

const STAGE_A: FilterCapabilities = {
  fields: { duration: ['>', '>='], service: ['=', 'in'], trace_id: ['='], method: ['=~'] },
  param_operators: ['='],
  special_values: [],
  orders: ['start_desc', 'duration_desc'],
  group_by: ['time', 'service'],
};

describe('capabilitiesOf', () => {
  const caps = capabilitiesOf(STAGE_A);

  it('enables a field the build lists, with its listed operators only', () => {
    expect(caps.supported('service')).toBeNull();
    expect(caps.supported('service', 'in')).toBeNull();
    expect(caps.supported('service', '!=')).toBe('This backend does not support != on service.');
  });

  it('disables a catalog field the build does not list', () => {
    expect(caps.supported('class')).toBe('This backend cannot filter by class.');
  });

  it('gives call params the param operators', () => {
    expect(caps.supported('user.tenant', '=')).toBeNull();
    expect(caps.supported('user.tenant', 'in')).toBe('This backend does not support in on user.tenant.');
    const none = capabilitiesOf({ ...STAGE_A, param_operators: [] });
    expect(none.supported('user.tenant')).toBe('This backend cannot filter by call parameters.');
  });

  it('rejects None and Any with = and != until the build lists them', () => {
    expect(caps.conditionReason({ field: 'trace_id', op: '=', values: ['None'] })).toBe('This backend cannot match None yet.');
    expect(caps.conditionReason({ field: 'trace_id', op: '=', values: ['abc'] })).toBeNull();
    // With =~, Any is plain text.
    expect(caps.conditionReason({ field: 'method', op: '=~', values: ['Any'] })).toBeNull();
    const b = capabilitiesOf({ ...STAGE_A, special_values: ['None', 'Any'] });
    expect(b.conditionReason({ field: 'trace_id', op: '=', values: ['None'] })).toBeNull();
  });

  it('reports the condition reason before the value check', () => {
    expect(caps.conditionReason({ field: 'service', op: '!=', values: ['None'] })).toBe(
      'This backend does not support != on service.',
    );
  });

  it('lists the orders and groupings', () => {
    expect([...caps.orders]).toEqual(['start_desc', 'duration_desc']);
    expect(caps.canGroupBy('time')).toBe(true);
    expect(caps.canGroupBy('pod')).toBe(false);
    expect(caps.canGroupBy('user.tenant')).toBe(false);
    expect(capabilitiesOf({ ...STAGE_A, group_by_params: true }).canGroupBy('user.tenant')).toBe(true);
  });
});
