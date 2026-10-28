import { describe, expect, it } from 'vitest';

import { capabilitiesOf } from '../../filter/capabilities';

import { ALL_FIELDS, EMPTY_DRAFT, MAX_VALUES, stageView } from './suggestions';
import type { StageInput } from './suggestions';

const base: StageInput = { draft: EMPTY_DRAFT, text: '', fields: ALL_FIELDS, supported: () => null, values: () => [] };

describe('stageView', () => {
  it('offers a whole typed condition first', () => {
    const view = stageView({ ...base, text: 'duration>1s' });
    expect(view.items[0]).toMatchObject({ label: 'duration>1s', action: { kind: 'condition' } });
  });

  it('offers a whole call parameter condition when the bar takes call parameters', () => {
    const view = stageView({ ...base, params: true, text: 'user.tenant=acme' });
    expect(view.items[0]).toMatchObject({
      label: 'user.tenant=acme',
      action: { kind: 'condition', condition: { field: 'user.tenant', op: '=', values: ['acme'] } },
    });
    expect(view.foot).toContain('call parameter');
  });

  it('offers a typed call parameter name as a field after the method search', () => {
    const view = stageView({ ...base, params: true, text: 'user.tenant', supported: (f) => (f === 'user.tenant' ? 'no' : null) });
    expect(view.items.map((i) => i.label)).toEqual(['method =~ user.tenant', 'user.tenant']);
    expect(view.items[1]).toMatchObject({ hint: 'Call parameter', disabledReason: 'no', action: { kind: 'field', field: 'user.tenant' } });
  });

  it('offers no call parameter when the bar does not take them', () => {
    const fields = ['namespace', 'service', 'pod'];
    expect(stageView({ ...base, fields, text: 'user.tenant=acme' }).items).toEqual([]);
    expect(stageView({ ...base, fields, text: 'user.tenant' }).items).toEqual([]);
    expect(stageView({ ...base, text: 'user.tenant=acme' }).items.map((i) => i.label)).toEqual([]);
  });

  it('offers no call parameter for a catalog field left out of the list', () => {
    const view = stageView({ ...base, fields: ['pod'], params: true, text: 'duration>1s' });
    expect(view.items).toEqual([]);
  });

  it('lists the catalog values of a closed field instead of the source', () => {
    const view = stageView({ ...base, draft: { field: 'error', op: '=', values: [] }, values: () => [{ value: 'x' }] });
    expect(view.items.map((i) => i.label)).toEqual(['true', 'false']);
  });

  it('keeps the catalog order of a closed field and takes the counts the source has', () => {
    const counted = (field: string) =>
      stageView({
        ...base,
        draft: { field, op: '=', values: [] },
        values: () => [{ value: 'false', count: 90 }, { value: 'true', count: 4 }, { value: 'long_clean', count: 7 }],
      }).items.map((i) => [i.label, i.count]);
    expect(counted('error')).toEqual([
      ['true', 4],
      ['false', 90],
    ]);
    expect(counted('retention_class')).toContainEqual(['long_clean', 7]);
    expect(counted('retention_class').filter(([, n]) => n === undefined).length).toBeGreaterThan(0);
  });

  it('caps the value list and says how many matched', () => {
    const many = Array.from({ length: 20 }, (_, i) => ({ value: `pod-${i}` }));
    const view = stageView({ ...base, draft: { field: 'pod', op: '=', values: [] }, values: () => many });
    expect(view.items).toHaveLength(MAX_VALUES);
    expect(view.foot).toMatch(/^Showing 8 of 20 values\./);
  });

  it('offers range presets for between and single durations for the other operators', () => {
    const range = stageView({ ...base, draft: { field: 'duration', op: 'between', values: [] } });
    expect(range.items.map((i) => i.label)).toEqual(['100ms..500ms', '500ms..2s', '1s..5s']);
    expect(range.foot).toBe('Type a range such as 100ms..2s.');

    const bound = stageView({ ...base, draft: { field: 'duration', op: '>', values: [] }, text: '1' });
    expect(bound.items.map((i) => i.label)).toEqual(['Use "1"', '10ms', '100ms', '1s']);
    expect(bound.foot).toBe('Type any duration such as 250ms or 1.5s.');
  });

  it('offers None and Any for = and != on an optional field only, and not while typing', () => {
    const labels = (draft: StageInput['draft'], text = '') => stageView({ ...base, draft, text }).items.map((i) => i.label);
    expect(labels({ field: 'trace_id', op: '!=', values: [] })).toEqual(['None', 'Any']);
    expect(labels({ field: 'trace_id', op: '=~', values: [] })).toEqual([]);
    expect(labels({ field: 'trace_id', op: '=', values: [] }, 'ab')).toEqual(['Use "ab"']);
    expect(labels({ field: 'pod', op: '=', values: [] })).toEqual([]);
  });

  it('disables None, Any, and whole conditions the backend cannot apply', () => {
    const caps = capabilitiesOf({
      fields: { trace_id: ['=', '!='], service: ['='] },
      param_operators: [],
      special_values: ['Any'],
      orders: [],
      group_by: [],
    });
    const checked: StageInput = { ...base, supported: caps.supported, conditionReason: caps.conditionReason };

    const specials = stageView({ ...checked, draft: { field: 'trace_id', op: '=', values: [] } }).items;
    expect(specials.map((i) => [i.label, i.disabledReason])).toEqual([
      ['None', 'This backend cannot match None yet.'],
      ['Any', undefined],
    ]);

    const typedValue = stageView({ ...checked, draft: { field: 'trace_id', op: '=', values: [] }, text: 'None' }).items[0]!;
    expect(typedValue).toMatchObject({ label: 'Use "None"', disabledReason: 'This backend cannot match None yet.' });
    const plain = stageView({ ...checked, draft: { field: 'trace_id', op: '=', values: [] }, text: 'abc' }).items[0]!;
    expect(plain.disabledReason).toBeUndefined();

    const typed = stageView({ ...checked, text: 'trace_id=None' }).items[0]!;
    expect(typed).toMatchObject({ label: 'trace_id=None', disabledReason: 'This backend cannot match None yet.' });
    const search = stageView({ ...checked, text: 'checkout' }).items.at(-1)!;
    expect(search).toMatchObject({ label: 'method =~ checkout', disabledReason: 'This backend cannot filter by method.' });
  });

  it('leaves out the values already picked for in', () => {
    const view = stageView({
      ...base,
      draft: { field: 'pod', op: 'in', values: ['a'] },
      values: () => [{ value: 'a' }, { value: 'b' }],
    });
    expect(view.items.map((i) => i.label)).toEqual(['Apply 1 value', 'b']);
  });
});
