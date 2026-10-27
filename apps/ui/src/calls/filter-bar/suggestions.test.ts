import { describe, expect, it } from 'vitest';

import { ALL_FIELDS, EMPTY_DRAFT, MAX_VALUES, stageView } from './suggestions';
import type { StageInput } from './suggestions';

const base: StageInput = { draft: EMPTY_DRAFT, text: '', fields: ALL_FIELDS, supported: () => null, values: () => [] };

describe('stageView', () => {
  it('offers a whole typed condition first', () => {
    const view = stageView({ ...base, text: 'duration>1s' });
    expect(view.items[0]).toMatchObject({ label: 'duration>1s', action: { kind: 'condition' } });
  });

  it('lists the catalog values of a closed field instead of the source', () => {
    const view = stageView({ ...base, draft: { field: 'error', op: '=', values: [] }, values: () => [{ value: 'x' }] });
    expect(view.items.map((i) => i.label)).toEqual(['true', 'false']);
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

  it('leaves out the values already picked for in', () => {
    const view = stageView({
      ...base,
      draft: { field: 'pod', op: 'in', values: ['a'] },
      values: () => [{ value: 'a' }, { value: 'b' }],
    });
    expect(view.items.map((i) => i.label)).toEqual(['Apply 1 value', 'b']);
  });
});
