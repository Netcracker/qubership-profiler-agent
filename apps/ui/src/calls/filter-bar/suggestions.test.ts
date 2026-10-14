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

  it('leaves out the values already picked for in', () => {
    const view = stageView({
      ...base,
      draft: { field: 'pod', op: 'in', values: ['a'] },
      values: () => [{ value: 'a' }, { value: 'b' }],
    });
    expect(view.items.map((i) => i.label)).toEqual(['Apply 1 value', 'b']);
  });
});
