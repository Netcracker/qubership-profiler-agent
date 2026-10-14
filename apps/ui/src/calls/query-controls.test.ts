import { describe, expect, it } from 'vitest';

import type { Condition } from '../filter/condition';
import { hasErrorsOnly, selectedChip, withChip, withErrorsOnly } from './query-controls';

const gt = (value: string): Condition => ({ field: 'duration', op: '>', values: [value] });

describe('duration chips', () => {
  it('lights All without a duration condition and a chip for exactly one matching > condition', () => {
    expect(selectedChip([])).toBe('');
    expect(selectedChip([gt('500ms')])).toBe('500ms');
    expect(selectedChip([gt('0.5s')])).toBe('500ms');
    expect(selectedChip([gt('3000ms')])).toBe('3s');
  });

  it('lights no chip for another operator, another value, or two duration conditions', () => {
    expect(selectedChip([{ field: 'duration', op: '>=', values: ['500ms'] }])).toBeUndefined();
    expect(selectedChip([gt('250ms')])).toBeUndefined();
    expect(selectedChip([gt('500ms'), { field: 'duration', op: '<', values: ['5s'] }])).toBeUndefined();
  });

  it('replaces every duration condition with the picked chip', () => {
    const pod: Condition = { field: 'pod', op: '=', values: ['a'] };
    expect(withChip([pod, gt('1s'), { field: 'duration', op: '<', values: ['5s'] }], '3s')).toEqual([gt('3s'), pod]);
    expect(withChip([pod, gt('1s')], '')).toEqual([pod]);
  });
});

describe('Errors only', () => {
  it('adds and removes the error=true condition', () => {
    const on = withErrorsOnly([], true);
    expect(on).toEqual([{ field: 'error', op: '=', values: ['true'] }]);
    expect(hasErrorsOnly(on)).toBe(true);
    expect(withErrorsOnly(on, false)).toEqual([]);
  });
});
