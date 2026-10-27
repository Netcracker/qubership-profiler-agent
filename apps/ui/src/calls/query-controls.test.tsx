import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { Condition } from '../filter/condition';
import { QueryControls, SortBy, hasErrorsOnly, selectedChip, withChip, withErrorsOnly } from './query-controls';
import type { QueryControlsProps } from './query-controls';

afterEach(cleanup);

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
    expect(selectedChip([{ field: 'duration', op: '>', values: [] }])).toBeUndefined();
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

describe('QueryControls', () => {
  const POD: Condition = { field: 'pod', op: '=', values: ['a'] };

  function renderControls(props: Partial<QueryControlsProps> = {}) {
    const onConditionsChange = vi.fn<(next: Condition[]) => void>();
    render(
      <QueryControls
        conditions={[POD]}
        onConditionsChange={onConditionsChange}
        hideSystem={false}
        onHideSystemChange={vi.fn()}
        dirty={false}
        running={false}
        disabled={false}
        onRun={vi.fn()}
        {...props}
      />,
    );
    return onConditionsChange;
  }

  it('turns "Errors only" on and off as the error=true condition', async () => {
    const onChange = renderControls();
    const errorsOnly = screen.getByRole('switch', { name: 'Errors only' });
    expect(errorsOnly).not.toBeChecked();
    await userEvent.click(errorsOnly);
    expect(onChange).toHaveBeenLastCalledWith([POD, { field: 'error', op: '=', values: ['true'] }]);
  });

  it('shows "Errors only" as on for error=true and removes the condition on a click', async () => {
    const onChange = renderControls({ conditions: [{ field: 'error', op: '=', values: ['true'] }, POD] });
    const errorsOnly = screen.getByRole('switch', { name: 'Errors only' });
    expect(errorsOnly).toBeChecked();
    await userEvent.click(errorsOnly);
    expect(onChange).toHaveBeenLastCalledWith([POD]);
  });
});

describe('SortBy', () => {
  it('flips the direction and keeps the field', async () => {
    const onChange = vi.fn();
    const { rerender } = render(<SortBy value="duration_desc" onChange={onChange} />);
    await userEvent.click(screen.getByRole('button', { name: 'Descending. Click to sort ascending.' }));
    expect(onChange).toHaveBeenLastCalledWith('duration_asc');

    rerender(<SortBy value="duration_asc" onChange={onChange} />);
    await userEvent.click(screen.getByRole('button', { name: 'Ascending. Click to sort descending.' }));
    expect(onChange).toHaveBeenLastCalledWith('duration_desc');
  });
});
