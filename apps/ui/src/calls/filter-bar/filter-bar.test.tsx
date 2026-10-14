import { cleanup, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { Condition } from '../../filter/condition';
import { unsupportedReason } from '../../filter/translate';
import { FilterBar } from './filter-bar';
import type { FilterBarProps } from './filter-bar';

afterEach(cleanup);

const PODS = [{ value: 'checkout-a', count: 12 }, { value: 'checkout-b', count: 3 }, { value: 'billing-a' }];

function Harness(props: Partial<FilterBarProps> & { initial?: Condition[]; onConditions?: (c: Condition[]) => void }) {
  const { initial = [], onConditions, ...rest } = props;
  const [conditions, setConditions] = useState<Condition[]>(initial);
  return (
    <FilterBar
      conditions={conditions}
      onChange={(next) => {
        setConditions(next);
        onConditions?.(next);
      }}
      values={(field) => (field === 'pod' ? PODS : [])}
      {...rest}
    />
  );
}

function setup(props: Parameters<typeof Harness>[0] = {}) {
  const onConditions = vi.fn<(c: Condition[]) => void>();
  render(<Harness onConditions={onConditions} {...props} />);
  const input = screen.getByRole('combobox', { name: 'Filter calls' });
  return { input, onConditions, last: () => onConditions.mock.lastCall?.[0] };
}

const options = () => within(screen.getByRole('listbox')).getAllByRole('option');

describe('FilterBar', () => {
  it('lists the fields in a listbox of options on focus', async () => {
    const { input } = setup();
    await userEvent.click(input);
    expect(screen.getByRole('listbox', { name: 'Filter by' })).toBeInTheDocument();
    expect(options().map((o) => o.textContent)).toContain('podPod name');
  });

  it('builds a condition through the field, operator, and value stages with the keyboard', async () => {
    const { input, last } = setup();
    await userEvent.click(input);
    await userEvent.type(input, 'pod');
    await userEvent.keyboard('{Enter}');
    expect(screen.getByRole('listbox', { name: 'Operator for pod' })).toBeInTheDocument();
    await userEvent.keyboard('{Enter}');
    expect(screen.getByRole('listbox', { name: 'Values for pod' })).toBeInTheDocument();
    await userEvent.keyboard('{ArrowDown}{Enter}');
    expect(last()).toEqual([{ field: 'pod', op: '=', values: ['checkout-b'] }]);
  });

  it('moves the highlight with the arrows', async () => {
    const { input } = setup();
    await userEvent.click(input);
    await userEvent.keyboard('{ArrowDown}{ArrowDown}{ArrowUp}');
    expect(options()[1]).toHaveAttribute('aria-selected', 'true');
    expect(input).toHaveAttribute('aria-activedescendant', options()[1]!.id);
  });

  it('collects several values for in and applies them with Enter', async () => {
    const { input, last } = setup();
    await userEvent.click(input);
    await userEvent.type(input, 'pod{Enter}in');
    await userEvent.keyboard('{Enter}{Enter}{ArrowDown}{Enter}');
    expect(screen.getByRole('option', { name: /Apply 2 values/ })).toBeInTheDocument();
    await userEvent.keyboard('{Enter}');
    expect(last()).toEqual([{ field: 'pod', op: 'in', values: ['checkout-a', 'checkout-b'] }]);
  });

  it('offers None and Any for an optional field', async () => {
    const { input, last } = setup();
    await userEvent.click(input);
    await userEvent.type(input, 'trace_id{Enter}{Enter}');
    expect(options().map((o) => o.textContent?.split('no ')[0])).toContain('None');
    await userEvent.keyboard('{ArrowDown}{Enter}');
    expect(last()).toEqual([{ field: 'trace_id', op: '=', values: ['Any'] }]);
  });

  it('offers method =~ for a typed word', async () => {
    const { input, last } = setup();
    await userEvent.click(input);
    await userEvent.type(input, 'Invoice.pay');
    expect(options()[0]).toHaveTextContent('method =~ Invoice.pay');
    await userEvent.keyboard('{Enter}');
    expect(last()).toEqual([{ field: 'method', op: '=~', values: ['Invoice\\.pay'] }]);
  });

  it('shows the error line for an invalid duration and adds nothing', async () => {
    const { input, onConditions } = setup();
    await userEvent.click(input);
    await userEvent.type(input, 'duration{Enter}{Enter}fast{Enter}');
    expect(screen.getByRole('alert')).toHaveTextContent('Enter a duration such as 500ms or 2s.');
    expect(input).toHaveAttribute('aria-invalid', 'true');
    expect(onConditions).not.toHaveBeenCalled();
  });

  it('disables an unsupported field and operator with the reason, and cannot pick them', async () => {
    const { input, onConditions } = setup({ supported: unsupportedReason });
    await userEvent.click(input);
    await userEvent.type(input, 'trace_id');
    const traceId = screen.getByRole('option', { name: /^trace_id/ });
    expect(traceId).toHaveAttribute('aria-disabled', 'true');
    expect(traceId).toHaveAttribute('title', 'The backend does not support this filter yet.');
    await userEvent.keyboard('{Enter}');
    await userEvent.pointer({ keys: '[MouseLeft]', target: traceId });
    expect(screen.getByRole('listbox', { name: 'Filter by' })).toBeInTheDocument();

    await userEvent.clear(input);
    await userEvent.type(input, 'pod{Enter}');
    const notEqual = screen.getByRole('option', { name: /!=/ });
    expect(notEqual).toHaveAttribute('aria-disabled', 'true');
    await userEvent.pointer({ keys: '[MouseLeft]', target: notEqual });
    expect(screen.getByRole('listbox', { name: 'Operator for pod' })).toBeInTheDocument();
    expect(onConditions).not.toHaveBeenCalled();
  });

  it('steps back through the draft and into the last token with Backspace', async () => {
    const { input, last } = setup({ initial: [{ field: 'duration', op: '>', values: ['500ms'] }] });
    await userEvent.click(input);
    await userEvent.type(input, 'pod{Enter}{Enter}');
    expect(screen.getByRole('listbox', { name: 'Values for pod' })).toBeInTheDocument();
    await userEvent.keyboard('{Backspace}');
    expect(screen.getByRole('listbox', { name: 'Operator for pod' })).toBeInTheDocument();
    await userEvent.keyboard('{Backspace}');
    expect(screen.getByRole('listbox', { name: 'Filter by' })).toBeInTheDocument();
    await userEvent.keyboard('{Backspace}');
    // The token comes back as a draft with its value in the input, ready to edit.
    expect(last()).toEqual([]);
    expect(input).toHaveValue('500ms');
    expect(screen.getByRole('listbox', { name: 'Values for duration' })).toBeInTheDocument();
    await userEvent.keyboard('{Backspace}{Backspace}{Backspace}{Backspace}{Backspace}1s{Enter}');
    expect(last()).toEqual([{ field: 'duration', op: '>', values: ['1s'] }]);
  });

  it('closes the list with Escape and runs with Ctrl+Enter', async () => {
    const onRun = vi.fn();
    const { input } = setup({ onRun });
    await userEvent.click(input);
    await userEvent.keyboard('{Escape}');
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument();
    await userEvent.keyboard('{Control>}{Enter}{/Control}');
    expect(onRun).toHaveBeenCalledTimes(1);
  });

  it('renders a token with a reason as not applied', () => {
    setup({
      initial: [{ field: 'pod', op: '!=', values: ['checkout-a'] }],
      tokenReason: () => 'The backend does not support this filter yet.',
    });
    const remove = screen.getByRole('button', { name: 'Remove the filter pod != checkout-a' });
    expect(remove.parentElement).toHaveAttribute('data-applied', 'false');
    expect(remove.parentElement).toHaveAttribute('title', 'The backend does not support this filter yet.');
  });

  it('removes one token and clears all', async () => {
    const { last } = setup({
      initial: [
        { field: 'pod', op: '=', values: ['checkout-a'] },
        { field: 'error', op: '=', values: ['true'] },
      ],
    });
    await userEvent.click(screen.getByRole('button', { name: 'Remove the filter pod = checkout-a' }));
    expect(last()).toEqual([{ field: 'error', op: '=', values: ['true'] }]);
    await userEvent.click(screen.getByRole('button', { name: 'Clear all filters' }));
    expect(last()).toEqual([]);
  });
});
