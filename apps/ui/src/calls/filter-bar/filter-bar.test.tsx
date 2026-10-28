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

  it('reports the field whose values it lists, and adds the footer note', async () => {
    const onValueField = vi.fn<(field: string | null) => void>();
    const { input } = setup({ onValueField, valueNote: (field) => (field === 'pod' ? 'Loading values…' : null) });
    await userEvent.click(input);
    await userEvent.type(input, 'pod');
    await userEvent.keyboard('{Enter}');
    expect(onValueField).toHaveBeenLastCalledWith(null);
    await userEvent.keyboard('{Enter}');
    expect(onValueField).toHaveBeenLastCalledWith('pod');
    expect(screen.getByText(/^Loading values… /)).toBeInTheDocument();
    await userEvent.keyboard('{Escape}{Backspace}');
    expect(onValueField).toHaveBeenLastCalledWith(null);
  });

  it('shows the value note while there is nothing to pick yet', async () => {
    let note: string | null = 'Loading values…';
    const { input } = setup({ valueNote: () => note });
    await userEvent.click(input);
    await userEvent.type(input, 'service');
    await userEvent.keyboard('{Enter}{Enter}');
    const list = screen.getByRole('listbox', { name: 'Values for service' });
    expect(within(list).queryAllByRole('option')).toEqual([]);
    expect(screen.getByText(/^Loading values… /)).toBeInTheDocument();

    cleanup();
    note = null;
    const again = setup({ valueNote: () => note });
    await userEvent.click(again.input);
    await userEvent.type(again.input, 'service');
    await userEvent.keyboard('{Enter}{Enter}');
    expect(screen.queryByRole('listbox')).toBeNull();
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

  it('drops the last picked value of an in list with Backspace', async () => {
    const { input, last } = setup();
    await userEvent.click(input);
    await userEvent.type(input, 'pod{Enter}in');
    await userEvent.keyboard('{Enter}{Enter}{ArrowDown}{Enter}');
    expect(screen.getByRole('option', { name: /Apply 2 values/ })).toBeInTheDocument();
    await userEvent.keyboard('{Backspace}');
    expect(screen.getByRole('option', { name: /Apply 1 value/ })).toBeInTheDocument();
    await userEvent.keyboard('{Enter}');
    expect(last()).toEqual([{ field: 'pod', op: 'in', values: ['checkout-a'] }]);
  });

  it('steps back into an in token with its values as the draft', async () => {
    const { input, last } = setup({ initial: [{ field: 'pod', op: 'in', values: ['checkout-a', 'billing-a'] }] });
    await userEvent.click(input);
    await userEvent.keyboard('{Backspace}');
    expect(last()).toEqual([]);
    expect(input).toHaveValue('');
    expect(screen.getByRole('option', { name: /Apply 2 values/ })).toBeInTheDocument();
    // The picked values leave the suggestions.
    expect(options().map((o) => o.textContent)).not.toContain('billing-a');
    await userEvent.keyboard('{ArrowDown}{Enter}{Enter}');
    expect(last()).toEqual([{ field: 'pod', op: 'in', values: ['checkout-a', 'billing-a', 'checkout-b'] }]);
  });

  it('does nothing on Backspace in an empty bar', async () => {
    const { input, onConditions } = setup();
    await userEvent.click(input);
    await userEvent.keyboard('{Backspace}');
    expect(onConditions).not.toHaveBeenCalled();
    expect(screen.getByRole('listbox', { name: 'Filter by' })).toBeInTheDocument();
  });

  it('reopens a closed list with Enter instead of picking', async () => {
    const { input, onConditions } = setup();
    await userEvent.click(input);
    await userEvent.type(input, 'pod');
    await userEvent.keyboard('{Escape}');
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument();
    await userEvent.keyboard('{Enter}');
    expect(screen.getByRole('listbox', { name: 'Filter by' })).toBeInTheDocument();
    expect(onConditions).not.toHaveBeenCalled();
  });

  it('hides the list when nothing matches the typed text', async () => {
    const { input } = setup({ fields: ['namespace', 'service', 'pod'] });
    await userEvent.click(input);
    await userEvent.type(input, 'zzz');
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument();
    expect(input).toHaveAttribute('aria-expanded', 'false');
    expect(input).not.toHaveAttribute('aria-activedescendant');
  });

  it('moves the highlight to the hovered option', async () => {
    const { input } = setup();
    await userEvent.click(input);
    await userEvent.hover(options()[2]!);
    expect(options()[2]).toHaveAttribute('aria-selected', 'true');
    expect(input).toHaveAttribute('aria-activedescendant', options()[2]!.id);
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

  it('renders a token with a note as applied in the browser', () => {
    setup({ initial: [{ field: 'trace_id', op: '=', values: ['abc'] }], tokenNote: () => 'Filters the loaded rows.' });
    const token = screen.getByRole('button', { name: 'Remove the filter trace_id = abc' }).parentElement!;
    expect(token).toHaveAttribute('data-applied', 'browser');
    expect(token).toHaveAttribute('title', 'Filters the loaded rows.');
  });

  describe('a preset token', () => {
    const preset = (onRemove: () => void) => ({
      key: 'hide_system',
      field: 'method',
      op: '!~',
      value: 'system/proxy',
      title: 'Hides system calls.',
      onRemove,
    });

    it('comes before the conditions and is removed with its ×', async () => {
      const onRemove = vi.fn();
      setup({ initial: [{ field: 'error', op: '=', values: ['true'] }], presets: [preset(onRemove)] });
      const remove = screen.getByRole('button', { name: 'Remove the filter method !~ system/proxy' });
      expect(remove.parentElement).toHaveAttribute('title', 'Hides system calls.');
      expect(remove.parentElement).toHaveAttribute('data-applied', 'browser');
      const tokens = [...document.querySelectorAll('[data-applied]')].map((t) => t.textContent);
      expect(tokens).toEqual(['method!~system/proxy', 'error=true']);
      await userEvent.click(remove);
      expect(onRemove).toHaveBeenCalledTimes(1);
    });

    it('is removed with Backspace once no condition is left', async () => {
      const onRemove = vi.fn();
      const { input, last } = setup({ initial: [{ field: 'error', op: '=', values: ['true'] }], presets: [preset(onRemove)] });
      await userEvent.click(input);
      // The first Backspace steps into the condition, which comes back as a draft with its value.
      await userEvent.keyboard('{Backspace}');
      expect(last()).toEqual([]);
      expect(onRemove).not.toHaveBeenCalled();
      // Four delete "true", then the operator, the field, and the preset go.
      await userEvent.keyboard('{Backspace}{Backspace}{Backspace}{Backspace}{Backspace}{Backspace}');
      expect(onRemove).not.toHaveBeenCalled();
      await userEvent.keyboard('{Backspace}');
      expect(onRemove).toHaveBeenCalledTimes(1);
    });

    it('is removed by "Clear all filters"', async () => {
      const onRemove = vi.fn();
      setup({ presets: [preset(onRemove)] });
      await userEvent.click(screen.getByRole('button', { name: 'Clear all filters' }));
      expect(onRemove).toHaveBeenCalledTimes(1);
    });
  });
});
