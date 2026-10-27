import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { ColumnChooser, ColumnSettings } from './column-chooser';
import { defaultColumnPrefs } from './column-prefs';
import type { ColumnPrefs } from './column-prefs';

afterEach(cleanup);

function Harness({ initial, onPrefs }: { initial: ColumnPrefs; onPrefs: (p: ColumnPrefs) => void }) {
  const [prefs, setPrefs] = useState(initial);
  return (
    <ColumnSettings
      prefs={prefs}
      onPrefsChange={(next) => {
        setPrefs(next);
        onPrefs(next);
      }}
    />
  );
}

function setup(initial: ColumnPrefs = defaultColumnPrefs()) {
  const onPrefs = vi.fn<(p: ColumnPrefs) => void>();
  render(<Harness initial={initial} onPrefs={onPrefs} />);
  return { last: () => onPrefs.mock.lastCall?.[0] };
}

const SMALL: ColumnPrefs = { order: ['start', 'duration', 'pod'], hidden: ['pod'], widths: { start: 200 } };

describe('ColumnSettings', () => {
  it('lists the columns in the stored order with their titles and visibility', () => {
    setup(SMALL);
    const boxes = screen.getAllByRole('checkbox');
    expect(boxes.map((b) => b.closest('label')!.textContent)).toEqual(['Start', 'Duration', 'Pod']);
    expect(boxes.map((b) => (b as HTMLInputElement).checked)).toEqual([true, true, false]);
  });

  it('hides and shows a column', async () => {
    const { last } = setup(SMALL);
    await userEvent.click(screen.getByRole('checkbox', { name: 'Duration' }));
    expect(last()).toEqual({ ...SMALL, hidden: ['pod', 'duration'] });
    await userEvent.click(screen.getByRole('checkbox', { name: 'Pod' }));
    expect(last()).toEqual({ ...SMALL, hidden: ['duration'] });
  });

  it('keeps the last visible column checked and disabled', () => {
    setup({ ...SMALL, hidden: ['duration', 'pod'] });
    expect(screen.getByRole('checkbox', { name: 'Start' })).toBeDisabled();
    expect(screen.getByRole('checkbox', { name: 'Start' })).toBeChecked();
    expect(screen.getByRole('checkbox', { name: 'Pod' })).toBeEnabled();
  });

  it('moves a column up and down, and cannot move past either end', async () => {
    const { last } = setup(SMALL);
    expect(screen.getByRole('button', { name: 'Move Start column up' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Move Pod column down' })).toBeDisabled();

    await userEvent.click(screen.getByRole('button', { name: 'Move Pod column up' }));
    expect(last()).toEqual({ ...SMALL, order: ['start', 'pod', 'duration'] });
    await userEvent.click(screen.getByRole('button', { name: 'Move Start column down' }));
    expect(last()).toEqual({ ...SMALL, order: ['pod', 'start', 'duration'] });
  });

  it('resets to the default columns', async () => {
    const { last } = setup(SMALL);
    await userEvent.click(screen.getByRole('button', { name: 'Reset columns' }));
    expect(last()).toEqual(defaultColumnPrefs());
  });

  it('names a column without a title by its key', () => {
    setup({ order: ['start', 'retired'], hidden: [], widths: {} });
    expect(screen.getByRole('checkbox', { name: 'retired' })).toBeInTheDocument();
  });
});

describe('ColumnChooser', () => {
  it('opens the column settings from the "Columns" button', async () => {
    render(<ColumnChooser prefs={SMALL} onPrefsChange={vi.fn()} />);
    expect(screen.queryByRole('button', { name: 'Reset columns' })).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Choose columns' }));
    expect(await screen.findByRole('button', { name: 'Reset columns' })).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: 'Duration' })).toBeChecked();
  });
});
