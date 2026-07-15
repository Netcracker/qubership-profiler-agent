import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { EMPTY_CALLS_SEARCH } from '../url/search-params';
import type { CallsSearchState } from '../url/search-params';
import { CallsToolbar } from './calls-toolbar';
import { defaultColumnPrefs } from './column-prefs';
import { DEFAULT_COLUMN_ORDER } from './columns';

// Render-level regression coverage for the draft/URL desync bugs found in the
// PR 708 review: these need real DOM events and re-renders, not just the pure
// filter-state helpers.

function search(overrides: Partial<CallsSearchState> = {}): CallsSearchState {
  return { ...EMPTY_CALLS_SEARCH, fromMs: 1, toMs: 2, ...overrides };
}

describe('CallsToolbar method-substring input', () => {
  afterEach(cleanup);

  it('resyncs the draft when the committed query changes under it (back/forward)', () => {
    const { rerender } = render(
      <CallsToolbar search={search({ query: 'Tax' })} onSearchChange={vi.fn()} prefs={defaultColumnPrefs(DEFAULT_COLUMN_ORDER)} onPrefsChange={vi.fn()} disabled={false} />,
    );
    const input = screen.getByPlaceholderText('Method substring') as HTMLInputElement;
    expect(input.value).toBe('Tax');

    // Simulates a browser-back navigation: the parent re-renders with a new
    // committed `search.query` the user never typed into this input.
    rerender(
      <CallsToolbar search={search({ query: 'Invoice' })} onSearchChange={vi.fn()} prefs={defaultColumnPrefs(DEFAULT_COLUMN_ORDER)} onPrefsChange={vi.fn()} disabled={false} />,
    );
    expect(input.value).toBe('Invoice');
  });

  it('commits an empty filter immediately when the field is cleared, without Enter', async () => {
    const user = userEvent.setup();
    const onSearchChange = vi.fn();
    render(
      <CallsToolbar search={search({ query: 'Tax' })} onSearchChange={onSearchChange} prefs={defaultColumnPrefs(DEFAULT_COLUMN_ORDER)} onPrefsChange={vi.fn()} disabled={false} />,
    );
    const input = screen.getByPlaceholderText('Method substring') as HTMLInputElement;
    await user.clear(input);

    expect(input.value).toBe('');
    expect(onSearchChange).toHaveBeenCalledWith(expect.objectContaining({ query: '' }));
  });

  it('does not commit on every keystroke while the field is non-empty', async () => {
    const user = userEvent.setup();
    const onSearchChange = vi.fn();
    render(
      <CallsToolbar search={search({ query: '' })} onSearchChange={onSearchChange} prefs={defaultColumnPrefs(DEFAULT_COLUMN_ORDER)} onPrefsChange={vi.fn()} disabled={false} />,
    );
    await user.type(screen.getByPlaceholderText('Method substring'), 'Tax');
    expect(onSearchChange).not.toHaveBeenCalled();
  });
});

describe('CallsToolbar duration filter', () => {
  afterEach(cleanup);

  it('commits an upper-bound expression to min 0 / max on Enter', async () => {
    const user = userEvent.setup();
    const onSearchChange = vi.fn();
    render(
      <CallsToolbar search={search()} onSearchChange={onSearchChange} prefs={defaultColumnPrefs(DEFAULT_COLUMN_ORDER)} onPrefsChange={vi.fn()} disabled={false} />,
    );
    const input = screen.getByLabelText('Duration filter');
    await user.clear(input);
    await user.type(input, '<100ms{Enter}');
    expect(onSearchChange).toHaveBeenLastCalledWith(expect.objectContaining({ durationMinMs: 0, durationMaxMs: 100 }));
  });

  it('commits a range expression to both bounds', async () => {
    const user = userEvent.setup();
    const onSearchChange = vi.fn();
    render(
      <CallsToolbar search={search()} onSearchChange={onSearchChange} prefs={defaultColumnPrefs(DEFAULT_COLUMN_ORDER)} onPrefsChange={vi.fn()} disabled={false} />,
    );
    const input = screen.getByLabelText('Duration filter');
    await user.clear(input);
    await user.type(input, '100ms..200ms{Enter}');
    expect(onSearchChange).toHaveBeenLastCalledWith(expect.objectContaining({ durationMinMs: 100, durationMaxMs: 200 }));
  });

  it('does not commit an unparseable expression', async () => {
    const user = userEvent.setup();
    const onSearchChange = vi.fn();
    render(
      <CallsToolbar search={search()} onSearchChange={onSearchChange} prefs={defaultColumnPrefs(DEFAULT_COLUMN_ORDER)} onPrefsChange={vi.fn()} disabled={false} />,
    );
    const input = screen.getByLabelText('Duration filter') as HTMLInputElement;
    await user.clear(input);
    await user.type(input, 'fast{Enter}');
    expect(onSearchChange).not.toHaveBeenCalled();
    expect(input.value).toBe('fast');
  });

  it('a preset chip sets a lower bound and clears the upper one', async () => {
    // AntD hides the real radio input behind a label with pointer-events: none.
    const user = userEvent.setup({ pointerEventsCheck: 0 });
    const onSearchChange = vi.fn();
    render(
      <CallsToolbar search={search({ durationMinMs: 0, durationMaxMs: 100 })} onSearchChange={onSearchChange} prefs={defaultColumnPrefs(DEFAULT_COLUMN_ORDER)} onPrefsChange={vi.fn()} disabled={false} />,
    );
    await user.click(screen.getByRole('radio', { name: '>100ms' }));
    expect(onSearchChange).toHaveBeenLastCalledWith(expect.objectContaining({ durationMinMs: 100, durationMaxMs: 0 }));
  });
});

describe('CallsToolbar column settings', () => {
  afterEach(cleanup);

  it('disables unchecking the last visible column and Reset restores the rest', async () => {
    const user = userEvent.setup();
    const onPrefsChange = vi.fn();
    const prefs = { ...defaultColumnPrefs(DEFAULT_COLUMN_ORDER), hidden: DEFAULT_COLUMN_ORDER.slice(1) };
    render(
      <CallsToolbar search={search()} onSearchChange={vi.fn()} prefs={prefs} onPrefsChange={onPrefsChange} disabled={false} />,
    );
    // The trigger's accessible name concatenates its icon and label with no
    // separating space ("settingColumns"), so match loosely.
    await user.click(screen.getByRole('button', { name: /columns/i }));

    const lastVisible = screen.getByRole('checkbox', { name: new RegExp(DEFAULT_COLUMN_ORDER[0]!, 'i') });
    expect(lastVisible).toBeChecked();
    expect(lastVisible).toBeDisabled();

    await user.click(screen.getByRole('button', { name: 'Reset columns' }));
    expect(onPrefsChange).toHaveBeenCalledWith(defaultColumnPrefs(DEFAULT_COLUMN_ORDER));
  });
});
