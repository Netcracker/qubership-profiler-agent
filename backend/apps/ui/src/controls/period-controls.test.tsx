import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import dayjs from 'dayjs';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { PeriodControls } from './period-controls';
import { BROWSER_ZONE, exprFromMs } from './time-range';
import type { TimeRange } from './time-range';
import { getZone, setZone } from '../ui/timezone';

// Render-level coverage for the picker's commit paths: real DOM events through
// the popover, not just the pure time-range helpers.

afterEach(() => {
  cleanup();
  setZone(BROWSER_ZONE); // the timezone store is app-global; reset it between tests
});

const PLACEHOLDER = 'now-6h or 2026-07-13 12:00:00';
const EMPTY: TimeRange = { from: null, to: null, fromMs: null, toMs: null };

/** An absolute (epoch-ms) range, as the URL/page would hand it to the picker. */
function msRange(fromMs: number, toMs: number): TimeRange {
  return { from: String(fromMs), to: String(toMs), fromMs, toMs };
}

function renderPicker(value: TimeRange = EMPTY, canApply = true) {
  const onCommitRange = vi.fn();
  const onApply = vi.fn();
  render(
    <PeriodControls
      value={value}
      onCommitRange={onCommitRange}
      onApply={onApply}
      canApply={canApply}
      applying={false}
    />,
  );
  return { onCommitRange, onApply };
}

describe('PeriodControls', () => {
  it('commits absolute From/To as epoch-ms tokens on "Apply time range"', async () => {
    const user = userEvent.setup();
    const { onCommitRange } = renderPicker();

    await user.click(screen.getByRole('button', { name: 'Time range' }));
    const [from, to] = screen.getAllByPlaceholderText(PLACEHOLDER);
    await user.type(from!, '2026-07-13 08:00:00');
    await user.type(to!, '2026-07-13 12:30:00');
    await user.click(screen.getByRole('button', { name: 'Apply time range' }));

    expect(onCommitRange).toHaveBeenCalledWith({
      from: String(dayjs('2026-07-13 08:00:00', 'YYYY-MM-DD HH:mm:ss').valueOf()),
      to: String(dayjs('2026-07-13 12:30:00', 'YYYY-MM-DD HH:mm:ss').valueOf()),
    });
  });

  it('commits a quick range as live relative tokens', async () => {
    const user = userEvent.setup();
    const { onCommitRange } = renderPicker();

    await user.click(screen.getByRole('button', { name: 'Time range' }));
    await user.click(screen.getByRole('button', { name: 'Last 6 hours' }));

    // Relative, not frozen ms, so a shared link stays live (Grafana-style).
    expect(onCommitRange).toHaveBeenCalledWith({ from: 'now-6h', to: 'now' });
  });

  it('keeps the quick-range name on the trigger after picking it', async () => {
    const user = userEvent.setup();
    const now = Date.now();
    renderPicker(msRange(now - 6 * 60 * 60 * 1000, now));

    await user.click(screen.getByRole('button', { name: 'Time range' }));
    await user.click(screen.getByRole('button', { name: 'Last 6 hours' }));

    expect(screen.getByRole('button', { name: 'Time range' })).toHaveTextContent('Last 6 hours');
  });

  it('labels the trigger from the committed window, not an unapplied field edit', async () => {
    const user = userEvent.setup();
    const now = Date.now();
    renderPicker(msRange(now - 6 * 60 * 60 * 1000, now));

    await user.click(screen.getByRole('button', { name: 'Time range' }));
    await user.click(screen.getByRole('button', { name: 'Last 6 hours' }));

    // Edit From to now-12h but do NOT apply it.
    await user.click(screen.getByRole('button', { name: 'Time range' }));
    const [from] = screen.getAllByPlaceholderText(PLACEHOLDER);
    await user.clear(from!);
    await user.type(from!, 'now-12h');

    const trigger = screen.getByRole('button', { name: 'Time range' });
    expect(trigger).toHaveTextContent('Last 6 hours');
    expect(trigger).not.toHaveTextContent('Last 12 hours');
  });

  it('discards an unapplied field edit when the popover is reopened', async () => {
    const user = userEvent.setup();
    const now = Date.now();
    const fromMs = now - 6 * 60 * 60 * 1000;
    renderPicker(msRange(fromMs, now));

    const trigger = screen.getByRole('button', { name: 'Time range' });
    await user.click(trigger);
    const [from] = screen.getAllByPlaceholderText(PLACEHOLDER) as HTMLInputElement[];
    await user.clear(from!);
    await user.type(from!, 'now-12h');
    expect(from!.value).toBe('now-12h');

    await user.click(trigger); // close without applying
    await user.click(trigger); // reopen

    const [reopened] = screen.getAllByPlaceholderText(PLACEHOLDER) as HTMLInputElement[];
    expect(reopened!.value).toBe(exprFromMs(fromMs)); // committed window, edit gone
  });

  it('closes on Escape, cancelling the edit', async () => {
    const user = userEvent.setup();
    const now = Date.now();
    const fromMs = now - 6 * 60 * 60 * 1000;
    renderPicker(msRange(fromMs, now));

    const trigger = screen.getByRole('button', { name: 'Time range' });
    await user.click(trigger);
    const [from] = screen.getAllByPlaceholderText(PLACEHOLDER) as HTMLInputElement[];
    await user.clear(from!);
    await user.type(from!, 'now-1h');

    fireEvent.keyDown(from!, { key: 'Escape' }); // Escape from inside the popover closes it
    await user.click(trigger); // reopen — if Escape had not closed it, this would close it instead

    const [reopened] = screen.getAllByPlaceholderText(PLACEHOLDER) as HTMLInputElement[];
    expect(reopened!.value).toBe(exprFromMs(fromMs));
  });

  it('flags an unparseable expression instead of committing', async () => {
    const user = userEvent.setup();
    const { onCommitRange } = renderPicker();

    await user.click(screen.getByRole('button', { name: 'Time range' }));
    const [from] = screen.getAllByPlaceholderText(PLACEHOLDER);
    await user.type(from!, 'yesterday');
    await user.click(screen.getByRole('button', { name: 'Apply time range' }));

    expect(onCommitRange).not.toHaveBeenCalled();
  });

  it('rejects a From that is after To', async () => {
    const user = userEvent.setup();
    const { onCommitRange } = renderPicker();

    await user.click(screen.getByRole('button', { name: 'Time range' }));
    const [from, to] = screen.getAllByPlaceholderText(PLACEHOLDER);
    await user.type(from!, '2026-07-13 12:00:00');
    await user.type(to!, '2026-07-13 08:00:00');
    await user.click(screen.getByRole('button', { name: 'Apply time range' }));

    expect(onCommitRange).not.toHaveBeenCalled();
    expect(screen.getByText(/must be before/i)).toBeInTheDocument();
  });

  it('shifts the window back by half its width, committing absolute ms', async () => {
    const user = userEvent.setup();
    const { onCommitRange } = renderPicker(msRange(1000, 3000));

    await user.click(screen.getByRole('button', { name: 'Move time range backwards' }));
    expect(onCommitRange).toHaveBeenCalledWith({ from: '0', to: '2000' });
  });

  it('zooms out around the centre, committing absolute ms', async () => {
    const user = userEvent.setup();
    const { onCommitRange } = renderPicker(msRange(1000, 3000));

    await user.click(screen.getByRole('button', { name: 'Zoom out time range' }));
    expect(onCommitRange).toHaveBeenCalledWith({ from: '0', to: '4000' });
  });

  it('disables Apply until the selection is dirty', () => {
    const now = Date.now();
    const window = msRange(now - 3_600_000, now);
    const { unmount } = render(
      <PeriodControls value={window} onCommitRange={vi.fn()} onApply={vi.fn()} canApply={false} applying={false} />,
    );
    expect(screen.getByRole('button', { name: 'Apply' })).toBeDisabled();
    unmount();

    renderPicker(window, true);
    expect(screen.getByRole('button', { name: 'Apply' })).toBeEnabled();
  });

  it('disables navigation until a window is set', () => {
    renderPicker();
    expect(screen.getByRole('button', { name: 'Move time range backwards' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Move time range forwards' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Zoom out time range' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Zoom in time range' })).toBeDisabled();
  });

  it('re-renders the absolute field in the chosen zone', async () => {
    const user = userEvent.setup();
    const fromMs = Date.UTC(2026, 6, 13, 12, 0, 0);
    renderPicker(msRange(fromMs, fromMs + 3_600_000));

    await user.click(screen.getByRole('button', { name: 'Time range' }));
    const [from] = screen.getAllByPlaceholderText(PLACEHOLDER) as HTMLInputElement[];
    expect(from!.value).toBe(exprFromMs(fromMs)); // browser zone first

    await user.click(screen.getByRole('combobox'));
    await user.type(screen.getByRole('combobox'), 'UTC');
    await user.click(screen.getByTitle('UTC'));

    expect(from!.value).toBe('2026-07-13 12:00:00'); // the same instant, in UTC
  });

  it('applies a zone change immediately and does not revert it on cancel', async () => {
    const user = userEvent.setup();
    const fromMs = Date.UTC(2026, 6, 13, 12, 0, 0);
    renderPicker(msRange(fromMs, fromMs + 3_600_000));

    const trigger = screen.getByRole('button', { name: 'Time range' });
    await user.click(trigger);
    const [from] = screen.getAllByPlaceholderText(PLACEHOLDER) as HTMLInputElement[];
    expect(from!.value).toBe(exprFromMs(fromMs)); // browser zone

    await user.click(screen.getByRole('combobox'));
    await user.type(screen.getByRole('combobox'), 'UTC');
    await user.click(screen.getByTitle('UTC'));
    expect(from!.value).toBe('2026-07-13 12:00:00'); // shifted to UTC at once
    expect(getZone()).toBe('UTC'); // committed app-wide immediately, no Apply needed

    // Cancel closes the popover, but the zone is a setting, not a draft — it stays.
    fireEvent.keyDown(from!, { key: 'Escape' });
    await user.click(trigger); // reopen

    const [reopened] = screen.getAllByPlaceholderText(PLACEHOLDER) as HTMLInputElement[];
    expect(reopened!.value).toBe('2026-07-13 12:00:00'); // still UTC
    expect(getZone()).toBe('UTC');
  });

  it('shows each zone with its current offset and searches by offset', async () => {
    const user = userEvent.setup();
    const fromMs = Date.UTC(2026, 6, 13, 12, 0, 0);
    renderPicker(msRange(fromMs, fromMs + 3_600_000));

    await user.click(screen.getByRole('button', { name: 'Time range' }));
    await user.click(screen.getByRole('combobox'));
    await user.type(screen.getByRole('combobox'), '+09:00');

    const tokyo = screen.getByTitle('Asia/Tokyo');
    expect(tokyo).toBeInTheDocument();
    expect(tokyo).toHaveTextContent('UTC+09:00');
  });
});
