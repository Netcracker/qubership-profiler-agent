import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { CallJSON, StatsResponse } from '../api/types';
import { timeStats } from '../mocks/stats';
import { CallsHistogram } from './calls-histogram';
import type { CallsHistogramProps } from './calls-histogram';

afterEach(() => {
  cleanup();
  localStorage.clear();
});

const FROM = 1_700_000_000_000;
const TO = FROM + 60_000;

/** Calls at the given offsets from FROM, in ms; a negative offset marks a failed call. */
function stats(offsets: number[]): StatsResponse {
  const calls = offsets.map(
    (o) => ({ ts_ms: FROM + Math.abs(o), duration_ms: 10, error_flag: o < 0, cpu_time_ms: 0, wait_time_ms: 0, queue_wait_ms: 0, suspend_ms: 0, params: {} }) as unknown as CallJSON,
  );
  return timeStats(calls, FROM, TO, 6);
}

function renderHistogram(props: Partial<CallsHistogramProps> = {}) {
  const onZoom = vi.fn();
  const onRetry = vi.fn();
  render(
    <CallsHistogram
      state={{ kind: 'ready', data: stats([1000, 2000, -15_000, 59_000]) }}
      fromMs={FROM}
      toMs={TO}
      onZoom={onZoom}
      onRetry={onRetry}
      hideSystem={false}
      {...props}
    />,
  );
  return { onZoom, onRetry };
}

/** Gives the plot a 600 px width, which jsdom does not lay out. */
function sizePlot(): HTMLElement {
  const plot = screen.getByTestId('histogram-plot');
  plot.getBoundingClientRect = () => ({ left: 0, top: 0, width: 600, height: 96, right: 600, bottom: 96, x: 0, y: 0, toJSON: () => ({}) });
  return plot;
}

describe('CallsHistogram', () => {
  it('shows the totals and one bar per bucket', () => {
    renderHistogram();
    expect(screen.getByText('4 calls · 1 error')).toBeInTheDocument();
    expect(screen.getAllByTestId('histogram-bar')).toHaveLength(6);
    expect(screen.getByRole('img', { name: '4 calls · 1 error' })).toBeInTheDocument();
  });

  it('zooms in on the buckets a drag selects', async () => {
    const { onZoom } = renderHistogram();
    const plot = sizePlot();
    fireEvent.mouseDown(plot, { clientX: 150, button: 0 });
    fireEvent.mouseMove(plot, { clientX: 350 });
    fireEvent.mouseUp(plot, { clientX: 350 });
    expect(screen.getByTestId('histogram-selection')).toHaveAttribute('width', '3');
    await userEvent.click(screen.getByRole('button', { name: /Zoom in/ }));
    // Buckets 1 to 3 of six 10-second buckets.
    expect(onZoom).toHaveBeenCalledWith(FROM + 10_000, FROM + 40_000);
    expect(screen.queryByTestId('histogram-selection')).toBeNull();
  });

  it('clears a selection without zooming', async () => {
    const { onZoom } = renderHistogram();
    const plot = sizePlot();
    fireEvent.mouseDown(plot, { clientX: 10, button: 0 });
    fireEvent.mouseUp(plot, { clientX: 10 });
    await userEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(screen.queryByTestId('histogram-selection')).toBeNull();
    expect(onZoom).not.toHaveBeenCalled();
  });

  it('ignores a right button press and stops a drag when the pointer leaves', () => {
    renderHistogram();
    const plot = sizePlot();
    fireEvent.mouseDown(plot, { clientX: 10, button: 2 });
    expect(screen.queryByTestId('histogram-selection')).toBeNull();
    fireEvent.mouseDown(plot, { clientX: 10, button: 0 });
    fireEvent.mouseLeave(plot);
    fireEvent.mouseMove(plot, { clientX: 590 });
    expect(screen.getByTestId('histogram-selection')).toHaveAttribute('width', '1');
  });

  it('describes the bucket under the pointer', () => {
    renderHistogram();
    const plot = sizePlot();
    fireEvent.mouseMove(plot, { clientX: 150 });
    expect(screen.getByText(/· 1 call · 1 error · p95 10ms$/)).toBeInTheDocument();
    fireEvent.mouseLeave(plot);
    expect(screen.queryByText(/p95/)).toBeNull();
  });

  it('collapses and remembers it', async () => {
    renderHistogram();
    await userEvent.click(screen.getByRole('button', { name: /Calls over time/ }));
    expect(screen.queryByTestId('histogram-plot')).toBeNull();
    expect(localStorage.getItem('profiler.histogram.collapsed')).toBe('1');
    cleanup();
    renderHistogram();
    expect(screen.queryByTestId('histogram-plot')).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: /Calls over time/ }));
    expect(localStorage.getItem('profiler.histogram.collapsed')).toBeNull();
  });

  it('shows an error with a retry', async () => {
    const { onRetry } = renderHistogram({ state: { kind: 'error', message: '503 Service Unavailable' } });
    expect(screen.getByText('Counting the calls failed')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(onRetry).toHaveBeenCalled();
  });

  it('says so while it counts', () => {
    renderHistogram({ state: { kind: 'loading' } });
    expect(screen.getByText('Counting calls…')).toBeInTheDocument();
    expect(screen.queryAllByTestId('histogram-bar')).toHaveLength(0);
  });

  it('notes a partial answer and the system/proxy calls it still counts', () => {
    renderHistogram({ state: { kind: 'ready', data: { ...stats([]), partial: true } }, hideSystem: true });
    expect(screen.getByText(/the counts may be low/)).toBeInTheDocument();
    expect(screen.getByText(/filters the loaded rows only/)).toBeInTheDocument();
  });
});
