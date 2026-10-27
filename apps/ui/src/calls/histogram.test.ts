import { describe, expect, it } from 'vitest';

import type { StatsRow } from '../api/types';
import { axisLabel, bucketAt, niceMax, selectionRange } from './histogram';

const row = (key: number): StatsRow => ({
  key: String(key),
  count: 0,
  errors: 0,
  duration_ms_sum: 0,
  p50_ms: null,
  p95_ms: null,
  p99_ms: null,
  cpu_time_ms_sum: 0,
  wait_time_ms_sum: 0,
  queue_wait_ms_sum: 0,
  suspend_ms_sum: 0,
});

describe('niceMax', () => {
  it('rounds up to 1, 2, or 5 times a power of ten', () => {
    expect(niceMax(0)).toBe(1);
    expect(niceMax(1)).toBe(1);
    expect(niceMax(2)).toBe(2);
    expect(niceMax(3)).toBe(5);
    expect(niceMax(7)).toBe(10);
    expect(niceMax(10)).toBe(10);
    expect(niceMax(11)).toBe(20);
    expect(niceMax(450)).toBe(500);
    expect(niceMax(501)).toBe(1000);
  });
});

describe('bucketAt', () => {
  it('maps an x offset to its bucket and clamps to the plot', () => {
    expect(bucketAt(0, 600, 60)).toBe(0);
    expect(bucketAt(9.9, 600, 60)).toBe(0);
    expect(bucketAt(10, 600, 60)).toBe(1);
    expect(bucketAt(599, 600, 60)).toBe(59);
    expect(bucketAt(700, 600, 60)).toBe(59);
    expect(bucketAt(-5, 600, 60)).toBe(0);
  });

  it('answers 0 for a plot with no size or no buckets', () => {
    expect(bucketAt(10, 0, 60)).toBe(0);
    expect(bucketAt(10, 600, 0)).toBe(0);
  });
});

describe('selectionRange', () => {
  const rows = [row(1000), row(2000), row(3000), row(4000)];

  it('covers the buckets from the start of the first to the end of the last, in either drag direction', () => {
    expect(selectionRange(rows, 1, 2, 1000, 4500)).toEqual({ fromMs: 2000, toMs: 4000 });
    expect(selectionRange(rows, 2, 1, 1000, 4500)).toEqual({ fromMs: 2000, toMs: 4000 });
  });

  it('ends a selection of the last bucket at the window end', () => {
    expect(selectionRange(rows, 3, 3, 1000, 4500)).toEqual({ fromMs: 4000, toMs: 4500 });
  });
});

describe('axisLabel', () => {
  it('shows the date only for a window longer than a day', () => {
    const ts = Date.UTC(2026, 9, 27, 14, 5);
    expect(axisLabel(ts, 60 * 60 * 1000, 'UTC')).toBe('14:05');
    expect(axisLabel(ts, 2 * 24 * 60 * 60 * 1000, 'UTC')).toBe('Oct 27 14:05');
  });
});
