import dayjs from 'dayjs';

import { BROWSER_ZONE } from '../controls/time-range';
import type { StatsRow } from '../api/types';

// The arithmetic of the calls histogram, apart from the DOM: the y-axis top, which bucket a pointer is over, and
// the time range a selection covers.

/** The y-axis top: the smallest 1, 2, or 5 times a power of ten that is at least `max`. */
export function niceMax(max: number): number {
  if (max <= 1) return 1;
  const power = 10 ** Math.floor(Math.log10(max));
  for (const step of [1, 2, 5, 10]) {
    if (step * power >= max) return step * power;
  }
  return 10 * power;
}

/** The bucket under an x offset into a plot `width` pixels wide with `n` buckets, clamped to the plot. */
export function bucketAt(x: number, width: number, n: number): number {
  if (width <= 0 || n <= 0) return 0;
  return Math.min(n - 1, Math.max(0, Math.floor((x / width) * n)));
}

/** The start of a bucket in Unix ms, from its row key. */
export function bucketStart(row: StatsRow): number {
  return Number(row.key);
}

/**
 * The time range buckets `a` to `b`, in either order, cover: from the start of the first to the end of the last. The
 * last bucket of the window ends at `toMs`, not a whole bucket later (02 §2.9).
 */
export function selectionRange(
  rows: readonly StatsRow[],
  a: number,
  b: number,
  bucketMs: number,
  toMs: number,
): { fromMs: number; toMs: number } {
  const [lo, hi] = a <= b ? [a, b] : [b, a];
  return { fromMs: bucketStart(rows[lo]!), toMs: Math.min(toMs, bucketStart(rows[hi]!) + bucketMs) };
}

/** An axis label: the time of day, with the date when the window spans more than a day. */
export function axisLabel(tsMs: number, spanMs: number, zone: string = BROWSER_ZONE): string {
  const t = zone === BROWSER_ZONE ? dayjs(tsMs) : dayjs(tsMs).tz(zone);
  return t.format(spanMs > 24 * 60 * 60 * 1000 ? 'MMM D HH:mm' : 'HH:mm');
}
