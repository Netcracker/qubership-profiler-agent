import { describe, expect, it } from 'vitest';

import { BROWSER_ZONE } from '../controls/time-range';
import { durationHeat, formatStart, formatTs, formatTsShort } from './format';

// 2026-07-13 12:00:00.000 UTC.
const TS = Date.UTC(2026, 6, 13, 12, 0, 0);

describe('formatTs', () => {
  it('renders an explicit zone as wall-clock time there', () => {
    expect(formatTs(TS, 'UTC')).toBe('2026-07-13 12:00:00.000');
    expect(formatTsShort(TS, 'UTC')).toBe('07-13 12:00:00');
  });

  it('renders another zone at its own offset', () => {
    expect(formatTs(TS, 'Asia/Tokyo')).toBe('2026-07-13 21:00:00.000'); // UTC+9
  });

  it('defaults to the browser zone', () => {
    // Whatever the runner's zone is, the default matches the no-arg (browser) render.
    const browser = formatTs(TS, BROWSER_ZONE);
    expect(formatTs(TS)).toBe(browser);
  });
});

describe('formatStart', () => {
  // 2026-07-13 15:30:00.000 UTC.
  const NOW = Date.UTC(2026, 6, 13, 15, 30, 0);

  it('shows the time alone for a call of today', () => {
    expect(formatStart(Date.UTC(2026, 6, 13, 0, 0, 1, 250), 'UTC', NOW)).toBe('00:00:01.250');
  });

  it('shows the date and the time for a call of yesterday', () => {
    expect(formatStart(Date.UTC(2026, 6, 12, 23, 59, 59, 999), 'UTC', NOW)).toBe('Jul 12 23:59:59.999');
  });

  it('keeps the year for a call of another year', () => {
    expect(formatStart(Date.UTC(2025, 11, 31, 23, 0, 0), 'UTC', Date.UTC(2026, 0, 1, 12, 0, 0))).toBe(
      '2025-12-31 23:00:00.000',
    );
  });

  it('decides "today" in the display timezone', () => {
    // 22:00 UTC on July 12 is 07:00 on July 13 in Tokyo (UTC+9), where `now` is 00:30 on July 14.
    const call = Date.UTC(2026, 6, 12, 22, 0, 0);
    const now = Date.UTC(2026, 6, 13, 15, 30, 0);
    expect(formatStart(call, 'UTC', now)).toBe('Jul 12 22:00:00.000');
    expect(formatStart(call, 'Asia/Tokyo', now)).toBe('Jul 13 07:00:00.000');
    // Forty minutes earlier, `now` is 23:50 on July 13 in Tokyo, the call's day there.
    expect(formatStart(call, 'Asia/Tokyo', now - 40 * 60 * 1000)).toBe('07:00:00.000');
    // In UTC that `now` is still on July 13, a day after the call.
    expect(formatStart(call, 'UTC', now - 40 * 60 * 1000)).toBe('Jul 12 22:00:00.000');
  });
});

describe('durationHeat', () => {
  it.each([
    [0, 'ok'],
    [99, 'ok'],
    [100, 'warn'],
    [999, 'warn'],
    [1000, 'heat-mid'],
    [2999, 'heat-mid'],
    [3000, 'crit'],
    [120_000, 'crit'],
  ])('colors %i ms with --pf-color-%s', (ms, step) => {
    expect(durationHeat(ms)).toBe(`var(--pf-color-${step})`);
  });
});
