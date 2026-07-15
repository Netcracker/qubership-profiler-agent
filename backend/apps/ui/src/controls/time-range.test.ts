import dayjs from 'dayjs';
import { describe, expect, it } from 'vitest';

import {
  describeWindow,
  exprFromMs,
  resolveQuickRange,
  resolveTimeExpr,
  shiftWindow,
  zoneOffsetLabel,
  zoomWindow,
} from './time-range';

// `now` for the deterministic cases: 2026-07-13 12:00:00.000Z.
const NOW = Date.UTC(2026, 6, 13, 12, 0, 0);

describe('resolveTimeExpr', () => {
  it('resolves bare now to the reference instant', () => {
    expect(resolveTimeExpr('now', NOW)).toBe(NOW);
  });

  it('subtracts relative offsets', () => {
    expect(resolveTimeExpr('now-6h', NOW)).toBe(NOW - 6 * 3600_000);
    expect(resolveTimeExpr('now-15m', NOW)).toBe(NOW - 15 * 60_000);
  });

  it('adds relative offsets', () => {
    expect(resolveTimeExpr('now+1d', NOW)).toBe(NOW + 24 * 3600_000);
  });

  it('keeps month arithmetic calendar-correct rather than fixed-length', () => {
    // A fixed 30-day month would land on a different day; dayjs stays on the 13th.
    expect(resolveTimeExpr('now-1M', NOW)).toBe(dayjs(NOW).subtract(1, 'month').valueOf());
  });

  it('tolerates surrounding whitespace', () => {
    expect(resolveTimeExpr('  now-1h  ', NOW)).toBe(NOW - 3600_000);
  });

  it('parses absolute timestamps in the field format', () => {
    const ms = resolveTimeExpr('2026-07-13 12:00:00', NOW);
    expect(dayjs(ms!).format('YYYY-MM-DD HH:mm:ss')).toBe('2026-07-13 12:00:00');
  });

  it('accepts a bare date as midnight', () => {
    const ms = resolveTimeExpr('2026-07-13', NOW);
    expect(dayjs(ms!).format('YYYY-MM-DD HH:mm:ss')).toBe('2026-07-13 00:00:00');
  });

  it('rejects gibberish and malformed expressions', () => {
    expect(resolveTimeExpr('yesterday', NOW)).toBeNull();
    expect(resolveTimeExpr('now-6x', NOW)).toBeNull();
    expect(resolveTimeExpr('', NOW)).toBeNull();
  });
});

describe('exprFromMs', () => {
  it('renders a bound in the field format and round-trips through resolveTimeExpr', () => {
    const text = exprFromMs(NOW);
    expect(resolveTimeExpr(text, NOW)).toBe(NOW);
  });

  it('renders an unset bound as empty', () => {
    expect(exprFromMs(null)).toBe('');
  });
});

describe('resolveQuickRange', () => {
  it('turns a relative range into an absolute window', () => {
    expect(resolveQuickRange({ from: 'now-6h', to: 'now', label: 'Last 6 hours' }, NOW)).toEqual({
      fromMs: NOW - 6 * 3600_000,
      toMs: NOW,
    });
  });
});

describe('describeWindow', () => {
  it('reports the quick-range name when the live expressions still match one', () => {
    expect(describeWindow({ fromMs: NOW - 6 * 3600_000, toMs: NOW }, 'now-6h', 'now')).toBe('Last 6 hours');
  });

  it('falls back to the absolute window for custom expressions', () => {
    // Resolve then format in the same local zone so the round-trip is tz-agnostic.
    const from = resolveTimeExpr('2026-07-13 08:00:00', NOW)!;
    const to = resolveTimeExpr('2026-07-13 09:30:00', NOW)!;
    expect(describeWindow({ fromMs: from, toMs: to }, '2026-07-13 08:00:00', '2026-07-13 09:30:00')).toBe(
      '2026-07-13 08:00:00 to 2026-07-13 09:30:00',
    );
  });

  it('prompts when the window is unset', () => {
    expect(describeWindow({ fromMs: null, toMs: null })).toBe('Select time range');
  });
});

describe('time zones', () => {
  it('reads an absolute field as wall-clock time in the given zone', () => {
    // NOW is exactly 2026-07-13 12:00:00 UTC, so the UTC wall-clock text resolves back to it.
    expect(resolveTimeExpr('2026-07-13 12:00:00', NOW, 'UTC')).toBe(NOW);
  });

  it('renders and round-trips a bound in a named zone', () => {
    expect(exprFromMs(NOW, 'UTC')).toBe('2026-07-13 12:00:00');
    expect(resolveTimeExpr(exprFromMs(NOW, 'UTC'), NOW, 'UTC')).toBe(NOW);
  });

  it('returns null (never throws) for empty or malformed input in a named zone', () => {
    // dayjs.tz throws on an invalid date; an empty field must not crash the picker.
    expect(resolveTimeExpr('', NOW, 'UTC')).toBeNull();
    expect(resolveTimeExpr('   ', NOW, 'Asia/Tokyo')).toBeNull();
    expect(resolveTimeExpr('not a date', NOW, 'Asia/Tokyo')).toBeNull();
    expect(resolveTimeExpr('2026-13-40 09:00:00', NOW, 'UTC')).toBeNull();
  });

  it('keeps relative expressions zone-independent', () => {
    expect(resolveTimeExpr('now-6h', NOW, 'UTC')).toBe(resolveTimeExpr('now-6h', NOW, 'Asia/Tokyo'));
  });

  it('reports the zone offset', () => {
    expect(zoneOffsetLabel('UTC', NOW)).toBe('UTC+00:00');
  });
});

describe('shiftWindow', () => {
  it('moves the window back by half its width', () => {
    expect(shiftWindow({ fromMs: 1000, toMs: 3000 }, -1)).toEqual({ fromMs: 0, toMs: 2000 });
  });

  it('moves the window forward by half its width', () => {
    expect(shiftWindow({ fromMs: 1000, toMs: 3000 }, 1)).toEqual({ fromMs: 2000, toMs: 4000 });
  });

  it('leaves an unset window untouched', () => {
    expect(shiftWindow({ fromMs: null, toMs: null }, -1)).toEqual({ fromMs: null, toMs: null });
  });
});

describe('zoomWindow', () => {
  it('zooms out around the centre', () => {
    expect(zoomWindow({ fromMs: 1000, toMs: 3000 }, 2)).toEqual({ fromMs: 0, toMs: 4000 });
  });

  it('zooms in around the centre', () => {
    expect(zoomWindow({ fromMs: 1000, toMs: 3000 }, 0.5)).toEqual({ fromMs: 1500, toMs: 2500 });
  });
});
