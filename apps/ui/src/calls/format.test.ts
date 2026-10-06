import { describe, expect, it } from 'vitest';

import { BROWSER_ZONE } from '../controls/time-range';
import { formatTs, formatTsShort } from './format';

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
