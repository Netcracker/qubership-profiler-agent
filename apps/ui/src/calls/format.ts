import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';

import { BROWSER_ZONE } from '../controls/time-range';

dayjs.extend(utc);
dayjs.extend(timezone);

/** 412ms · 4.6s · 3m 12s · 1h 04m — compact, no sub-ms noise. */
export function formatDurationMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3_600_000) {
    const m = Math.floor(ms / 60_000);
    const s = Math.round((ms % 60_000) / 1000);
    return `${m}m ${s}s`;
  }
  const h = Math.floor(ms / 3_600_000);
  const m = Math.round((ms % 3_600_000) / 60_000);
  return `${h}h ${String(m).padStart(2, '0')}m`;
}

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MiB`;
  return `${(n / 1024 ** 3).toFixed(2)} GiB`;
}

export function formatCount(n: number): string {
  return n.toLocaleString('en-US');
}

/** Render a timestamp in the display timezone (default: the browser's); the URL stays Unix ms. */
export function formatTs(tsMs: number, zone: string = BROWSER_ZONE): string {
  return (zone === BROWSER_ZONE ? dayjs(tsMs) : dayjs(tsMs).tz(zone)).format('YYYY-MM-DD HH:mm:ss.SSS');
}

export function formatTsShort(tsMs: number, zone: string = BROWSER_ZONE): string {
  return (zone === BROWSER_ZONE ? dayjs(tsMs) : dayjs(tsMs).tz(zone)).format('MM-DD HH:mm:ss');
}

/**
 * The color of the duration heat dot: under 100 ms, under 1 s, under 3 s, and 3 s or more. Each step is a theme
 * variable from theme/theme.css, so the dot follows the light and dark themes.
 */
export function durationHeat(ms: number): string {
  if (ms < 100) return 'var(--pf-color-ok)';
  if (ms < 1000) return 'var(--pf-color-warn)';
  if (ms < 3000) return 'var(--pf-color-heat-mid)';
  return 'var(--pf-color-crit)';
}
