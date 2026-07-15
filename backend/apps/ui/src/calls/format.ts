import dayjs from 'dayjs';

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

/** Timestamps display in the browser timezone (09 §2.3); the URL stays Unix ms. */
export function formatTs(tsMs: number): string {
  return dayjs(tsMs).format('YYYY-MM-DD HH:mm:ss.SSS');
}

export function formatTsShort(tsMs: number): string {
  return dayjs(tsMs).format('MM-DD HH:mm:ss');
}

/** Heat colour for the duration dot: green → yellow → orange → red. */
export function durationHeat(ms: number): string {
  if (ms < 100) return '#52c41a';
  if (ms < 1000) return '#faad14';
  if (ms < 3000) return '#fa8c16';
  return '#f5222d';
}
