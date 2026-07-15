import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';

// Grafana-style time expressions for the period picker. A field holds either an
// absolute timestamp or a relative expression (`now`, `now-6h`); both resolve to
// an epoch-millis window on Apply, so the committed URL state stays absolute and
// nothing downstream has to learn about relative time.

dayjs.extend(customParseFormat);
dayjs.extend(utc);
dayjs.extend(timezone);

// Sentinel zone: interpret and render absolute fields in the browser's local
// zone. Any other value is an IANA name (`UTC`, `Europe/Moscow`, ...).
export const BROWSER_ZONE = '';

/** Committed window edited by the picker; `null` means "not set yet". */
export interface DraftWindow {
  fromMs: number | null;
  toMs: number | null;
}

/**
 * A time range as the URL holds it (Grafana-style): `from`/`to` are the raw
 * tokens — a relative expression (`now-3h`) or an epoch-ms integer string —
 * kept verbatim so a relative range stays live; `fromMs`/`toMs` are those tokens
 * resolved against the load's `now`.
 */
export interface TimeRange {
  from: string | null;
  to: string | null;
  fromMs: number | null;
  toMs: number | null;
}

const MS_TOKEN = /^\d+$/;

/** True for a relative expression (`now`, `now-6h`); false for an absolute token. */
export function isRelativeExpr(expr: string): boolean {
  return expr.trim().startsWith('now');
}

/** The URL token for an absolute instant is its epoch ms; a relative expr stays itself. */
export function toUrlToken(expr: string, ms: number): string {
  return isRelativeExpr(expr) ? expr.trim() : String(ms);
}

/** Resolve a URL token (relative expr or epoch-ms string) to epoch ms, or null. */
export function resolveUrlTime(token: string | null, nowMs: number): number | null {
  if (token === null) return null;
  const t = token.trim();
  if (t === '') return null;
  if (MS_TOKEN.test(t)) {
    const v = Number(t);
    return Number.isSafeInteger(v) && v >= 0 ? v : null;
  }
  return resolveTimeExpr(t, nowMs);
}

/** A relative range offered in the quick-ranges list, stored as expressions. */
export interface QuickRange {
  from: string;
  to: string;
  label: string;
}

export const QUICK_RANGES: readonly QuickRange[] = [
  { from: 'now-5m', to: 'now', label: 'Last 5 minutes' },
  { from: 'now-15m', to: 'now', label: 'Last 15 minutes' },
  { from: 'now-30m', to: 'now', label: 'Last 30 minutes' },
  { from: 'now-1h', to: 'now', label: 'Last 1 hour' },
  { from: 'now-3h', to: 'now', label: 'Last 3 hours' },
  { from: 'now-6h', to: 'now', label: 'Last 6 hours' },
  { from: 'now-12h', to: 'now', label: 'Last 12 hours' },
  { from: 'now-24h', to: 'now', label: 'Last 24 hours' },
  { from: 'now-2d', to: 'now', label: 'Last 2 days' },
  { from: 'now-7d', to: 'now', label: 'Last 7 days' },
  { from: 'now-30d', to: 'now', label: 'Last 30 days' },
];

export const ABSOLUTE_FORMAT = 'YYYY-MM-DD HH:mm:ss';

// Accepted absolute inputs, widest-first: a bare date means midnight, a missing
// seconds field means :00. Strict parsing (the third arg) keeps a stray relative
// expression from being coerced into a nonsensical date.
const ABSOLUTE_FORMATS = [ABSOLUTE_FORMAT, 'YYYY-MM-DD HH:mm', 'YYYY-MM-DD'];

const RELATIVE = /^now(?:([+-])(\d+)([smhdwMy]))?$/;

const UNITS: Record<string, dayjs.ManipulateType> = {
  s: 'second',
  m: 'minute',
  h: 'hour',
  d: 'day',
  w: 'week',
  M: 'month',
  y: 'year',
};

/**
 * Resolve a `From`/`To` field to epoch millis against `nowMs`, or `null` when the
 * text is neither a known absolute format nor a relative expression. Absolute
 * text is read as wall-clock time in `zone`; relative offsets are instant maths,
 * so they ignore the zone, and go through dayjs to stay calendar-correct.
 */
export function resolveTimeExpr(expr: string, nowMs: number, zone: string = BROWSER_ZONE): number | null {
  const trimmed = expr.trim();
  const relative = RELATIVE.exec(trimmed);
  if (relative !== null) {
    if (relative[1] === undefined) return nowMs; // bare `now`
    const amount = Number(relative[2]) * (relative[1] === '-' ? -1 : 1);
    const unit = UNITS[relative[3]!];
    return dayjs(nowMs).add(amount, unit).valueOf();
  }
  if (zone === BROWSER_ZONE) {
    const absolute = dayjs(trimmed, ABSOLUTE_FORMATS, true);
    return absolute.isValid() ? absolute.valueOf() : null;
  }
  // dayjs.tz has no strict-parse mode and throws (formatToParts on an invalid
  // date) for input that does not match — including an empty field — so screen
  // each format with a plain strict parse first. Accept a format only if it
  // reproduces the input exactly, or `2026-13-40` would silently roll over.
  for (const format of ABSOLUTE_FORMATS) {
    if (!dayjs(trimmed, format, true).isValid()) continue;
    const parsed = dayjs.tz(trimmed, format, zone);
    if (parsed.isValid() && parsed.format(format) === trimmed) return parsed.valueOf();
  }
  return null;
}

/** Render a committed bound back into an editable absolute field value in `zone`. */
export function exprFromMs(ms: number | null, zone: string = BROWSER_ZONE): string {
  if (ms === null) return '';
  return (zone === BROWSER_ZONE ? dayjs(ms) : dayjs(ms).tz(zone)).format(ABSOLUTE_FORMAT);
}

/** Resolve a quick range against `nowMs`; bounds are non-null for the built-in list. */
export function resolveQuickRange(range: QuickRange, nowMs: number): DraftWindow {
  return { fromMs: resolveTimeExpr(range.from, nowMs), toMs: resolveTimeExpr(range.to, nowMs) };
}

/**
 * Label for the picker trigger. Prefer the quick-range name when the live field
 * expressions still match one, so a `now-6h` pick reads "Last 6 hours" rather
 * than a frozen timestamp; otherwise fall back to the absolute window in `zone`.
 */
export function describeWindow(
  window: DraftWindow,
  fromExpr?: string,
  toExpr?: string,
  zone: string = BROWSER_ZONE,
): string {
  if (window.fromMs === null || window.toMs === null) return 'Select time range';
  if (fromExpr !== undefined && toExpr !== undefined) {
    const quick = QUICK_RANGES.find((r) => r.from === fromExpr && r.to === toExpr);
    if (quick !== undefined) return quick.label;
  }
  return `${exprFromMs(window.fromMs, zone)} to ${exprFromMs(window.toMs, zone)}`;
}

// Grafana's toolbar navigation. Shifting moves the window by a fraction of its
// own width; zooming scales it around its centre (factor > 1 zooms out, < 1
// zooms in). All operate on the instant window, so they are zone-independent.
export const SHIFT_FACTOR = 0.5;
export const ZOOM_OUT_FACTOR = 2;
export const ZOOM_IN_FACTOR = 0.5;

export function shiftWindow(window: DraftWindow, direction: 1 | -1, factor: number = SHIFT_FACTOR): DraftWindow {
  if (window.fromMs === null || window.toMs === null) return window;
  const delta = Math.round(direction * (window.toMs - window.fromMs) * factor);
  return { fromMs: window.fromMs + delta, toMs: window.toMs + delta };
}

export function zoomWindow(window: DraftWindow, factor: number): DraftWindow {
  if (window.fromMs === null || window.toMs === null) return window;
  const span = window.toMs - window.fromMs;
  const centre = window.fromMs + span / 2;
  const half = (span * factor) / 2;
  return { fromMs: Math.round(centre - half), toMs: Math.round(centre + half) };
}

export interface ZoneOption {
  value: string;
  label: string;
}

// Fallback when the runtime lacks Intl.supportedValuesOf (older engines).
const FALLBACK_ZONES = ['UTC', 'Europe/London', 'Europe/Moscow', 'America/New_York', 'Asia/Tokyo'];

/** The browser's own IANA zone name, e.g. `Europe/Moscow`. */
export function browserZoneName(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone;
}

/** Picker options: browser zone first, then UTC, then the full IANA list. */
export function zoneOptions(): ZoneOption[] {
  const supported =
    typeof Intl.supportedValuesOf === 'function' ? Intl.supportedValuesOf('timeZone') : FALLBACK_ZONES;
  const rest = supported.filter((z) => z !== 'UTC');
  return [
    { value: BROWSER_ZONE, label: `Browser time (${browserZoneName()})` },
    { value: 'UTC', label: 'UTC' },
    ...rest.map((z) => ({ value: z, label: z })),
  ];
}

/** The UTC offset of `zone` at `ms`, e.g. `UTC+03:00`. */
export function zoneOffsetLabel(zone: string, ms: number): string {
  const at = zone === BROWSER_ZONE ? dayjs(ms) : dayjs(ms).tz(zone);
  return `UTC${at.format('Z')}`;
}
