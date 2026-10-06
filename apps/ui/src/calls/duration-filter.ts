// Duration filter expression, ported from the old UI's Duration$parse
// (profiler-ui/src/profiler.mjs:665-701). Accepts a lower bound (`>400ms`,
// `400ms`, `>=400ms`), an upper bound (`<100ms`, `<=100ms`), an exact match
// (`=500ms`), or a range (`100ms..200ms`). A bare number without a unit is
// seconds, the same as the old UI, so `3` and `3s` both mean 3000 ms.
//
// The bounds map straight onto the backend's duration_min_ms / duration_max_ms
// query params (02 §2.3), which already accept both ends — so a max or a range
// needs no backend change, only this parse and the URL state that carries it.

export interface DurationBound {
  /** Inclusive lower bound in ms, or null for "no minimum". */
  minMs: number | null;
  /** Upper bound in ms, or null for "no maximum". */
  maxMs: number | null;
}

const SIMPLE = /^(>=|<=|>|<|=)?(\d+(?:\.\d+)?)(s|ms)?$/;
const RANGE = /^(\d+(?:\.\d+)?)(s|ms)?\.{2,}(\d+(?:\.\d+)?)(s|ms)?$/;

/** ms when the unit is `ms`, else seconds (bare number or `s`), rounded to whole ms. */
function toMs(value: number, unit: string | undefined): number {
  return Math.round(unit === 'ms' ? value : value * 1000);
}

/**
 * Parses a duration filter expression into its bounds. Returns `{minMs: null,
 * maxMs: null}` for an empty string ("no filter") and `null` for anything the
 * grammar does not accept, so a caller can flag the input as invalid.
 */
export function parseDurationFilter(raw: string): DurationBound | null {
  const val = raw.replace(/\s+/g, '');
  if (val === '') return { minMs: null, maxMs: null };

  const simple = SIMPLE.exec(val);
  if (simple !== null) {
    const ms = toMs(Number(simple[2]), simple[3]);
    const sign = simple[1];
    if (sign === '=') return { minMs: ms, maxMs: ms };
    if (sign === '<' || sign === '<=') return { minMs: null, maxMs: ms };
    return { minMs: ms, maxMs: null }; // no sign, '>', or '>=' — a lower bound
  }

  const range = RANGE.exec(val);
  if (range !== null) {
    let a = toMs(Number(range[1]), range[2]);
    let b = toMs(Number(range[3]), range[4]);
    if (a > b) [a, b] = [b, a];
    return { minMs: a, maxMs: b };
  }

  return null;
}

/** ms as `500ms` or `1.5s` — whole seconds stay in seconds, the rest in ms. */
function formatMs(ms: number): string {
  return ms >= 1000 && ms % 1000 === 0 ? `${ms / 1000}s` : `${ms}ms`;
}

/**
 * Renders bounds back to an expression, the inverse of parseDurationFilter, so
 * a preset click or a shared URL can fill the text field with editable text.
 */
export function formatDurationFilter(bound: DurationBound): string {
  const { minMs, maxMs } = bound;
  if (minMs === null && maxMs === null) return '';
  if (maxMs === null) return `>${formatMs(minMs!)}`;
  if (minMs === null) return `<${formatMs(maxMs)}`;
  if (minMs === maxMs) return `=${formatMs(minMs)}`;
  return `${formatMs(minMs)}..${formatMs(maxMs)}`;
}
