import { isRelativeExpr, resolveUrlTime } from '../controls/time-range';
import { isRetentionClass } from '../api/types';
import type { RetentionClass } from '../api/types';

// The URL is the source of truth (09 §1, §6): the window, selection, and
// filters live in the query string, so a shared link reopens the same view.
// The pagination cursor never enters the URL — it is transient scroll state.

/** Default duration chip: page one must not be sub-millisecond noise (09 §2.3). */
export const DEFAULT_DURATION_MIN_MS = 500;

/**
 * Default window tokens for a screen that opts into one via `defaultWindow`: the
 * last hour, Grafana-style. Calls lands on recent data instead of an empty
 * prompt, and the range stays live because the tokens are relative.
 */
export const DEFAULT_FROM = 'now-1h';
export const DEFAULT_TO = 'now';

/** /ui/calls and /ui/pods query state (09 §6). */
export interface CallsSearchState {
  /**
   * Window tokens as the URL holds them, Grafana-style: a relative expression
   * (`now-3h`) that stays live, or an epoch-ms string; null until a period is
   * applied. `fromMs`/`toMs` are these resolved against the load's `now`.
   */
  from: string | null;
  to: string | null;
  fromMs: number | null;
  toMs: number | null;
  /** Selected services, `namespace/service`. */
  services: string[];
  /** Individually selected pods, `namespace/service/pod`. */
  pods: string[];
  /** Lower duration bound in ms; 0 means "no minimum". */
  durationMinMs: number;
  /** Upper duration bound in ms; 0 means "no maximum" (09 §2.3, old-UI grammar). */
  durationMaxMs: number;
  errorOnly: boolean;
  retentionClasses: RetentionClass[];
  /** Method substring query (the future `$param=` language is 08 R3). */
  query: string;
  /** Hide system/proxy noise (client-side idle-tag filter); default on. */
  hideSystem: boolean;
}

export const EMPTY_CALLS_SEARCH: CallsSearchState = {
  from: null,
  to: null,
  fromMs: null,
  toMs: null,
  services: [],
  pods: [],
  durationMinMs: DEFAULT_DURATION_MIN_MS,
  durationMaxMs: 0,
  errorOnly: false,
  retentionClasses: [],
  query: '',
  hideSystem: true,
};

function parseMs(raw: string | null): number | null {
  if (raw === null || raw === '') return null;
  const v = Number(raw);
  return Number.isSafeInteger(v) && v >= 0 ? v : null;
}

/** Keep a window token only when it resolves; drop garbage to null. */
function parseWindowToken(raw: string | null, nowMs: number): string | null {
  if (raw === null || raw === '') return null;
  const token = raw.trim();
  return resolveUrlTime(token, nowMs) === null ? null : token;
}

/** Parse knobs a screen can opt into. */
export interface ParseCallsOptions {
  /**
   * Fill an absent or unparseable window with the default last hour so the
   * screen opens on recent data. Calls opts in; Pods keeps its Apply-gated
   * "pick a period" prompt.
   */
  defaultWindow?: boolean;
}

/** `nowMs` anchors relative tokens; pass a per-load value so resolution is stable. */
export function parseCallsSearch(
  sp: URLSearchParams,
  nowMs: number = Date.now(),
  opts: ParseCallsOptions = {},
): CallsSearchState {
  const durationRaw = parseMs(sp.get('duration_min_ms'));
  const from = parseWindowToken(sp.get('from'), nowMs) ?? (opts.defaultWindow ? DEFAULT_FROM : null);
  const to = parseWindowToken(sp.get('to'), nowMs) ?? (opts.defaultWindow ? DEFAULT_TO : null);
  return {
    from,
    to,
    fromMs: resolveUrlTime(from, nowMs),
    toMs: resolveUrlTime(to, nowMs),
    services: sp.getAll('service').filter((s) => s.split('/').length === 2),
    pods: sp.getAll('pod').filter((s) => s.split('/').length === 3),
    durationMinMs: durationRaw ?? DEFAULT_DURATION_MIN_MS,
    durationMaxMs: parseMs(sp.get('duration_max_ms')) ?? 0,
    errorOnly: sp.get('error_only') === 'true',
    retentionClasses: sp.getAll('retention_class').filter(isRetentionClass),
    query: sp.get('q') ?? '',
    hideSystem: sp.get('hide_system') !== 'false',
  };
}

/** True when a window bound is a live relative expression — a permalink candidate. */
export function hasRelativeWindow(state: CallsSearchState): boolean {
  return (state.from !== null && isRelativeExpr(state.from)) || (state.to !== null && isRelativeExpr(state.to));
}

/**
 * Pin the resolved window into the tokens, turning a live relative range into a
 * permalink — the `y`-hotkey equivalent of GitHub's branch → commit-SHA link.
 */
export function freezeWindow(state: CallsSearchState): CallsSearchState {
  return {
    ...state,
    from: state.fromMs === null ? state.from : String(state.fromMs),
    to: state.toMs === null ? state.to : String(state.toMs),
  };
}

/** Inverse of parseCallsSearch; omits values equal to the defaults. */
export function callsSearchToParams(state: CallsSearchState): URLSearchParams {
  const sp = new URLSearchParams();
  if (state.from !== null) sp.set('from', state.from);
  if (state.to !== null) sp.set('to', state.to);
  for (const s of state.services) sp.append('service', s);
  for (const p of state.pods) sp.append('pod', p);
  if (state.durationMinMs !== DEFAULT_DURATION_MIN_MS) {
    sp.set('duration_min_ms', String(state.durationMinMs));
  }
  if (state.durationMaxMs !== 0) sp.set('duration_max_ms', String(state.durationMaxMs));
  if (state.errorOnly) sp.set('error_only', 'true');
  for (const c of state.retentionClasses) sp.append('retention_class', c);
  if (state.query !== '') sp.set('q', state.query);
  if (!state.hideSystem) sp.set('hide_system', 'false');
  return sp;
}

/** /ui/tree/:pk query hints (02 §2.2 cold lookup). */
export interface TreeSearchState {
  tsMs: number | null;
  retentionClass: RetentionClass | null;
}

export function parseTreeSearch(sp: URLSearchParams): TreeSearchState {
  const classRaw = sp.get('retention_class');
  return {
    tsMs: parseMs(sp.get('ts_ms')),
    retentionClass: classRaw !== null && isRetentionClass(classRaw) ? classRaw : null,
  };
}

/** Builds the {base}tree/:pk href a calls row opens in a new tab (09 §2.3); base is the build-time UI base. */
export function treeHref(pkPath: string, tsMs: number, retentionClass: RetentionClass): string {
  const sp = new URLSearchParams({ ts_ms: String(tsMs), retention_class: retentionClass });
  return `${import.meta.env.BASE_URL}tree/${encodeURIComponent(pkPath)}?${sp.toString()}`;
}
