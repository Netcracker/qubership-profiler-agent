import { isRetentionClass } from '../api/types';
import type { RetentionClass } from '../api/types';

// The URL is the source of truth (09 §1, §6): the window, selection, and
// filters live in the query string, so a shared link reopens the same view.
// The pagination cursor never enters the URL — it is transient scroll state.

/** Default duration chip: page one must not be sub-millisecond noise (09 §2.3). */
export const DEFAULT_DURATION_MIN_MS = 500;

/** /ui/calls and /ui/pods query state (09 §6). */
export interface CallsSearchState {
  /** Frozen window, Unix ms; null until the user applies a period. */
  fromMs: number | null;
  toMs: number | null;
  /** Selected services, `namespace/service`. */
  services: string[];
  /** Individually selected pods, `namespace/service/pod`. */
  pods: string[];
  /** Duration chip; 0 means "All". */
  durationMinMs: number;
  errorOnly: boolean;
  retentionClasses: RetentionClass[];
  /** Method substring query (the future `$param=` language is 08 R3). */
  query: string;
  /** Hide system/proxy noise (client-side idle-tag filter); default on. */
  hideSystem: boolean;
}

export const EMPTY_CALLS_SEARCH: CallsSearchState = {
  fromMs: null,
  toMs: null,
  services: [],
  pods: [],
  durationMinMs: DEFAULT_DURATION_MIN_MS,
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

export function parseCallsSearch(sp: URLSearchParams): CallsSearchState {
  const durationRaw = parseMs(sp.get('duration_min_ms'));
  return {
    fromMs: parseMs(sp.get('from')),
    toMs: parseMs(sp.get('to')),
    services: sp.getAll('service').filter((s) => s.split('/').length === 2),
    pods: sp.getAll('pod').filter((s) => s.split('/').length === 3),
    durationMinMs: durationRaw ?? DEFAULT_DURATION_MIN_MS,
    errorOnly: sp.get('error_only') === 'true',
    retentionClasses: sp.getAll('retention_class').filter(isRetentionClass),
    query: sp.get('q') ?? '',
    hideSystem: sp.get('hide_system') !== 'false',
  };
}

/** Inverse of parseCallsSearch; omits values equal to the defaults. */
export function callsSearchToParams(state: CallsSearchState): URLSearchParams {
  const sp = new URLSearchParams();
  if (state.fromMs !== null) sp.set('from', String(state.fromMs));
  if (state.toMs !== null) sp.set('to', String(state.toMs));
  for (const s of state.services) sp.append('service', s);
  for (const p of state.pods) sp.append('pod', p);
  if (state.durationMinMs !== DEFAULT_DURATION_MIN_MS) {
    sp.set('duration_min_ms', String(state.durationMinMs));
  }
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

/** Builds the /ui/tree/:pk href a calls row opens in a new tab (09 §2.3). */
export function treeHref(pkPath: string, tsMs: number, retentionClass: RetentionClass): string {
  const sp = new URLSearchParams({ ts_ms: String(tsMs), retention_class: retentionClass });
  return `/ui/tree/${encodeURIComponent(pkPath)}?${sp.toString()}`;
}
