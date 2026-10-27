import { isRelativeExpr, resolveUrlTime } from '../controls/time-range';
import { isRetentionClass } from '../api/types';
import { DEFAULT_ORDER, isCallsOrder } from '../calls/sort-order';
import type { CallsOrder } from '../calls/sort-order';
import type { RetentionClass } from '../api/types';
import type { Condition } from '../filter/condition';
import { formatCondition, parseCondition } from '../filter/grammar';
import { conditionsFromLegacyParams, hasLegacyParams } from '../filter/legacy';

// The URL is the source of truth (09 §1, §6): the window and the applied filters live in the query string, so a
// shared link reopens the same view. The pagination cursor never enters the URL — it is transient scroll state.

/**
 * Default window tokens for a screen that opts into `defaults`: the last hour,
 * Grafana-style. Calls lands on recent data instead of an empty prompt, and
 * the range stays live because the tokens are relative.
 */
export const DEFAULT_FROM = 'now-1h';
export const DEFAULT_TO = 'now';

/** The filter of a visit with no query string: page one must not be sub-millisecond noise (07 §11). */
export const DEFAULT_CONDITIONS: readonly Condition[] = [{ field: 'duration', op: '>', values: ['500ms'] }];

/** /ui/calls and /ui/pods query state (09 §6). Both screens read and write the same scheme. */
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
  /** The applied conditions, one repeatable `filter` parameter each. */
  conditions: Condition[];
  /** Hide system/proxy noise. A UI flag: the loaded rows are filtered in the browser (redesign plan §4.6). */
  hideSystem: boolean;
  /** "Sort by", `order=<field>_<asc|desc>`. Kept in the URL even while the browser does the sorting. */
  order: CallsOrder;
}

export const EMPTY_CALLS_SEARCH: CallsSearchState = {
  from: null,
  to: null,
  fromMs: null,
  toMs: null,
  conditions: [],
  hideSystem: false,
  order: DEFAULT_ORDER,
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
   * Fill an absent or unparseable window with the last hour, so the screen
   * opens on recent data, and give a visit with no query string the default
   * filter and "Hide system/proxy". Calls opts in; Pods keeps its "pick a
   * period" prompt.
   */
  defaults?: boolean;
}

function parseFlag(raw: string | null): boolean | null {
  if (raw === '1' || raw === 'true') return true;
  if (raw === '0' || raw === 'false') return false;
  return null;
}

/** An absent or unknown `order` falls back to start time, newest first. */
function parseOrder(raw: string | null): CallsOrder {
  return raw !== null && isCallsOrder(raw) ? raw : DEFAULT_ORDER;
}

/**
 * Reads the calls query state. A `filter` parameter that does not parse is dropped. A link without `filter` and with
 * the parameters of the old calls page is converted, with that page's own defaults. `nowMs` anchors relative tokens;
 * pass a per-load value so resolution is stable.
 */
export function parseCallsSearch(
  sp: URLSearchParams,
  nowMs: number = Date.now(),
  opts: ParseCallsOptions = {},
): CallsSearchState {
  const bare = sp.size === 0;
  const legacy = !sp.has('filter') && hasLegacyParams(sp);
  const from = parseWindowToken(sp.get('from'), nowMs) ?? (opts.defaults ? DEFAULT_FROM : null);
  const to = parseWindowToken(sp.get('to'), nowMs) ?? (opts.defaults ? DEFAULT_TO : null);

  let conditions: Condition[];
  if (legacy) conditions = conditionsFromLegacyParams(sp);
  else if (bare && opts.defaults) conditions = [...DEFAULT_CONDITIONS];
  else {
    conditions = sp.getAll('filter').flatMap((text) => {
      const r = parseCondition(text);
      return r.ok ? [r.condition] : [];
    });
  }

  // The old page hid system calls unless told not to; the new URL writes the flag whenever it is on.
  const hideDefault = legacy || (bare && opts.defaults === true);
  return {
    from,
    to,
    fromMs: resolveUrlTime(from, nowMs),
    toMs: resolveUrlTime(to, nowMs),
    conditions,
    hideSystem: parseFlag(sp.get('hide_system')) ?? hideDefault,
    order: parseOrder(sp.get('order')),
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

/**
 * Inverse of parseCallsSearch. The state is written in full, with the API's own parameter names (redesign plan
 * §4.2); the old parameters are never written.
 */
export function callsSearchToParams(state: CallsSearchState): URLSearchParams {
  const sp = new URLSearchParams();
  if (state.from !== null) sp.set('from', state.from);
  if (state.to !== null) sp.set('to', state.to);
  for (const c of state.conditions) sp.append('filter', formatCondition(c));
  sp.set('order', state.order);
  if (state.hideSystem) sp.set('hide_system', '1');
  return sp;
}

/** The fixed tabs of the tree page, in display order. The first one is the default. */
export const TREE_TABS = ['tree', 'flame', 'hotspots', 'params'] as const;

export type TreeTab = (typeof TREE_TABS)[number];

export function isTreeTab(s: string): s is TreeTab {
  return (TREE_TABS as readonly string[]).includes(s);
}

/** /ui/tree/:pk query state: the cold-lookup hints (02 §2.2) and the open fixed tab (09 §6). */
export interface TreeSearchState {
  tsMs: number | null;
  retentionClass: RetentionClass | null;
  /** A missing or unknown `tab` opens Call Tree. Derived tabs are browser state and never get here. */
  tab: TreeTab;
}

export function parseTreeSearch(sp: URLSearchParams): TreeSearchState {
  const classRaw = sp.get('retention_class');
  const tabRaw = sp.get('tab');
  return {
    tsMs: parseMs(sp.get('ts_ms')),
    retentionClass: classRaw !== null && isRetentionClass(classRaw) ? classRaw : null,
    tab: tabRaw !== null && isTreeTab(tabRaw) ? tabRaw : 'tree',
  };
}

/** Sets `tab` in a copy of the tree query string. The default tab is left out, so a plain link stays short. */
export function withTreeTab(sp: URLSearchParams, tab: TreeTab): URLSearchParams {
  const next = new URLSearchParams(sp);
  if (tab === 'tree') next.delete('tab');
  else next.set('tab', tab);
  return next;
}

/** Builds the {base}tree/:pk href a calls row opens in a new tab (09 §2.3); base is the build-time UI base. */
export function treeHref(pkPath: string, tsMs: number, retentionClass: RetentionClass, tab: TreeTab = 'tree'): string {
  const sp = withTreeTab(new URLSearchParams({ ts_ms: String(tsMs), retention_class: retentionClass }), tab);
  return `${import.meta.env.BASE_URL}tree/${encodeURIComponent(pkPath)}?${sp.toString()}`;
}
