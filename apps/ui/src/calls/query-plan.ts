import type { NamespaceNode } from '../discovery/group-pods';
import { isBrowserCondition } from '../filter/browser';
import type { Capabilities } from '../filter/capabilities';
import type { Condition } from '../filter/condition';
import { formatCondition } from '../filter/grammar';
import type { PodSelection } from '../filter/legacy';
import { selectionPods, translateConditions } from '../filter/translate';
import type { SkippedCondition, TranslatedParams } from '../filter/translate';
import { DEFAULT_ORDER, LEGACY_ORDERS, serverCanOrder } from './sort-order';
import type { CallsOrder } from './sort-order';

// How the calls page turns the applied conditions and "Sort by" into a /calls request. A backend with the filter
// model (it reports `filter_capabilities`, 02 §2.3.4) takes each condition unchanged as a `filter` parameter. An older
// one takes only the legacy parameters, so the conditions are translated (redesign plan §4.2.1).

export interface QueryPlan {
  /** The /calls parameters besides the window; null while the request cannot be built yet or must not be sent. */
  params: TranslatedParams | null;
  /** The conditions that are not sent, each with the reason. */
  skipped: SkippedCondition[];
  /** The conditions applied to the loaded rows in the browser instead. */
  browser: Condition[];
  /** True while the request waits for /pods. */
  waitingForPods: boolean;
  /** True when the request depends on /pods, so a /pods failure or partial answer affects it. */
  needsPods: boolean;
  /** Why no call can match, or null. */
  noMatch: string | null;
  /** True when /calls sorts by the order; otherwise the loaded rows are sorted in the browser. */
  serverOrdered: boolean;
  /** True when the request expands to pod tuples and needs the request-line guard. */
  expandsPods: boolean;
}

const NO_POD_MATCHES = 'No pod in this window matches the namespace, service, and pod filters.';

/** True when the request expands the selection's services, which takes the /pods data. */
const selectionNeedsPods = (selection: PodSelection | null): boolean => (selection?.services.length ?? 0) > 0;

/**
 * The plan with the filter model. A condition the build lacks is skipped, and the rest are sent unchanged. An old
 * link's selection has no text form, so it travels as the `pod` tuples /calls takes alongside `filter` (02 §2.3.3).
 */
function filterPlan(
  conditions: readonly Condition[],
  order: CallsOrder,
  caps: Capabilities,
  selection: PodSelection | null,
  namespaces: readonly NamespaceNode[] | null,
): QueryPlan {
  const skipped: SkippedCondition[] = [];
  const sent: string[] = [];
  for (const condition of conditions) {
    const reason = caps.conditionReason(condition);
    if (reason === null) sent.push(formatCondition(condition));
    else skipped.push({ condition, reason });
  }
  const serverOrdered = serverCanOrder(order, caps.orders);
  const params: TranslatedParams = { filters: sent };
  if (serverOrdered && order !== DEFAULT_ORDER) params.order = order;
  const plan: QueryPlan = {
    params,
    skipped,
    browser: [],
    waitingForPods: false,
    needsPods: selectionNeedsPods(selection),
    noMatch: null,
    serverOrdered,
    expandsPods: selection !== null,
  };
  if (selection === null) return plan;
  const pods = selectionPods(selection, namespaces);
  // An empty `pod` list would select every pod, so a selection with no pod in the window sends nothing.
  if (pods === null) return { ...plan, params: null, waitingForPods: true };
  if (pods.length === 0) return { ...plan, params: null, noMatch: NO_POD_MATCHES };
  return { ...plan, params: { ...params, pods } };
}

/** The plan for a backend without the filter model. */
function legacyPlan(
  conditions: readonly Condition[],
  order: CallsOrder,
  namespaces: readonly NamespaceNode[] | null,
  selection: PodSelection | null,
): QueryPlan {
  const t = translateConditions(conditions, namespaces, selection);
  const browser = conditions.filter(isBrowserCondition);
  const noMatch = t.contradiction
    ? 'The filters exclude each other, so no call can match.'
    : t.noPods
      ? NO_POD_MATCHES
      : null;
  return {
    params: t.noPods || t.contradiction ? null : t.params,
    skipped: t.skipped.filter((s) => !isBrowserCondition(s.condition)),
    browser,
    waitingForPods: t.params === null,
    needsPods: conditions.some((c) => ['namespace', 'service', 'pod'].includes(c.field)) || selectionNeedsPods(selection),
    noMatch,
    serverOrdered: serverCanOrder(order, LEGACY_ORDERS),
    expandsPods: true,
  };
}

/**
 * Plans the request. `caps` is undefined while /config loads, which holds the request back so that the first one
 * already has the right form; null means a backend without the filter model. `selection` is an old link's selection
 * (`CallsSearchState.selection`).
 */
export function planQuery(
  conditions: readonly Condition[],
  order: CallsOrder,
  caps: Capabilities | null | undefined,
  namespaces: readonly NamespaceNode[] | null,
  selection: PodSelection | null = null,
): QueryPlan {
  if (caps === undefined) {
    return {
      params: null,
      skipped: [],
      browser: [],
      waitingForPods: false,
      needsPods: false,
      noMatch: null,
      serverOrdered: true,
      expandsPods: false,
    };
  }
  return caps === null
    ? legacyPlan(conditions, order, namespaces, selection)
    : filterPlan(conditions, order, caps, selection, namespaces);
}
