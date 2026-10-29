import type { CallsFilter } from '../api/endpoints';
import { isRetentionClass } from '../api/types';
import type { RetentionClass } from '../api/types';
import { parseDurationMs, parseDurationRange } from '../calls/duration-filter';
import type { NamespaceNode } from '../discovery/group-pods';
import { ANY, NONE, regexLiteral } from './condition';
import type { Condition, Operator } from './condition';
import type { PodSelection } from './legacy';

// A backend without the filter model reports no `filter_capabilities` (02 §2.3.4) and takes only the legacy /calls
// parameters, so the applied conditions are translated to them (UI redesign plan §4.2.1). See calls/query-plan.ts.

/** The reason shown on a field, an operator, or a token that the backend cannot apply yet. */
export const UNSUPPORTED_REASON = 'The backend does not support this filter yet.';

const IDENTITY_FIELDS = ['namespace', 'service', 'pod'] as const;
type IdentityField = (typeof IDENTITY_FIELDS)[number];

function isIdentityField(field: string): field is IdentityField {
  return (IDENTITY_FIELDS as readonly string[]).includes(field);
}

/**
 * The field and operator pairs of the §4.2.1 table. A Map, because the field comes from the user: a plain object
 * would answer `constructor` or `toString` with an inherited member.
 */
const SUPPORTED: ReadonlyMap<string, readonly Operator[]> = new Map<string, readonly Operator[]>([
  ['namespace', ['=', 'in']],
  ['service', ['=', 'in']],
  ['pod', ['=', 'in']],
  ['duration', ['>', '>=', '<', '<=', 'between']],
  ['error', ['=']],
  ['retention_class', ['=', 'in']],
  ['method', ['=~']],
]);

/**
 * Why the filter bar disables a field or an operator, or null when it is enabled. Without `op`, a field is enabled
 * when at least one of its operators is.
 */
export function unsupportedReason(field: string, op?: Operator): string | null {
  const ops = SUPPORTED.get(field);
  if (ops === undefined) return UNSUPPORTED_REASON;
  return op === undefined || ops.includes(op) ? null : UNSUPPORTED_REASON;
}

/** The /calls parameters a set of conditions turns into; the window comes from the time range. */
export type TranslatedParams = Omit<CallsFilter, 'fromMs' | 'toMs' | 'limit'>;

export interface SkippedCondition {
  condition: Condition;
  reason: string;
}

export interface Translation {
  /** Null while a namespace, service, or pod condition waits for the /pods data. */
  params: TranslatedParams | null;
  /** The conditions that were not sent, each with the reason. */
  skipped: SkippedCondition[];
  /** True when the namespace, service, and pod conditions match no pod in the window. */
  noPods: boolean;
  /** True when the conditions exclude each other, such as two duration ranges that do not overlap. */
  contradiction: boolean;
}

interface PodTuple {
  tuple: string;
  namespace: string;
  service: string;
  pod: string;
}

function allPodTuples(namespaces: readonly NamespaceNode[]): PodTuple[] {
  return namespaces.flatMap((ns) =>
    ns.services.flatMap((svc) => svc.pods.map((p) => ({ tuple: p.tuple, namespace: ns.namespace, service: svc.service, pod: p.pod }))),
  );
}

/**
 * The pods of an old link's selection, as the old page sent them: its pods as they are, and every pod of its services
 * in the window. Null while services wait for the /pods data.
 */
function selectionTuples(selection: PodSelection, namespaces: readonly NamespaceNode[] | null): PodTuple[] | null {
  const pods = selection.pods.map((tuple) => {
    const [namespace, service, pod] = tuple.split('/') as [string, string, string];
    return { tuple, namespace, service, pod };
  });
  if (selection.services.length === 0) return pods;
  if (namespaces === null) return null;
  const seen = new Set(selection.pods);
  const fromServices = allPodTuples(namespaces).filter(
    (p) => selection.services.includes(`${p.namespace}/${p.service}`) && !seen.has(p.tuple),
  );
  return [...pods, ...fromServices];
}

/** Why one condition cannot be sent, or null when it can. */
function valueReason(c: Condition): string | null {
  if (unsupportedReason(c.field, c.op) !== null) return UNSUPPORTED_REASON;
  if (c.values.length === 0) return `The filter ${c.field} ${c.op} needs a value.`;
  if (c.values.some((v) => v === NONE || v === ANY)) return `The backend cannot match ${c.field} ${c.op} None or Any yet.`;
  if (c.field === 'error' && c.values[0] !== 'true') return 'Only error=true can be sent until the backend takes filters.';
  if (c.field === 'retention_class' && !c.values.every(isRetentionClass)) return UNSUPPORTED_REASON;
  if (c.field === 'method' && regexLiteral(c.values[0]!) === null) {
    return 'The backend matches a plain word in the method only; regular expressions come later.';
  }
  return null;
}

/**
 * The pod tuples of an old link's selection, for a request that sends its conditions as they are. Null while services
 * wait for the /pods data.
 */
export function selectionPods(selection: PodSelection, namespaces: readonly NamespaceNode[] | null): string[] | null {
  return selectionTuples(selection, namespaces)?.map((p) => p.tuple) ?? null;
}

/**
 * Turns the applied conditions into today's /calls parameters. Namespace, service, and pod conditions expand to pod
 * tuples from the /pods data, intersected across conditions and with the selection of an old link; duration bounds
 * intersect, and a strict bound moves by 1 ms; retention classes intersect. One method search can be sent.
 */
export function translateConditions(
  conditions: readonly Condition[],
  namespaces: readonly NamespaceNode[] | null,
  selection: PodSelection | null = null,
): Translation {
  const skipped: SkippedCondition[] = [];
  const params: TranslatedParams = {};
  const identity: Condition[] = [];
  let minMs: number | null = null;
  let maxMs: number | null = null;
  let classes: RetentionClass[] | null = null;
  const skip = (condition: Condition, reason: string): void => {
    skipped.push({ condition, reason });
  };

  for (const c of conditions) {
    const reason = valueReason(c);
    if (reason !== null) {
      skip(c, reason);
      continue;
    }
    const value = c.values[0]!;
    if (isIdentityField(c.field)) {
      identity.push(c);
    } else if (c.field === 'duration') {
      let lo: number | null = null;
      let hi: number | null = null;
      if (c.op === 'between') {
        const range = parseDurationRange(value);
        if (range !== null) [lo, hi] = [range.minMs, range.maxMs];
      } else {
        const ms = parseDurationMs(value);
        if (ms !== null) {
          if (c.op === '>') lo = ms + 1;
          if (c.op === '>=') lo = ms;
          if (c.op === '<') hi = ms - 1;
          if (c.op === '<=') hi = ms;
        }
      }
      // /calls reads 0 as "no bound", so an upper bound under 1 ms cannot be sent.
      if ((lo === null && hi === null) || (hi !== null && hi < 1)) {
        skip(c, 'This duration bound cannot be sent until the backend takes filters.');
        continue;
      }
      if (lo !== null) minMs = Math.max(minMs ?? 0, lo);
      if (hi !== null) maxMs = Math.min(maxMs ?? Number.MAX_SAFE_INTEGER, hi);
    } else if (c.field === 'error') {
      params.errorOnly = true;
    } else if (c.field === 'retention_class') {
      const values = c.values as RetentionClass[];
      classes = classes === null ? [...values] : classes.filter((v) => values.includes(v));
    } else if (c.field === 'method') {
      if (params.method !== undefined) {
        skip(c, 'Only one method search can be sent until the backend takes filters.');
        continue;
      }
      params.method = regexLiteral(value)!;
    }
  }

  if (minMs !== null && minMs > 0) params.durationMinMs = minMs;
  if (maxMs !== null) params.durationMaxMs = maxMs;
  if (classes !== null) params.retentionClasses = classes;

  const contradiction = (minMs !== null && maxMs !== null && minMs > maxMs) || (classes !== null && classes.length === 0);
  if (identity.length === 0 && selection === null) return { params, skipped, noPods: false, contradiction };
  const candidates = selection !== null ? selectionTuples(selection, namespaces) : namespaces && allPodTuples(namespaces);
  if (candidates === null) return { params: null, skipped, noPods: false, contradiction };
  const pods = candidates.filter((p) => identity.every((c) => c.values.includes(p[c.field as IdentityField]))).map((p) => p.tuple);
  return { params: { ...params, pods }, skipped, noPods: pods.length === 0, contradiction };
}
