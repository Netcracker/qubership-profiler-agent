import { formatMs } from '../calls/duration-filter';
import { isRetentionClass } from '../api/types';
import { escapeRegex } from './condition';
import type { Condition } from './condition';

// Links made before the filter bar carry `service`, `pod`, `duration_min_ms`, `duration_max_ms`, `error_only`,
// `retention_class`, and `q`. They are read and turned into conditions, and never written again (UI redesign plan
// §4.2).

/** The URL parameters of the old calls page. */
export const LEGACY_PARAMS = ['service', 'pod', 'duration_min_ms', 'duration_max_ms', 'error_only', 'retention_class', 'q'];

/** The lower duration bound the old page applied when `duration_min_ms` was absent. */
const LEGACY_DEFAULT_MIN_MS = 500;

export function hasLegacyParams(sp: URLSearchParams): boolean {
  return LEGACY_PARAMS.some((name) => sp.has(name));
}

function eqOrIn(field: string, values: readonly string[]): Condition[] {
  const unique = [...new Set(values)];
  if (unique.length === 0) return [];
  return [{ field, op: unique.length === 1 ? '=' : 'in', values: unique }];
}

function readMs(raw: string | null): number | null {
  if (raw === null || raw === '') return null;
  const v = Number(raw);
  return Number.isSafeInteger(v) && v >= 0 ? v : null;
}

/**
 * Converts the old parameters to conditions. The old page took the union of the selected services and pods; the
 * conditions take the intersection. A pod pinned inside a selected service, the usual case, narrows to that pod as
 * the old page did. A pod of another service drops that service from the result. Services from several namespaces
 * widen to every pair of those namespaces and services.
 */
export function conditionsFromLegacyParams(sp: URLSearchParams): Condition[] {
  const services = sp.getAll('service').map((s) => s.split('/')).filter((p) => p.length === 2);
  const pods = sp.getAll('pod').map((s) => s.split('/')).filter((p) => p.length === 3);
  const out: Condition[] = [
    ...eqOrIn('namespace', [...services, ...pods].map((p) => p[0]!)),
    ...eqOrIn('service', [...services, ...pods].map((p) => p[1]!)),
    ...eqOrIn('pod', pods.map((p) => p[2]!)),
  ];

  // The old page sent its 500 ms default when the parameter was absent; 0 meant no lower bound.
  const minMs = readMs(sp.get('duration_min_ms')) ?? LEGACY_DEFAULT_MIN_MS;
  const maxMs = readMs(sp.get('duration_max_ms')) ?? 0;
  if (minMs > 0 && maxMs > 0) {
    out.push({ field: 'duration', op: 'between', values: [`${formatMs(minMs)}..${formatMs(maxMs)}`] });
  } else if (minMs > 0) {
    out.push({ field: 'duration', op: '>=', values: [formatMs(minMs)] });
  } else if (maxMs > 0) {
    out.push({ field: 'duration', op: '<=', values: [formatMs(maxMs)] });
  }

  if (sp.get('error_only') === 'true') out.push({ field: 'error', op: '=', values: ['true'] });
  out.push(...eqOrIn('retention_class', sp.getAll('retention_class').filter(isRetentionClass)));
  const q = (sp.get('q') ?? '').trim();
  // `q` was a substring match; quoting it keeps that meaning under `=~`.
  if (q !== '') out.push({ field: 'method', op: '=~', values: [escapeRegex(q)] });
  return out;
}
