import { formatMs } from '../calls/duration-filter';
import { isRetentionClass } from '../api/types';
import { escapeRegex } from './condition';
import type { Condition } from './condition';

// Links made before the filter bar carry `service`, `pod`, `duration_min_ms`, `duration_max_ms`, `error_only`,
// `retention_class`, and `q`. They are read and turned into conditions (UI redesign plan §4.2). The old page took the
// union of its services and pods, which conditions, all of which must match, cannot always express; such a selection
// is kept, and only `service` and `pod` are written again, for it.

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
 * The services (`namespace/service`) and pods (`namespace/service/pod`) of the old calls page. A call of any of them
 * matches: the union of the services and the pods.
 */
export interface PodSelection {
  services: readonly string[];
  pods: readonly string[];
}

/** Reads the `service` and `pod` parameters; null when neither names anything. */
export function readPodSelection(sp: URLSearchParams): PodSelection | null {
  const services = [...new Set(sp.getAll('service').filter((s) => s.split('/').length === 2))];
  const pods = [...new Set(sp.getAll('pod').filter((s) => s.split('/').length === 3))];
  return services.length === 0 && pods.length === 0 ? null : { services, pods };
}

/** Writes a selection back as the `service` and `pod` parameters. */
export function writePodSelection(sp: URLSearchParams, selection: PodSelection | null): void {
  for (const s of selection?.services ?? []) sp.append('service', s);
  for (const p of selection?.pods ?? []) sp.append('pod', p);
}

/**
 * The conditions that select exactly the keys, or null when they cannot. The conditions match every combination of
 * their values, so they are exact only when the keys are all those combinations, such as the services of one
 * namespace, or one service in two namespaces.
 */
function exactConditions(keys: readonly string[], fields: readonly string[]): Condition[] | null {
  const parts = keys.map((k) => k.split('/'));
  const columns = fields.map((_, i) => [...new Set(parts.map((p) => p[i]!))]);
  const combinations = columns.reduce((n, values) => n * values.length, 1);
  return combinations === keys.length ? fields.flatMap((f, i) => eqOrIn(f, columns[i]!)) : null;
}

/**
 * Converts the old selection. A pod of a selected service adds nothing to the union, so it is dropped. Conditions
 * take the place of what is left when they select the same pods; otherwise, such as for services from two
 * namespaces or a service and a pod of another service, the selection stays as it was.
 */
function convertSelection(selection: PodSelection | null): { conditions: Condition[]; selection: PodSelection | null } {
  if (selection === null) return { conditions: [], selection: null };
  const services = selection.services;
  const pods = selection.pods.filter((p) => !services.includes(p.split('/').slice(0, 2).join('/')));
  const exact =
    pods.length === 0
      ? exactConditions(services, ['namespace', 'service'])
      : services.length === 0
        ? exactConditions(pods, ['namespace', 'service', 'pod'])
        : null;
  return exact === null ? { conditions: [], selection: { services, pods } } : { conditions: exact, selection: null };
}

export interface LegacyConversion {
  conditions: Condition[];
  /** The services and pods that conditions cannot express, kept as they were; null when there are none. */
  selection: PodSelection | null;
}

/** Converts the old parameters to conditions, and to a selection where conditions would select other calls. */
export function convertLegacyParams(sp: URLSearchParams): LegacyConversion {
  const { conditions: out, selection } = convertSelection(readPodSelection(sp));

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
  return { conditions: out, selection };
}
