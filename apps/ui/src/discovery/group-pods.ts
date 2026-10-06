import type { PodEntry } from '../api/types';

// /pods returns a flat list of pod-restart tuples; the namespace → service →
// pod tree is grouped client-side (07 §4) — there is no /namespaces endpoint
// and none is needed at v1 cluster sizes.

/** A pod-restart is "live" if its data reaches this close to now. */
export const LIVE_THRESHOLD_MS = 2 * 60 * 1000;

export interface PodNode {
  pod: string;
  /** `namespace/service/pod`, the /calls filter tuple. */
  tuple: string;
  restarts: PodEntry[];
  live: boolean;
  timeMinMs: number;
  timeMaxMs: number;
}

export interface ServiceNode {
  namespace: string;
  service: string;
  /** `namespace/service`, the selection unit key (09 §2.1). */
  key: string;
  pods: PodNode[];
  restartCount: number;
  live: boolean;
}

export interface NamespaceNode {
  namespace: string;
  services: ServiceNode[];
}

export function groupPods(entries: readonly PodEntry[], nowMs: number): NamespaceNode[] {
  const byNamespace = new Map<string, Map<string, Map<string, PodEntry[]>>>();
  for (const e of entries) {
    const services = byNamespace.get(e.namespace) ?? new Map<string, Map<string, PodEntry[]>>();
    byNamespace.set(e.namespace, services);
    const pods = services.get(e.service) ?? new Map<string, PodEntry[]>();
    services.set(e.service, pods);
    const restarts = pods.get(e.pod) ?? [];
    restarts.push(e);
    pods.set(e.pod, restarts);
  }

  const namespaces: NamespaceNode[] = [];
  for (const [namespace, services] of byNamespace) {
    const serviceNodes: ServiceNode[] = [];
    for (const [service, pods] of services) {
      const podNodes: PodNode[] = [];
      for (const [pod, restarts] of pods) {
        restarts.sort((a, b) => a.restart_time_ms - b.restart_time_ms);
        const timeMaxMs = Math.max(...restarts.map((r) => r.time_max_ms));
        podNodes.push({
          pod,
          tuple: `${namespace}/${service}/${pod}`,
          restarts,
          live: nowMs - timeMaxMs < LIVE_THRESHOLD_MS,
          timeMinMs: Math.min(...restarts.map((r) => r.time_min_ms)),
          timeMaxMs,
        });
      }
      podNodes.sort((a, b) => a.pod.localeCompare(b.pod));
      serviceNodes.push({
        namespace,
        service,
        key: `${namespace}/${service}`,
        pods: podNodes,
        restartCount: podNodes.reduce((sum, p) => sum + p.restarts.length, 0),
        live: podNodes.some((p) => p.live),
      });
    }
    serviceNodes.sort((a, b) => a.service.localeCompare(b.service));
    namespaces.push({ namespace, services: serviceNodes });
  }
  namespaces.sort((a, b) => a.namespace.localeCompare(b.namespace));
  return namespaces;
}

/**
 * Expands the URL selection into /calls pod tuples (02 §2.3 has no service
 * param, so a fully selected service becomes all of its pods in the window).
 * Returns null while the /pods data needed for the expansion is not there yet.
 */
export function expandSelection(
  namespaces: NamespaceNode[] | null,
  services: readonly string[],
  pods: readonly string[],
): string[] | null {
  if (services.length === 0) return [...new Set(pods)];
  if (namespaces === null) return null;
  const out = new Set<string>(pods);
  for (const ns of namespaces) {
    for (const svc of ns.services) {
      if (services.includes(svc.key)) {
        for (const pod of svc.pods) out.add(pod.tuple);
      }
    }
  }
  return [...out];
}
