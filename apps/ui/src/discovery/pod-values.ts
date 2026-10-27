import type { ValueSuggestion } from '../calls/filter-bar/suggestions';
import type { NamespaceNode } from './group-pods';

// Value suggestions for the namespace, service, and pod fields of the filter bar, from the /pods data of the time
// range (redesign plan §4.2.1). Task 3.9 replaces them with /stats counts.

function unique(values: string[]): ValueSuggestion[] {
  return [...new Set(values)].sort((a, b) => a.localeCompare(b)).map((value) => ({ value }));
}

/** Builds the bar's value source; other fields get no suggestions. */
export function podValueSource(namespaces: readonly NamespaceNode[] | null): (field: string) => readonly ValueSuggestion[] {
  const nss = namespaces ?? [];
  const services = nss.flatMap((ns) => ns.services);
  const byField: Record<string, ValueSuggestion[]> = {
    namespace: unique(nss.map((ns) => ns.namespace)),
    service: unique(services.map((s) => s.service)),
    pod: unique(services.flatMap((s) => s.pods.map((p) => p.pod))),
  };
  return (field) => byField[field] ?? [];
}
