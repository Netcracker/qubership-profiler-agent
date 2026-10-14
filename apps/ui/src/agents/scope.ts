import { AGENT_STATUSES, DEFAULT_SCOPE } from './types';
import type { AgentConfigAgent, AgentConfigScope, AgentConfigScopeId, AgentStatus } from './types';

// What the page derives from the wire models: labels, the scope tree's shape,
// and the status that stands for a group of agents.

export const STATUS_LABELS: Record<AgentStatus, string> = {
  applied: 'Applied',
  pending: 'Pending',
  failed: 'Failed',
  unsupported: 'Not managed',
};

// Worst first. An agent that cannot be managed ranks above an applied one: it
// does not run the config this page shows.
const SEVERITY: readonly AgentStatus[] = ['failed', 'pending', 'unsupported', 'applied'];

/** The status that stands for a group of agents: the worst one any of them has. */
export function worstStatus(counts: Record<AgentStatus, number>): AgentStatus {
  return SEVERITY.find((status) => counts[status] > 0) ?? 'applied';
}

export function countByStatus(agents: readonly AgentConfigAgent[]): Record<AgentStatus, number> {
  const counts = Object.fromEntries(AGENT_STATUSES.map((status) => [status, 0])) as Record<AgentStatus, number>;
  for (const agent of agents) counts[agent.status] += 1;
  return counts;
}

/** The scope's full name: `Default`, `<namespace>`, or `<namespace> / <service>`. */
export function scopeLabel(id: AgentConfigScopeId): string {
  return id === DEFAULT_SCOPE ? 'Default' : id.replace('/', ' / ');
}

/** The scope's name within its parent, for a row of the indented scope list. */
export function scopeShortLabel(id: AgentConfigScopeId): string {
  return id === DEFAULT_SCOPE ? 'Default' : id.slice(id.indexOf('/') + 1);
}

/** Null for `default`, which inherits from nothing. */
export function parentScope(id: AgentConfigScopeId): AgentConfigScopeId | null {
  if (id === DEFAULT_SCOPE) return null;
  const slash = id.indexOf('/');
  return slash === -1 ? DEFAULT_SCOPE : id.slice(0, slash);
}

/**
 * The scope whose config `id` would use without its own: the nearest ancestor
 * that has one. Null for `default`.
 */
export function inheritedOwner(
  scopes: readonly AgentConfigScope[],
  id: AgentConfigScopeId,
): AgentConfigScopeId | null {
  for (let parent = parentScope(id); parent !== null; parent = parentScope(parent)) {
    if (parent === DEFAULT_SCOPE || scopes.some((scope) => scope.id === parent && scope.has_own_config)) return parent;
  }
  return null;
}
