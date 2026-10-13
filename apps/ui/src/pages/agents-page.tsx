import { Alert, Button, Layout, Tabs } from 'antd';
import { useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router';

import { AgentsTable } from '../agents/agents-table';
import { fetchAgentConfig, fetchAgentConfigAgents, fetchAgentConfigScopes } from '../agents/api';
import { STATUS_LABELS, countByStatus, scopeLabel, worstStatus } from '../agents/scope';
import { ScopePicker } from '../agents/scope-picker';
import { StatusDot } from '../agents/status-dot';
import { AGENT_STATUSES, DEFAULT_SCOPE } from '../agents/types';
import type { AgentConfigScopeId } from '../agents/types';
import { useLoadable } from '../agents/use-loadable';
import { agentsSearchToParams, parseAgentsSearch } from '../url/search-params';
import type { AgentsTab } from '../url/search-params';
import styles from './agents-page.module.css';

/** How long after an answer the page asks again while an agent is pending. */
export const AGENT_POLL_INTERVAL_MS = 2000;

function scopeKindText(scope: AgentConfigScopeId): string {
  if (scope === DEFAULT_SCOPE) return 'Applies to every agent without a more specific config';
  return scope.includes('/') ? 'Service' : 'Namespace';
}

function LoadError({ what, message, onRetry }: { what: string; message: string; onRetry: () => void }) {
  return (
    <Alert
      type="error"
      showIcon
      title={`Could not load ${what}`}
      description={message}
      action={<Button onClick={onRetry}>Retry</Button>}
    />
  );
}

/**
 * Agent configuration: pick a scope, then see its config and the agents that
 * run it. The scope and the tab live in the URL. The page exists only when
 * /config reports the feature (see AppRoutes), and the API behind it is a
 * draft that only the mock implements (agents/types.ts).
 */
export function AgentsPage({ pollIntervalMs = AGENT_POLL_INTERVAL_MS }: { pollIntervalMs?: number }) {
  const [searchParams, setSearchParams] = useSearchParams();
  const search = useMemo(() => parseAgentsSearch(searchParams), [searchParams]);
  const { scope, tab } = search;

  const scopes = useLoadable('scopes', fetchAgentConfigScopes);
  const config = useLoadable(`config:${scope}`, (signal) => fetchAgentConfig(scope, signal));
  const agents = useLoadable(`agents:${scope}`, (signal) => fetchAgentConfigAgents(scope, signal));

  const agentRows = agents.state.kind === 'ready' ? agents.state.data.agents : null;
  const counts = useMemo(() => countByStatus(agentRows ?? []), [agentRows]);

  // Poll only while an agent is pending. Each answer is a new `agentRows`, which
  // arms the next request, so a slow answer is never cut off by the next one.
  // The scope tree is asked again too: its dots show the same statuses.
  const pending = counts.pending > 0;
  const refetchAgents = agents.refetch;
  const refetchScopes = scopes.refetch;
  useEffect(() => {
    if (!pending) return;
    const timer = setTimeout(() => {
      refetchAgents();
      refetchScopes();
    }, pollIntervalMs);
    return () => clearTimeout(timer);
  }, [pending, agentRows, pollIntervalMs, refetchAgents, refetchScopes]);

  const openScope = (next: AgentConfigScopeId): void => setSearchParams(agentsSearchToParams({ scope: next, tab }));
  const openTab = (next: AgentsTab): void => setSearchParams(agentsSearchToParams({ scope, tab: next }));

  const loaded = config.state.kind === 'ready' ? config.state.data : null;

  return (
    <Layout.Content className={styles.page}>
      <div className={styles.column}>
        <div className={styles.top}>
          <ScopePicker
            scopes={scopes.state.kind === 'ready' ? scopes.state.data.scopes : []}
            value={scope}
            status={worstStatus(counts)}
            onChange={openScope}
            onOpen={refetchScopes}
          />
          <span className={styles.dim}>{scopeKindText(scope)}</span>
          <div className={styles.summary}>
            {AGENT_STATUSES.filter((status) => counts[status] > 0).map((status) => (
              <span key={status} className={styles.count}>
                <StatusDot status={status} />
                {counts[status]} {STATUS_LABELS[status].toLowerCase()}
              </span>
            ))}
            {loaded === null ? null : <span className={`pf-mono ${styles.dim}`}>revision {loaded.revision}</span>}
          </div>
        </div>

        {scopes.state.kind === 'error' ? (
          <LoadError what="the scopes" message={scopes.state.message} onRetry={scopes.refetch} />
        ) : null}
        {config.state.kind === 'error' ? (
          <LoadError
            what={`the config of ${scopeLabel(scope)}`}
            message={config.state.message}
            onRetry={config.refetch}
          />
        ) : null}

        {loaded !== null && loaded.owner !== loaded.scope ? (
          <div className={styles.inherited}>
            This scope has no config of its own and uses the one from <strong>{scopeLabel(loaded.owner)}</strong>.
            Change any setting to give it its own.
          </div>
        ) : null}

        <Tabs
          activeKey={tab}
          onChange={(key) => openTab(key as AgentsTab)}
          items={[
            { key: 'settings', label: 'Settings' },
            { key: 'raw', label: 'Raw config' },
            {
              key: 'agents',
              label: agentRows === null ? 'Agents' : `Agents (${agentRows.length})`,
              children:
                agents.state.kind === 'error' ? (
                  <LoadError what="the agents" message={agents.state.message} onRetry={agents.refetch} />
                ) : (
                  <AgentsTable agents={agentRows ?? []} loading={agents.state.kind === 'loading'} />
                ),
            },
          ]}
        />
      </div>
    </Layout.Content>
  );
}
