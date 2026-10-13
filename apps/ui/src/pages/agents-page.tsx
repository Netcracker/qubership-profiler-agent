import { Alert, Button, Layout, Modal, Popconfirm, Skeleton, Tabs } from 'antd';
import { useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router';

import { AgentsTable } from '../agents/agents-table';
import { ApplyBar } from '../agents/apply-bar';
import { fetchAgentConfigAgents, fetchAgentConfigScopes } from '../agents/api';
import { STATUS_LABELS, countByStatus, inheritedOwner, scopeLabel, worstStatus } from '../agents/scope';
import { ScopePicker } from '../agents/scope-picker';
import { SettingsCards } from '../agents/settings-cards';
import { StatusDot } from '../agents/status-dot';
import { AGENT_STATUSES, DEFAULT_SCOPE } from '../agents/types';
import type { AgentConfigScopeId } from '../agents/types';
import { useConfigEditor } from '../agents/use-config-editor';
import { useLoadable } from '../agents/use-loadable';
import { useUnsavedGuard } from '../agents/use-unsaved-guard';
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
  const editor = useConfigEditor(scope);
  const config = editor.config;
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

  const loaded = config.kind === 'ready' ? config.data : null;
  const hasOwnConfig = loaded !== null && loaded.owner === loaded.scope;
  const fallback = inheritedOwner(scopes.state.kind === 'ready' ? scopes.state.data.scopes : [], scope);
  // An agent that cannot pull config does not get the revision, so it is not counted.
  const managedAgents = agentRows === null ? null : agentRows.filter((agent) => agent.status !== 'unsupported').length;

  const guard = useUnsavedGuard(editor.dirty);

  // A save or a delete hands the agents a new revision: show them picking it up.
  const showRollout = (): void => {
    refetchAgents();
    refetchScopes();
    openTab('agents');
  };
  const apply = async (): Promise<void> => {
    if (await editor.apply()) showRollout();
  };
  const revertToInherited = async (): Promise<void> => {
    if (await editor.revertToInherited()) showRollout();
  };

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
            {hasOwnConfig && fallback !== null ? (
              <Popconfirm
                title="Use the inherited config?"
                description={
                  <>
                    This deletes the config of {scopeLabel(scope)}. Its agents get the config of{' '}
                    {scopeLabel(fallback)}.{editor.dirty ? ' Your unsaved changes are discarded.' : ''}
                  </>
                }
                okText="Delete own config"
                cancelText="Cancel"
                onConfirm={revertToInherited}
              >
                <Button disabled={editor.saving}>Use inherited config</Button>
              </Popconfirm>
            ) : null}
          </div>
        </div>

        {scopes.state.kind === 'error' ? (
          <LoadError what="the scopes" message={scopes.state.message} onRetry={scopes.refetch} />
        ) : null}
        {config.kind === 'error' ? (
          <LoadError what={`the config of ${scopeLabel(scope)}`} message={config.message} onRetry={editor.reload} />
        ) : null}
        {editor.error === null ? null : (
          <Alert type="error" showIcon closable title={editor.error} onClose={editor.clearError} />
        )}

        {loaded !== null && loaded.owner !== loaded.scope ? (
          <div className={styles.inherited}>
            This scope has no config of its own and uses the one from <strong>{scopeLabel(loaded.owner)}</strong>.
            Change any setting to give it its own.
          </div>
        ) : null}

        <Tabs
          activeKey={tab}
          // A hidden tab would keep working: Raw config renders the XML again on every settings edit.
          destroyOnHidden
          onChange={(key) => openTab(key as AgentsTab)}
          items={[
            {
              key: 'settings',
              label: 'Settings',
              children:
                loaded === null || editor.settings === null ? (
                  config.kind === 'loading' ? (
                    <Skeleton active />
                  ) : null
                ) : (
                  <SettingsCards
                    settings={editor.settings}
                    availablePlugins={loaded.available_plugins}
                    startup={loaded.startup}
                    onChange={editor.editSettings}
                  />
                ),
            },
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

        {editor.dirty ? (
          <ApplyBar
            createsOwnConfig={!hasOwnConfig}
            managedAgents={managedAgents}
            saving={editor.saving}
            onDiscard={editor.discard}
            onApply={apply}
          />
        ) : null}
      </div>

      <Modal
        open={editor.conflict}
        title="Someone else saved this scope first"
        destroyOnHidden
        closable={false}
        mask={{ closable: false }}
        footer={[
          <Button key="keep" onClick={editor.dismissConflict}>
            Keep editing
          </Button>,
          <Button key="reload" type="primary" onClick={editor.reload}>
            Reload
          </Button>,
        ]}
      >
        The config of {scopeLabel(scope)} changed after you opened it, so your changes were not applied. Reload shows
        the saved config and discards your changes. Keep editing leaves them on the page, but they cannot be applied
        until you reload.
      </Modal>

      <Modal
        open={guard.state === 'blocked'}
        destroyOnHidden
        title="Discard unsaved changes?"
        okText="Discard changes"
        cancelText="Keep editing"
        onOk={() => guard.proceed?.()}
        onCancel={() => guard.reset?.()}
      >
        The changes to {scopeLabel(scope)} are not applied. Leaving discards them.
      </Modal>
    </Layout.Content>
  );
}
