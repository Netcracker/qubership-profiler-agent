import { Table } from 'antd';
import type { TableColumnsType } from 'antd';
import { useMemo } from 'react';

import { formatTs } from '../calls/format';
import { useZone } from '../ui/timezone';
import styles from './agents-table.module.css';
import { STATUS_LABELS } from './scope';
import { StatusDot } from './status-dot';
import type { AgentConfigAgent } from './types';

/**
 * What the agent runs. An agent that is behind shows the step it has yet to
 * take, as `<running> → <assigned>`.
 */
export function revisionText(agent: AgentConfigAgent): string {
  if (agent.revision === null) return 'n/a';
  if (agent.target_revision === null || agent.target_revision === agent.revision) return String(agent.revision);
  return `${agent.revision} → ${agent.target_revision}`;
}

const agentKey = (agent: AgentConfigAgent): string => `${agent.namespace}/${agent.service}/${agent.pod}`;

/** The Agents tab: one row per agent in the scope and below it. */
export function AgentsTable({ agents, loading }: { agents: readonly AgentConfigAgent[]; loading: boolean }) {
  const zone = useZone();
  const columns = useMemo<TableColumnsType<AgentConfigAgent>>(
    () => [
      {
        title: 'Pod',
        dataIndex: 'pod',
        ellipsis: true,
        render: (pod: string, agent) => <span title={agentKey(agent)}>{pod}</span>,
      },
      { title: 'Agent', dataIndex: 'agent_version', width: 90, className: 'pf-mono' },
      { title: 'Revision', key: 'revision', width: 100, className: 'pf-mono', render: (_, agent) => revisionText(agent) },
      {
        title: 'Status',
        dataIndex: 'status',
        width: 140,
        render: (_, agent) => (
          <span className={styles.status}>
            <StatusDot status={agent.status} />
            {STATUS_LABELS[agent.status]}
          </span>
        ),
      },
      {
        title: 'Detail',
        dataIndex: 'detail',
        ellipsis: true,
        render: (detail: string) => (
          <span className={styles.detail} title={detail}>
            {detail}
          </span>
        ),
      },
      {
        title: 'Last report',
        dataIndex: 'last_report_ms',
        width: 200,
        className: 'pf-mono',
        render: (ms: number) => formatTs(ms, zone),
      },
    ],
    [zone],
  );

  return (
    <Table<AgentConfigAgent>
      size="small"
      rowKey={agentKey}
      columns={columns}
      dataSource={agents}
      loading={loading}
      pagination={false}
      scroll={{ x: 900 }}
      footer={() => (
        <span className={styles.help}>
          An agent pulls the config when it connects and when the revision changes, then reports whether it applied it.
        </span>
      )}
    />
  );
}
