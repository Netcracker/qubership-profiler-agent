import { PushpinOutlined } from '@ant-design/icons';
import { Button, Tag, Tooltip, Typography } from 'antd';
import type { ReactNode } from 'react';

import { pkToPath } from '../api/pk';
import type { CallJSON } from '../api/types';
import { treeHref } from '../url/search-params';
import { durationHeat, formatBytes, formatCount, formatDurationMs, formatTs } from './format';
import { isIdleMethod } from './idle-tags';

// The full calls column set (09 §2.3): the always-on identity columns plus
// the R1 metric columns the backend re-exposed (08 R1).

export interface CallColumnDef {
  key: string;
  title: string;
  defaultWidth: number;
  align?: 'right';
  render: (call: CallJSON) => ReactNode;
  /** Client-side sort over the loaded pages only (09 §2.3) — not a global ranking. */
  compare?: (a: CallJSON, b: CallJSON) => number;
}

export interface ColumnHandlers {
  /** Pin one pod from a row: the PK is pod-restart-scoped (09 §2.1). */
  onPinPod?: (tuple: string) => void;
}

export function buildCallColumns(handlers: ColumnHandlers): CallColumnDef[] {
  return [
    { key: 'start', title: 'Start', defaultWidth: 190, render: (c) => formatTs(c.ts_ms), compare: (a, b) => a.ts_ms - b.ts_ms },
    {
      key: 'duration',
      title: 'Duration',
      defaultWidth: 110,
      align: 'right',
      compare: (a, b) => a.duration_ms - b.duration_ms,
      render: (c) => (
        <a
          href={treeHref(pkToPath(c.pk), c.ts_ms, c.retention_class)}
          target="_blank"
          rel="noopener noreferrer"
          title="Open the call tree in a new tab"
        >
          <span
            style={{
              display: 'inline-block',
              width: 8,
              height: 8,
              borderRadius: 4,
              background: durationHeat(c.duration_ms),
              marginRight: 6,
            }}
          />
          {formatDurationMs(c.duration_ms)}
        </a>
      ),
    },
    {
      key: 'pod',
      title: 'Pod',
      defaultWidth: 210,
      render: (c) => {
        const tuple = `${c.pk.pod_namespace}/${c.pk.pod_service}/${c.pk.pod_name}`;
        return (
          <span style={{ whiteSpace: 'nowrap' }}>
            <Tooltip title={`${tuple} · restart ${formatTs(c.pk.restart_time_ms)}`}>
              <Typography.Text>{c.pk.pod_name}</Typography.Text>
            </Tooltip>
            {handlers.onPinPod !== undefined ? (
              <Button
                type="text"
                size="small"
                icon={<PushpinOutlined />}
                title={`Pin ${tuple} in the selection`}
                onClick={() => handlers.onPinPod?.(tuple)}
              />
            ) : null}
          </span>
        );
      },
    },
    {
      key: 'title',
      title: 'Title',
      defaultWidth: 380,
      render: (c) => (
        <span>
          <Typography.Text type={isIdleMethod(c.method) ? 'secondary' : undefined}>{c.method}</Typography.Text>{' '}
          {c.error_flag ? <Tag color="red">error</Tag> : null}
          {'sql' in c.params ? <Tag color="blue">sql</Tag> : null}
          {c.truncated_reason !== null ? (
            <Tooltip title={c.truncated_reason}>
              <Tag color="orange">no trace</Tag>
            </Tooltip>
          ) : null}
        </span>
      ),
    },
    { key: 'cpu', title: 'CPU', defaultWidth: 90, align: 'right', render: (c) => formatDurationMs(c.cpu_time_ms), compare: (a, b) => a.cpu_time_ms - b.cpu_time_ms },
    { key: 'suspend', title: 'Suspend', defaultWidth: 95, align: 'right', render: (c) => formatDurationMs(c.suspend_ms), compare: (a, b) => a.suspend_ms - b.suspend_ms },
    { key: 'queue', title: 'Queue', defaultWidth: 85, align: 'right', render: (c) => formatDurationMs(c.queue_wait_ms), compare: (a, b) => a.queue_wait_ms - b.queue_wait_ms },
    { key: 'calls', title: 'Calls', defaultWidth: 85, align: 'right', render: (c) => formatCount(c.child_calls), compare: (a, b) => a.child_calls - b.child_calls },
    { key: 'transactions', title: 'Tx', defaultWidth: 60, align: 'right', render: (c) => formatCount(c.transactions), compare: (a, b) => a.transactions - b.transactions },
    {
      key: 'diskIo',
      title: 'Disk IO',
      defaultWidth: 100,
      align: 'right',
      render: (c) => formatBytes(c.file_read + c.file_written),
      compare: (a, b) => a.file_read + a.file_written - (b.file_read + b.file_written),
    },
    {
      key: 'netIo',
      title: 'Net IO',
      defaultWidth: 100,
      align: 'right',
      render: (c) => formatBytes(c.net_read + c.net_written),
      compare: (a, b) => a.net_read + a.net_written - (b.net_read + b.net_written),
    },
    { key: 'memory', title: 'Memory', defaultWidth: 100, align: 'right', render: (c) => formatBytes(c.memory_used), compare: (a, b) => a.memory_used - b.memory_used },
  ];
}

export const DEFAULT_COLUMN_ORDER: readonly string[] = buildCallColumns({}).map((c) => c.key);
