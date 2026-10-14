import { PushpinOutlined } from '@ant-design/icons';
import { Button, Tag, Tooltip, Typography } from 'antd';
import type { ReactNode } from 'react';

import { httpTitle } from '../api/http-title';
import { pkToPath } from '../api/pk';
import type { CallJSON } from '../api/types';
import { BROWSER_ZONE } from '../controls/time-range';
import { treeHref } from '../url/search-params';
import { durationHeat, formatBytes, formatCount, formatDurationMs, formatTs } from './format';
import { isIdleMethod } from './idle-tags';
import { callTraceId } from './trace-id';
import styles from './columns.module.css';

// The calls columns (09 §2.3). The first nine are the default set of the
// redesign; the rest are the R1 metric columns (08 R1), hidden until the user
// turns them on in the column settings.

/** A field that a row shortcut filters by: a click on a pod or a trace ID in a row. */
export type PickFilterField = 'pod' | 'trace_id';

export interface CallColumnDef {
  key: string;
  title: string;
  defaultWidth: number;
  /** Hidden until the user turns the column on. */
  defaultHidden?: boolean;
  align?: 'right';
  render: (call: CallJSON) => ReactNode;
  /** Client-side sort over the loaded pages only (09 §2.3) — not a global ranking. */
  compare?: (a: CallJSON, b: CallJSON) => number;
}

export interface ColumnHandlers {
  /** Filters the calls by the row's pod or trace ID. Without it, those cells are plain text. */
  onPickFilter?: (field: PickFilterField, value: string) => void;
  /**
   * Pins the row's pod-restart in the discovery rail (09 §2.1). Goes with the rail
   * in task 3.3 of the UI redesign plan.
   */
  onPinPod?: (tuple: string) => void;
}

/** A 12 px fixed-width number, as the mockup sets every numeric cell. */
function num(text: string): ReactNode {
  return <span className={`pf-mono ${styles.num}`}>{text}</span>;
}

/** The trace ID cut to 12 characters; the full ID is in the cell's tooltip. */
export function shortTraceId(id: string): string {
  return id.length > 12 ? `${id.slice(0, 12)}…` : id;
}

function podCell(c: CallJSON, handlers: ColumnHandlers, zone: string): ReactNode {
  const pod = c.pk.pod_name;
  const tuple = `${c.pk.pod_namespace}/${c.pk.pod_service}/${pod}`;
  const { onPickFilter, onPinPod } = handlers;
  return (
    <span className={styles.podCell}>
      {onPickFilter !== undefined ? (
        <button type="button" className={`${styles.pick} ${styles.pickPod}`} title={`Filter by pod ${pod}`} onClick={() => onPickFilter('pod', pod)}>
          {pod}
        </button>
      ) : (
        <Tooltip title={`${tuple} · restart ${formatTs(c.pk.restart_time_ms, zone)}`}>
          <Typography.Text>{pod}</Typography.Text>
        </Tooltip>
      )}
      {onPinPod !== undefined ? (
        <Button
          type="text"
          size="small"
          icon={<PushpinOutlined />}
          title={`Pin ${tuple} in the selection`}
          aria-label={`Pin ${tuple} in the selection`}
          onClick={() => onPinPod(tuple)}
        />
      ) : null}
    </span>
  );
}

function traceCell(c: CallJSON, handlers: ColumnHandlers): ReactNode {
  const id = callTraceId(c);
  if (id === null) {
    return (
      <span className={styles.notTraced} title="This call carries no trace ID. The call tree still opens from the Duration link.">
        not traced
      </span>
    );
  }
  const { onPickFilter } = handlers;
  if (onPickFilter === undefined) {
    return (
      <span className={`pf-mono ${styles.num}`} title={id}>
        {shortTraceId(id)}
      </span>
    );
  }
  return (
    <button
      type="button"
      className={`pf-mono ${styles.num} ${styles.pick} ${styles.pickTrace}`}
      title={`Filter by trace_id ${id}`}
      onClick={() => onPickFilter('trace_id', id)}
    >
      {shortTraceId(id)}
    </button>
  );
}

export function buildCallColumns(handlers: ColumnHandlers, zone: string = BROWSER_ZONE): CallColumnDef[] {
  return [
    { key: 'start', title: 'Start', defaultWidth: 190, render: (c) => num(formatTs(c.ts_ms, zone)), compare: (a, b) => a.ts_ms - b.ts_ms },
    {
      key: 'duration',
      title: 'Duration',
      defaultWidth: 110,
      align: 'right',
      compare: (a, b) => a.duration_ms - b.duration_ms,
      render: (c) => (
        <>
          <span className={styles.heatDot} style={{ background: durationHeat(c.duration_ms) }} aria-hidden="true" />
          <a
            href={treeHref(pkToPath(c.pk), c.ts_ms, c.retention_class)}
            target="_blank"
            rel="noopener noreferrer"
            title="Open the call tree in a new tab"
            className={`pf-mono ${styles.num}`}
          >
            {formatDurationMs(c.duration_ms)}
          </a>
        </>
      ),
    },
    { key: 'pod', title: 'Pod', defaultWidth: 230, render: (c) => podCell(c, handlers, zone), compare: (a, b) => a.pk.pod_name.localeCompare(b.pk.pod_name) },
    {
      key: 'title',
      title: 'Title',
      defaultWidth: 380,
      render: (c) => {
        const http = httpTitle(c.params);
        return (
          <span>
            {http !== null ? (
              <Tooltip title={c.method}>
                <Typography.Text>{http}</Typography.Text>
              </Tooltip>
            ) : (
              <Typography.Text type={isIdleMethod(c.method) ? 'secondary' : undefined}>{c.method}</Typography.Text>
            )}{' '}
            {c.error_flag ? <Tag color="error">error</Tag> : null}
            {'sql' in c.params ? <Tag color="processing">sql</Tag> : null}
            {c.truncated_reason !== null ? (
              <Tooltip title={c.truncated_reason}>
                <Tag color="warning">no tree</Tag>
              </Tooltip>
            ) : null}
          </span>
        );
      },
    },
    { key: 'trace_id', title: 'trace_id', defaultWidth: 140, render: (c) => traceCell(c, handlers) },
    { key: 'cpu', title: 'CPU', defaultWidth: 80, align: 'right', render: (c) => num(formatDurationMs(c.cpu_time_ms)), compare: (a, b) => a.cpu_time_ms - b.cpu_time_ms },
    { key: 'queue', title: 'Queue', defaultWidth: 80, align: 'right', render: (c) => num(formatDurationMs(c.queue_wait_ms)), compare: (a, b) => a.queue_wait_ms - b.queue_wait_ms },
    { key: 'calls', title: 'Calls', defaultWidth: 70, align: 'right', render: (c) => num(formatCount(c.child_calls)), compare: (a, b) => a.child_calls - b.child_calls },
    {
      key: 'net_io',
      title: 'Net IO',
      defaultWidth: 90,
      align: 'right',
      render: (c) => num(formatBytes(c.net_read + c.net_written)),
      compare: (a, b) => a.net_read + a.net_written - (b.net_read + b.net_written),
    },
    { key: 'suspend', title: 'Suspend', defaultWidth: 90, defaultHidden: true, align: 'right', render: (c) => num(formatDurationMs(c.suspend_ms)), compare: (a, b) => a.suspend_ms - b.suspend_ms },
    { key: 'transactions', title: 'Tx', defaultWidth: 60, defaultHidden: true, align: 'right', render: (c) => num(formatCount(c.transactions)), compare: (a, b) => a.transactions - b.transactions },
    {
      key: 'disk_io',
      title: 'Disk IO',
      defaultWidth: 90,
      defaultHidden: true,
      align: 'right',
      render: (c) => num(formatBytes(c.file_read + c.file_written)),
      compare: (a, b) => a.file_read + a.file_written - (b.file_read + b.file_written),
    },
    { key: 'memory', title: 'Memory', defaultWidth: 90, defaultHidden: true, align: 'right', render: (c) => num(formatBytes(c.memory_used)), compare: (a, b) => a.memory_used - b.memory_used },
  ];
}

export const DEFAULT_COLUMN_ORDER: readonly string[] = buildCallColumns({}).map((c) => c.key);

export const DEFAULT_HIDDEN_COLUMNS: readonly string[] = buildCallColumns({})
  .filter((c) => c.defaultHidden === true)
  .map((c) => c.key);
