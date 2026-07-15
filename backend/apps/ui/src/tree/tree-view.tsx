import { LoginOutlined, LogoutOutlined, MoreOutlined } from '@ant-design/icons';
import { Alert, App, Button, Dropdown, Input, Modal, Popover, Space, Tag, Typography } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import type { CSSProperties, ReactNode } from 'react';

import { formatCount, formatDurationMs } from '../calls/format';
import { useElementHeight } from '../ui/use-element-height';
import { parseMethod } from './method-info';
import type { MethodInfo } from './method-info';
import { totalExecutions } from './model';
import type { TreeModel, TreeNode } from './model';
import { searchTree } from './search';
import { stacktraceText } from './stacktrace';
import { VirtualList } from './virtual-list';
import { buildRows, expandLarge, initialExpansion } from './visible-rows';
import type { NodeRow, ParamRow, TreeRow } from './visible-rows';

// The Call Tree tab (09 §3.3–§3.4): virtualised rows, one-click expansion
// that skips pass-through chains, params as aggregated mini-tree rows,
// Ctrl+hover stats, and the per-row operations (stacktrace and mark-red now;
// the transform-backed operations land with 5.3).

const ROW_HEIGHT = 26;
/** Size guard (07 §5.4): bound the auto-expansion, never freeze the tab. */
export const ROW_BUDGET = 20_000;

interface MethodDict {
  info: (idx: number) => MethodInfo;
}

function useMethodDict(model: TreeModel): MethodDict {
  return useMemo(() => {
    const cache = new Map<number, MethodInfo>();
    return {
      info: (idx: number) => {
        let parsed = cache.get(idx);
        if (parsed === undefined) {
          parsed = parseMethod(model.methods[idx] ?? '');
          cache.set(idx, parsed);
        }
        return parsed;
      },
    };
  }, [model]);
}

/** True while Control (or Command on macOS) is held — drives the stats popover. */
function useCtrlHeld(): boolean {
  const [held, setHeld] = useState(false);
  useEffect(() => {
    const down = (e: KeyboardEvent): void => {
      if (e.key === 'Control' || e.key === 'Meta') setHeld(true);
    };
    const up = (e: KeyboardEvent): void => {
      if (e.key === 'Control' || e.key === 'Meta') setHeld(false);
    };
    window.addEventListener('keydown', down);
    window.addEventListener('keyup', up);
    window.addEventListener('blur', () => setHeld(false));
    return () => {
      window.removeEventListener('keydown', down);
      window.removeEventListener('keyup', up);
    };
  }, []);
  return held;
}

function StatsContent({ node, dict }: { node: TreeNode; dict: MethodDict }) {
  const info = dict.info(node.methodIdx);
  const executions = node.selfExecutions;
  const stat = (label: string, value: string): ReactNode => (
    <tr key={label}>
      <td style={{ paddingRight: 12 }}>
        <Typography.Text type="secondary">{label}</Typography.Text>
      </td>
      <td style={{ textAlign: 'right' }}>{value}</td>
    </tr>
  );
  return (
    <div style={{ maxWidth: 480 }}>
      <Typography.Paragraph style={{ marginBottom: 4 }} code>
        {info.signature}
      </Typography.Paragraph>
      <table>
        <tbody>
          {stat('Self time', formatDurationMs(node.selfDurationMs))}
          {stat('Total time', formatDurationMs(node.durationMs))}
          {executions > 0 ? stat('Avg self / invocation', formatDurationMs(Math.round(node.selfDurationMs / executions))) : null}
          {executions > 0 ? stat('Avg total / invocation', formatDurationMs(Math.round(node.durationMs / executions))) : null}
          {stat('Self suspension', formatDurationMs(node.selfSuspensionMs))}
          {stat('Total suspension', formatDurationMs(node.suspensionMs))}
          {stat('Invocations (direct)', formatCount(node.selfExecutions))}
          {stat('Invocations (total)', formatCount(totalExecutions(node)))}
        </tbody>
      </table>
      {info.fileName !== '' ? (
        <Typography.Text type="secondary">
          {info.fileName}:{info.lineNumber}
          {info.jarName !== '' ? ` · ${info.jarName}` : ''}
        </Typography.Text>
      ) : null}
    </div>
  );
}

const rowBase: CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 6,
  height: ROW_HEIGHT,
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  fontSize: 13,
};

/** Per-node operations (09 §3.4), backed by the 5.3 transforms. */
export interface TreeViewOps {
  incoming: (node: TreeNode) => void;
  outgoing: (node: TreeNode) => void;
  findUsages: (node: TreeNode) => void;
  localHotspots: (node: TreeNode) => void;
  adjust: (node: TreeNode) => void;
  addCategory: (node: TreeNode) => void;
}

interface TreeViewProps {
  model: TreeModel;
  /** Auto-expansion hit the row budget; the view degraded, not froze. */
  onCapped?: (capped: boolean) => void;
  /** Absent on derived trees (op results), which stay read-only. */
  ops?: TreeViewOps;
}

export function TreeView({ model, onCapped, ops }: TreeViewProps) {
  const { message } = App.useApp();
  const dict = useMethodDict(model);
  const [containerRef, height] = useElementHeight<HTMLDivElement>(480);
  const ctrlHeld = useCtrlHeld();

  const initial = useMemo(() => initialExpansion(model, ROW_BUDGET), [model]);
  useEffect(() => onCapped?.(initial.capped), [initial.capped, onCapped]);

  const [expanded, setExpanded] = useState<Set<number>>(initial.expanded);
  const [revealedChains, setRevealedChains] = useState<Set<number>>(new Set());
  const [expandedParams, setExpandedParams] = useState<Set<string>>(new Set());
  const [marked, setMarked] = useState<Set<number>>(new Set());
  const [query, setQuery] = useState('');
  const [hoverNode, setHoverNode] = useState<number | null>(null);
  const [stacktrace, setStacktrace] = useState<string | null>(null);

  const search = useMemo(() => searchTree(model, query), [model, query]);

  const rows = useMemo(() => {
    const effectiveExpanded = search === null ? expanded : new Set([...expanded, ...search.expand]);
    return buildRows(
      model,
      { expanded: effectiveExpanded, revealedChains, expandedParams },
      search !== null,
    );
  }, [model, expanded, revealedChains, expandedParams, search]);

  const toggleNode = (row: NodeRow): void => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (row.expanded) {
        next.delete(row.node.id);
      } else {
        // Expanding unfolds every large descendant with the node's own
        // cutoffs, the way the old renderer did (renderNodeChilds).
        expandLarge(row.node, ROW_BUDGET, next);
      }
      return next;
    });
  };

  const revealChain = (row: NodeRow): void => {
    setRevealedChains((prev) => new Set(prev).add(row.node.id));
    setExpanded((prev) => {
      const next = new Set(prev);
      let cur = row.node;
      for (let i = 0; i < row.skippedLevels && cur.children.length > 0; i++) {
        next.add(cur.id);
        cur = cur.children[0]!;
        next.add(cur.id);
      }
      return next;
    });
  };

  const toggleParam = (row: ParamRow): void => {
    setExpandedParams((prev) => {
      const next = new Set(prev);
      if (next.has(row.pathKey)) next.delete(row.pathKey);
      else next.add(row.pathKey);
      return next;
    });
  };

  const scale = model.root.durationMs > 0 ? 60 / model.root.durationMs : 0;

  const renderNodeRow = (row: NodeRow): ReactNode => {
    const { node } = row;
    const info = dict.info(node.methodIdx);
    const barWidth = Math.min(60, node.durationMs * scale);
    const isMatch = search !== null && search.matched.has(node.id);
    const content = (
      <div
        style={{
          ...rowBase,
          paddingLeft: row.depth * 16,
          background: marked.has(node.id) ? '#fff1f0' : isMatch ? '#fffbe6' : node.category?.color,
        }}
        onMouseEnter={() => setHoverNode(node.id)}
        onMouseLeave={() => setHoverNode((cur) => (cur === node.id ? null : cur))}
      >
        <Button
          size="small"
          type="text"
          style={{ width: 22, minWidth: 22, height: 22, padding: 0 }}
          disabled={!row.hasChildren}
          onClick={() => toggleNode(row)}
        >
          {row.hasChildren ? (row.expanded ? '−' : '+') : '·'}
        </Button>
        {row.skippedLevels > 0 ? (
          <Tag
            style={{ cursor: 'pointer', marginInlineEnd: 0 }}
            title={`${row.skippedLevels} pass-through level${row.skippedLevels > 1 ? 's' : ''} skipped — click to reveal`}
            onClick={() => revealChain(row)}
          >
            ⤵{row.skippedLevels}
          </Tag>
        ) : null}
        <span style={{ width: 62, minWidth: 62 }}>
          {barWidth >= 0.6 ? (
            <span
              style={{
                display: 'inline-block',
                width: barWidth,
                height: 8,
                background: '#91caff',
                borderRadius: 2,
              }}
            />
          ) : null}
        </span>
        <Typography.Text style={{ fontVariantNumeric: 'tabular-nums' }}>
          {formatDurationMs(node.durationMs)} ({formatDurationMs(node.selfDurationMs)})
        </Typography.Text>
        {node.suspensionMs > 0 ? (
          <Typography.Text type="secondary" title="suspension total (self)">
            ⏸ {formatDurationMs(node.suspensionMs)} ({formatDurationMs(node.selfSuspensionMs)})
          </Typography.Text>
        ) : null}
        {node.selfExecutions !== 1 || node.childExecutions > 0 ? (
          <Typography.Text type="secondary" title={`invocations: ${node.selfExecutions} direct, ${totalExecutions(node)} total`}>
            ×{formatCount(node.selfExecutions)}
          </Typography.Text>
        ) : null}
        <Typography.Text ellipsis style={{ flex: 1 }} title={info.original}>
          {info.signature}
        </Typography.Text>
        {ops !== undefined && hoverNode === node.id ? (
          <>
            <Button
              size="small"
              type="text"
              icon={<LoginOutlined />}
              title="Incoming calls"
              style={{ width: 22, minWidth: 22 }}
              onClick={() => ops.incoming(node)}
            />
            <Button
              size="small"
              type="text"
              icon={<LogoutOutlined />}
              title="Outgoing calls"
              style={{ width: 22, minWidth: 22 }}
              onClick={() => ops.outgoing(node)}
            />
          </>
        ) : null}
        <Dropdown
          trigger={['click']}
          menu={{
            items: [
              { key: 'stacktrace', label: 'Get stacktrace' },
              { key: 'mark', label: marked.has(node.id) ? 'Unmark red' : 'Mark red' },
              { type: 'divider' },
              { key: 'incoming', label: 'Incoming calls', disabled: ops === undefined },
              { key: 'outgoing', label: 'Outgoing calls', disabled: ops === undefined },
              { key: 'usages', label: 'Find usages', disabled: ops === undefined },
              { key: 'local', label: 'Local hotspots', disabled: ops === undefined },
              { key: 'adjust', label: 'Adjust duration', disabled: ops === undefined },
              { key: 'category', label: 'Add category', disabled: ops === undefined },
            ],
            onClick: ({ key }) => {
              if (key === 'stacktrace') {
                setStacktrace(stacktraceText(model, node));
              } else if (key === 'mark') {
                setMarked((prev) => {
                  const next = new Set(prev);
                  if (next.has(node.id)) next.delete(node.id);
                  else next.add(node.id);
                  return next;
                });
              } else if (ops !== undefined) {
                if (key === 'incoming') ops.incoming(node);
                else if (key === 'outgoing') ops.outgoing(node);
                else if (key === 'usages') ops.findUsages(node);
                else if (key === 'local') ops.localHotspots(node);
                else if (key === 'adjust') ops.adjust(node);
                else if (key === 'category') ops.addCategory(node);
              }
            },
          }}
        >
          <Button size="small" type="text" icon={<MoreOutlined />} style={{ width: 22, minWidth: 22 }} />
        </Dropdown>
      </div>
    );
    if (ctrlHeld && hoverNode === node.id) {
      return (
        <Popover open placement="top" content={<StatsContent node={node} dict={dict} />}>
          {content}
        </Popover>
      );
    }
    return content;
  };

  const renderParamRow = (row: ParamRow): ReactNode => {
    const key = model.paramKeys[row.keyIdx] ?? `param ${row.keyIdx}`;
    const isOther = row.group.value === '::other';
    return (
      <div style={{ ...rowBase, paddingLeft: row.depth * 16 + 8, color: '#666' }}>
        <Button
          size="small"
          type="text"
          style={{ width: 22, minWidth: 22, height: 22, padding: 0 }}
          disabled={!row.hasChildren}
          onClick={() => toggleParam(row)}
        >
          {row.hasChildren ? (row.expanded ? '−' : '+') : '·'}
        </Button>
        <Tag style={{ marginInlineEnd: 0 }}>{key}</Tag>
        <Typography.Text
          type={isOther ? 'secondary' : undefined}
          italic={isOther}
          ellipsis
          style={{ flex: 1 }}
          title={row.group.value}
        >
          {row.group.value}
        </Typography.Text>
        {row.group.unresolved === true ? <Tag color="orange">unresolved</Tag> : null}
        <Typography.Text type="secondary" style={{ fontVariantNumeric: 'tabular-nums' }}>
          {formatDurationMs(row.group.durationMs)} ×{formatCount(row.group.executions)}
        </Typography.Text>
      </div>
    );
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, height: '100%' }}>
      <Space>
        <Input.Search
          placeholder="Search in the tree"
          allowClear
          size="small"
          style={{ width: 280 }}
          onSearch={setQuery}
          onChange={(e) => {
            if (e.target.value === '') setQuery('');
          }}
        />
        {search !== null ? (
          <Typography.Text type="secondary">
            {search.matchCount} match{search.matchCount === 1 ? '' : 'es'}
          </Typography.Text>
        ) : null}
        <Typography.Text type="secondary">Ctrl+hover a row for its stats.</Typography.Text>
      </Space>
      {search !== null && search.matchCount === 0 ? (
        <Alert type="info" showIcon title="Nothing in the tree matches the search." />
      ) : null}
      <div ref={containerRef} style={{ flex: 1, minHeight: 240 }}>
        <VirtualList
          items={rows}
          rowHeight={ROW_HEIGHT}
          height={height}
          itemKey={(row: TreeRow) => (row.kind === 'node' ? `n${row.node.id}` : row.pathKey)}
          renderRow={(row) => (row.kind === 'node' ? renderNodeRow(row) : renderParamRow(row))}
        />
      </div>
      <Modal
        open={stacktrace !== null}
        title="Stacktrace"
        onCancel={() => setStacktrace(null)}
        footer={
          <Button
            type="primary"
            onClick={() => {
              if (stacktrace !== null) {
                void navigator.clipboard.writeText(stacktrace).then(() => message.success('Copied'));
              }
            }}
          >
            Copy
          </Button>
        }
      >
        <pre style={{ maxHeight: 400, overflow: 'auto', fontSize: 12 }}>{stacktrace}</pre>
      </Modal>
    </div>
  );
}
