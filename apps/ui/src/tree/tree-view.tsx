import { EyeOutlined, MoreOutlined } from '@ant-design/icons';
import { Alert, App, Button, Dropdown, Input, Modal, Space, Tag, Typography, theme } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';

import { formatCount, formatDurationMs } from '../calls/format';
import { useElementHeight } from '../ui/use-element-height';
import { parseMethod } from './method-info';
import type { MethodInfo } from './method-info';
import { totalExecutions } from './model';
import type { TreeModel, TreeNode } from './model';
import { detectLanguage, InlineHighlight, ParamValueModal } from './param-value-viewer';
import type { ParamValueTarget } from './param-value-viewer';
import { searchTree } from './search';
import { stacktraceText } from './stacktrace';
import { VirtualList } from './virtual-list';
import { buildRows, expandLarge, initialExpansion } from './visible-rows';
import type { NodeRow, ParamRow, TreeRow } from './visible-rows';
import styles from './tree-view.module.css';

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
      <td className={styles.statLabel}>
        <Typography.Text type="secondary">{label}</Typography.Text>
      </td>
      <td className={styles.statValue}>{value}</td>
    </tr>
  );
  return (
    <div className={styles.statsContent}>
      <Typography.Paragraph className={styles.statsSignature} code>
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

/** Per-node operations (09 §3.4), backed by the 5.3 transforms. */
export interface TreeViewOps {
  incoming: (node: TreeNode) => void;
  outgoing: (node: TreeNode) => void;
  findUsages: (node: TreeNode) => void;
  localHotspots: (node: TreeNode) => void;
  adjust: (node: TreeNode) => void;
  addCategory: (node: TreeNode) => void;
}

/** Which way the tree grows; picks the menu button's arrow (old `tree.rv`). */
export type TreeDirection = 'top-down' | 'bottom-up';

interface TreeViewProps {
  model: TreeModel;
  /** 'bottom-up' on caller trees (incoming calls, hotspots); default 'top-down'. */
  direction?: TreeDirection;
  /** Auto-expansion hit the row budget; the view degraded, not froze. */
  onCapped?: (capped: boolean) => void;
  /** Absent on derived trees (op results), which stay read-only. */
  ops?: TreeViewOps;
  /** Expansion seed replacing the 10% auto-expansion (the hotspot grouping). */
  initialExpanded?: ReadonlySet<number>;
  /** Builds a `notComputed` node's children right before it expands. */
  computeChildren?: (node: TreeNode) => void;
}

export function TreeView({ model, direction = 'top-down', onCapped, ops, initialExpanded, computeChildren }: TreeViewProps) {
  const { message } = App.useApp();
  const { token } = theme.useToken();
  const dict = useMethodDict(model);
  const [containerRef, height] = useElementHeight<HTMLDivElement>(480);
  const ctrlHeld = useCtrlHeld();

  const initial = useMemo(
    () =>
      initialExpanded !== undefined
        ? { expanded: new Set(initialExpanded), capped: false }
        : initialExpansion(model, ROW_BUDGET),
    [model, initialExpanded],
  );
  useEffect(() => onCapped?.(initial.capped), [initial.capped, onCapped]);

  const [expanded, setExpanded] = useState<Set<number>>(initial.expanded);
  // A rebuilt model regrows lazy children from scratch; stale expansion
  // state would show grafted rows that no longer exist.
  useEffect(() => {
    if (initialExpanded !== undefined) setExpanded(new Set(initialExpanded));
  }, [initialExpanded]);
  const [revealedChains, setRevealedChains] = useState<Set<number>>(new Set());
  const [expandedParams, setExpandedParams] = useState<Set<string>>(new Set());
  const [marked, setMarked] = useState<Set<number>>(new Set());
  const [query, setQuery] = useState('');
  const [hoverNode, setHoverNode] = useState<TreeNode | null>(null);
  const [stacktrace, setStacktrace] = useState<string | null>(null);
  const [paramValue, setParamValue] = useState<ParamValueTarget | null>(null);

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
    // Lazy children (hotspots): build them before the expansion walks them.
    if (!row.expanded && row.node.notComputed === true) computeChildren?.(row.node);
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

  /** Node ids along the dominant-child chain: head, interiors, chain end. */
  const chainIds = (head: TreeNode, levels: number): number[] => {
    const ids = [head.id];
    let cur = head;
    for (let i = 0; i < levels && cur.children.length > 0; i++) {
      cur = cur.children[0]!;
      ids.push(cur.id);
    }
    return ids;
  };

  const revealChain = (row: NodeRow): void => {
    const ids = chainIds(row.node, row.skippedLevels);
    // Every chain node except the end is marked revealed, or its own
    // (shorter) chain would re-skip the levels below it — the old renderer
    // threaded an `uncollapsed` flag down the chain for the same reason.
    setRevealedChains((prev) => {
      const next = new Set(prev);
      for (const id of ids.slice(0, -1)) next.add(id);
      return next;
    });
    setExpanded((prev) => {
      const next = new Set(prev);
      for (const id of ids) next.add(id);
      return next;
    });
  };

  /** Inverse of revealChain: back to the “N levels skipped” row. */
  const collapseChain = (row: NodeRow): void => {
    const ids = chainIds(row.node, row.node.collapseLevels);
    setRevealedChains((prev) => {
      const next = new Set(prev);
      for (const id of ids.slice(0, -1)) next.delete(id);
      return next;
    });
    setExpanded((prev) => {
      const next = new Set(prev);
      // Prune what the reveal added — the interior chain nodes and the chain
      // end. The head stays expanded, so the skip shows through again.
      for (const id of ids.slice(1)) next.delete(id);
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
        className={styles.row}
        style={{
          paddingLeft: row.depth * 16,
          background: marked.has(node.id) ? token.colorErrorBg : isMatch ? token.colorWarningBg : node.category?.color,
        }}
        onMouseEnter={() => setHoverNode(node)}
        onMouseLeave={() => setHoverNode((cur) => (cur?.id === node.id ? null : cur))}
      >
        <Button
          size="small"
          type="text"
          className={styles.iconBtn}
          disabled={!row.hasChildren}
          aria-label={row.hasChildren ? (row.expanded ? 'Collapse node' : 'Expand node') : undefined}
          aria-hidden={row.hasChildren ? undefined : true}
          onClick={() => toggleNode(row)}
        >
          {row.hasChildren ? (row.expanded ? '−' : '+') : '·'}
        </Button>
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
          {/* A kebab icon reads as an actions menu; the old UI's direction
              arrow alone was easy to mistake for a tree marker (PR 708
              review #19). Left of the bar, so it never scrolls away. */}
          <Button
            size="small"
            type="text"
            title="Operations"
            aria-label={`Open node operations${direction === 'bottom-up' ? ' (bottom-up view)' : ''}`}
            className={styles.iconBtnMuted}
          >
            <MoreOutlined />
          </Button>
        </Dropdown>
        {row.skippedLevels > 0 ? (
          <Tag
            className={styles.chainTag}
            title={`${row.skippedLevels} pass-through level${row.skippedLevels > 1 ? 's' : ''} skipped — click to reveal`}
            onClick={() => revealChain(row)}
          >
            ⤵{row.skippedLevels}
          </Tag>
        ) : node.collapseLevels > 0 && revealedChains.has(node.id) && search === null ? (
          <Tag
            className={styles.chainTag}
            title={`${node.collapseLevels} pass-through level${node.collapseLevels > 1 ? 's' : ''} revealed — click to fold back`}
            onClick={() => collapseChain(row)}
          >
            ⤴{node.collapseLevels}
          </Tag>
        ) : null}
        {barWidth >= 0.6 ? (
          <span className={styles.durationBar} style={{ width: barWidth }} />
        ) : null}
        <Typography.Text className={styles.tabularNums}>
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
        <Typography.Text ellipsis className={styles.methodName} title={info.original}>
          {info.bareSignature === '' ? (
            info.original
          ) : (
            <>
              {/* font-size:0 hides the package yet keeps it selectable, so
                  copying the row yields the qualified name (old span.p). */}
              <span className={styles.pkgPrefix}>{info.packagePrefix}</span>
              {info.bareSignature}
            </>
          )}
        </Typography.Text>
      </div>
    );
    return content;
  };

  const renderParamRow = (row: ParamRow): ReactNode => {
    const key = model.paramKeys[row.keyIdx] ?? `param ${row.keyIdx}`;
    const isOther = row.group.value === '::other';
    const language = isOther ? null : detectLanguage(key, row.group.value);
    return (
      <div className={`${styles.row} ${styles.paramRow}`} style={{ paddingLeft: row.depth * 16 + 8 }}>
        <Button
          size="small"
          type="text"
          className={styles.iconBtn}
          disabled={!row.hasChildren}
          onClick={() => toggleParam(row)}
        >
          {row.hasChildren ? (row.expanded ? '−' : '+') : '·'}
        </Button>
        {/* Duration and invocations sit left of the value, the way the old
            renderSimpleTag laid tags out (duration, count, then the value). */}
        <Typography.Text type="secondary" className={styles.tabularNums}>
          {formatDurationMs(row.group.durationMs)} ×{formatCount(row.group.executions)}
        </Typography.Text>
        <Tag className={styles.tagFlush}>{key}</Tag>
        <Typography.Text
          type={isOther ? 'secondary' : undefined}
          italic={isOther}
          ellipsis={language === null}
          className={styles.paramValue}
          title={row.group.value}
        >
          {language === null ? row.group.value : <InlineHighlight language={language} value={row.group.value} />}
        </Typography.Text>
        {row.group.unresolved === true ? <Tag color="warning">unresolved</Tag> : null}
        {!isOther ? (
          <Button
            size="small"
            type="text"
            icon={<EyeOutlined />}
            title="View full value"
            aria-label="View full value"
            className={styles.iconBtnMuted}
            onClick={() => setParamValue({ key, value: row.group.value })}
          />
        ) : null}
      </div>
    );
  };

  return (
    <div className={styles.treeView}>
      <Space>
        <Input.Search
          placeholder="Search in the tree"
          allowClear
          size="small"
          className={styles.searchInput}
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
      <div ref={containerRef} className={styles.listContainer}>
        <VirtualList
          items={rows}
          rowHeight={ROW_HEIGHT}
          height={height}
          itemKey={(row: TreeRow) => (row.kind === 'node' ? `n${row.node.id}` : row.pathKey)}
          renderRow={(row) => (row.kind === 'node' ? renderNodeRow(row) : renderParamRow(row))}
        />
      </div>
      {/* The stats float in their own layer instead of wrapping the row in a
          Popover — reparenting the row would drop the user's text selection
          (the whole point of the copyable package span). */}
      {ctrlHeld && hoverNode !== null ? (
        <div className={styles.statsPopover}>
          <StatsContent node={hoverNode} dict={dict} />
        </div>
      ) : null}
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
        <pre className={styles.stacktracePre}>{stacktrace}</pre>
      </Modal>
      <ParamValueModal target={paramValue} onClose={() => setParamValue(null)} />
    </div>
  );
}
