import { EyeOutlined, MinusSquareOutlined, PlusSquareOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import { Alert, App, Button, Dropdown, Input, Modal, Popover, Switch, Tag, Typography } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';

import { formatCount, formatDurationMs } from '../calls/format';
import { setTreePref, useTreePrefs } from '../ui/tree-prefs';
import { useElementHeight } from '../ui/use-element-height';
import { parseMethod } from './method-info';
import type { MethodInfo } from './method-info';
import { chainEnd, findNodeById, totalExecutions } from './model';
import type { TreeModel, TreeNode } from './model';
import { detectLanguage, InlineHighlight, ParamValueModal } from './param-value-viewer';
import type { ParamValueTarget } from './param-value-viewer';
import { formatShare, rowHighlight, shareBar, shareOf } from './row-metrics';
import type { Heat } from './row-metrics';
import { searchTree } from './search';
import { stacktraceText } from './stacktrace';
import { TreeHelp } from './tree-help';
import { VirtualList } from './virtual-list';
import { buildRows, expandAll, expandLarge, guideCells, initialExpansion } from './visible-rows';
import type { NodeRow, ParamRow, TreeRow } from './visible-rows';
import styles from './tree-view.module.css';

// The Call Tree tab (09 §3.3–§3.4): virtualised rows, one-click expansion
// that skips pass-through chains, params as aggregated mini-tree rows,
// Ctrl+hover stats, and the per-row operations (stacktrace and mark-red now;
// the transform-backed operations land with 5.3).

const ROW_HEIGHT = 28;

const GUIDE_CLASS = {
  none: undefined,
  pass: styles.guidePass,
  tee: styles.guideTee,
  end: styles.guideEnd,
};
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
  /**
   * The selected node's id, when the page owns the selection and shares it
   * with another view. Left out, the tree keeps a selection of its own.
   */
  selectedId?: number | null;
  onSelect?: (id: number | null) => void;
}

const HEAT_CLASS: Record<Heat, string | undefined> = {
  none: undefined,
  warn: styles.heatWarn,
  crit: styles.heatCrit,
};

function classNames(...names: (string | false | undefined)[]): string {
  return names.filter(Boolean).join(' ');
}

export function TreeView({
  model,
  direction = 'top-down',
  onCapped,
  ops,
  initialExpanded,
  computeChildren,
  selectedId: sharedSelectedId,
  onSelect,
}: TreeViewProps) {
  const { message } = App.useApp();
  const dict = useMethodDict(model);
  const [containerRef, height] = useElementHeight<HTMLDivElement>(480);
  const ctrlHeld = useCtrlHeld();
  const prefs = useTreePrefs();

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
  const [ownSelectedId, setOwnSelectedId] = useState<number | null>(null);
  const selectedId = sharedSelectedId !== undefined ? sharedSelectedId : ownSelectedId;

  const toggleSelected = (node: TreeNode): void => {
    // The label stays selectable text. A drag that ends on it is a copy in
    // progress, not a click on the row.
    if ((window.getSelection()?.toString() ?? '') !== '') return;
    (onSelect ?? setOwnSelectedId)(selectedId === node.id ? null : node.id);
  };

  // A selection made in another view (the flame graph) can sit under a closed
  // node or inside a skipped chain. Open the way to it, so the row exists when
  // this tab shows next. A row selected here is on screen already, and both
  // updates are then no-ops.
  useEffect(() => {
    if (sharedSelectedId === undefined || sharedSelectedId === null) return;
    const target = findNodeById(model.root, sharedSelectedId);
    if (target === null) return;
    const ancestors: TreeNode[] = [];
    for (let n = target.parent; n !== null; n = n.parent) ancestors.push(n);
    const path = new Set<TreeNode>([target, ...ancestors]);
    // A chain head shows the children of its chain end. It hides the target
    // when the target is the end itself or branches off before it.
    const hiding = ancestors.filter((a) => {
      if (a.collapseLevels === 0) return false;
      const end = chainEnd(a, a.collapseLevels);
      return end === target || !path.has(end);
    });
    setExpanded((prev) =>
      ancestors.every((a) => prev.has(a.id)) ? prev : new Set([...prev, ...ancestors.map((a) => a.id)]),
    );
    setRevealedChains((prev) =>
      hiding.every((a) => prev.has(a.id)) ? prev : new Set([...prev, ...hiding.map((a) => a.id)]),
    );
  }, [model, sharedSelectedId]);

  const search = useMemo(() => searchTree(model, query), [model, query]);

  const rows = useMemo(() => {
    const effectiveExpanded = search === null ? expanded : new Set([...expanded, ...search.expand]);
    return buildRows(
      model,
      { expanded: effectiveExpanded, revealedChains, expandedParams },
      search !== null,
    );
  }, [model, expanded, revealedChains, expandedParams, search]);

  // Only a shared selection asks for a scroll: one made in this tab is under the pointer.
  const ensureVisible = useMemo(
    () =>
      sharedSelectedId === undefined || sharedSelectedId === null
        ? undefined
        : {
            key: sharedSelectedId,
            index: rows.findIndex((r) => r.kind === 'node' && r.node.id === sharedSelectedId),
          },
    [rows, sharedSelectedId],
  );

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

  /** Opens the whole tree, breadth first, as far as the row budget allows. */
  const expandEverything = (): void => {
    const result = expandAll(model.root, ROW_BUDGET, revealedChains, computeChildren);
    setExpanded(result.expanded);
    if (result.capped) {
      void message.warning(
        `The tree has more than ${formatCount(ROW_BUDGET)} rows, so it is expanded only that far, level by level. ` +
          'Expand the remaining branches by hand.',
      );
    }
  };

  /** Closes everything under the root; the root's children stay in view. */
  const collapseEverything = (): void => {
    setExpanded(new Set([model.root.id]));
  };

  const rootDurationMs = model.root.durationMs;

  /** The indent of a row. The cells keep their width with the lines switched off. */
  const renderGuides = (row: TreeRow): ReactNode => (
    <span className={styles.guides}>
      {guideCells(row).map((cell, i) => (
        <span key={i} className={classNames(styles.guide, prefs.treeLines && GUIDE_CLASS[cell])} />
      ))}
    </span>
  );

  const renderNodeRow = (row: NodeRow): ReactNode => {
    const { node } = row;
    const info = dict.info(node.methodIdx);
    const isMatch = search !== null && search.matched.has(node.id);
    const isSelected = selectedId === node.id;
    // Always on, with no switch: the numbers that stand out in this call (09 §3.3).
    const highlight = rowHighlight(node, rootDurationMs);
    const bar = shareBar(node, rootDurationMs);
    const totalShare = formatShare(shareOf(node.durationMs, rootDurationMs));
    const selfShare = formatShare(shareOf(node.selfDurationMs, rootDurationMs));
    const calls = node.selfExecutions;
    // The badge marks where a category starts; the nodes below inherit it.
    const ownCategory = node.category !== undefined && node.category !== node.parent?.category ? node.category : null;
    return (
      <div
        className={classNames(
          styles.row,
          isSelected && styles.rowSelected,
          marked.has(node.id) && styles.rowMarked,
          search !== null && !isMatch && styles.rowDim,
        )}
        onMouseEnter={() => setHoverNode(node)}
        onMouseLeave={() => setHoverNode((cur) => (cur?.id === node.id ? null : cur))}
      >
        {renderGuides(row)}
        {row.hasChildren ? (
          <button
            type="button"
            className={styles.iconBtn}
            aria-label={row.expanded ? 'Collapse node' : 'Expand node'}
            aria-expanded={row.expanded}
            onClick={() => toggleNode(row)}
          >
            <svg
              className={classNames(styles.icon, !row.expanded && styles.caretClosed)}
              width="10"
              height="10"
              viewBox="0 0 12 12"
              aria-hidden="true"
            >
              <path d="M2.5 4.5 6 8l3.5-3.5" />
            </svg>
          </button>
        ) : (
          <span className={styles.iconSlot} />
        )}
        <Dropdown
          trigger={['click']}
          menu={{
            items: [
              { key: 'incoming', label: 'Incoming calls', disabled: ops === undefined },
              { key: 'outgoing', label: 'Outgoing calls', disabled: ops === undefined },
              { key: 'stacktrace', label: 'Get stacktrace' },
              { type: 'divider' },
              { key: 'usages', label: 'Find usages', disabled: ops === undefined },
              { key: 'local', label: 'Local hotspots', disabled: ops === undefined },
              { key: 'adjust', label: 'Adjust duration', disabled: ops === undefined },
              { key: 'category', label: 'Add category', disabled: ops === undefined },
              { key: 'mark', label: marked.has(node.id) ? 'Unmark red' : 'Mark red' },
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
              review #19). Left of the numbers, so it never scrolls away. */}
          <button
            type="button"
            title="Operations"
            aria-label={`Open node operations${direction === 'bottom-up' ? ' (bottom-up view)' : ''}`}
            aria-haspopup="menu"
            className={styles.iconBtn}
          >
            <svg className={styles.icon} width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
              <path d="M6 2.5v.01M6 6v.01M6 9.5v.01" strokeWidth="2" />
            </svg>
          </button>
        </Dropdown>
        <span className={`pf-mono ${styles.numbers}`}>
          <span className={highlight.totalBold ? styles.totalBold : styles.total}>{formatDurationMs(node.durationMs)}</span>{' '}
          <span
            className={classNames(styles.dimNumber, HEAT_CLASS[highlight.self])}
            title={`${selfShare} of the call is spent in this method itself`}
          >
            ({formatDurationMs(node.selfDurationMs)})
          </span>{' '}
          <span
            className={classNames(styles.dimNumber, HEAT_CLASS[highlight.calls])}
            title={
              highlight.calls === 'none'
                ? `Called ${formatCount(calls)} time${calls === 1 ? '' : 's'}`
                : `Called ${formatCount(calls)} times from one place. Check for a loop that could be one batch call.`
            }
          >
            ×{formatCount(calls)}
          </span>
        </span>
        {/* A span, not a button: the text of a button cannot be selected, and
            copying the qualified name off a row is what the package is there for. */}
        <span
          role="button"
          tabIndex={0}
          aria-pressed={isSelected}
          className={styles.signature}
          title={info.original}
          onClick={() => toggleSelected(node)}
          onKeyDown={(e) => {
            if (e.key !== 'Enter' && e.key !== ' ') return;
            e.preventDefault();
            toggleSelected(node);
          }}
        >
          {info.bareSignature === '' ? (
            <span className={classNames(isMatch && styles.match)}>{info.original}</span>
          ) : (
            <>
              <span className={styles.pkg}>{info.packagePrefix}</span>
              <span className={classNames(isMatch && styles.match, highlight.self !== 'none' && styles.nameHot)}>
                {info.bareSignature}
              </span>
            </>
          )}
        </span>
        {row.skippedLevels > 0 ? (
          <button
            type="button"
            className={classNames(styles.badge, styles.chainBadge)}
            title={`${row.skippedLevels} pass-through level${row.skippedLevels > 1 ? 's are' : ' is'} skipped under this method. Click to show ${row.skippedLevels > 1 ? 'them' : 'it'}.`}
            onClick={() => revealChain(row)}
          >
            +{row.skippedLevels}
          </button>
        ) : node.collapseLevels > 0 && revealedChains.has(node.id) && search === null ? (
          <button
            type="button"
            className={classNames(styles.badge, styles.chainBadge)}
            title={`${node.collapseLevels} pass-through level${node.collapseLevels > 1 ? 's are' : ' is'} shown under this method. Click to skip ${node.collapseLevels > 1 ? 'them' : 'it'} again.`}
            onClick={() => collapseChain(row)}
          >
            −{node.collapseLevels}
          </button>
        ) : null}
        {ownCategory !== null ? (
          <span className={styles.badge} style={{ background: ownCategory.color }}>
            {ownCategory.name}
          </span>
        ) : null}
        <span className={styles.spacer} />
        <span
          className={styles.shareBar}
          title={`${totalShare} of the call, ${selfShare} in the method itself`}
        >
          <span className={styles.shareSelf} style={{ width: bar.selfWidth }} />
          <span className={styles.shareChildren} style={{ width: bar.childrenWidth }} />
        </span>
        <span className={`pf-mono ${styles.percent}`}>{totalShare}</span>
      </div>
    );
  };

  const renderParamRow = (row: ParamRow): ReactNode => {
    const key = model.paramKeys[row.keyIdx] ?? `param ${row.keyIdx}`;
    const isOther = row.group.value === '::other';
    const language = isOther ? null : detectLanguage(key, row.group.value);
    return (
      <div className={`${styles.row} ${styles.paramRow}`}>
        {renderGuides(row)}
        {/* A parameter has no operations; the two slots keep it under its method's numbers. */}
        {row.hasChildren ? (
          <button
            type="button"
            className={styles.iconBtn}
            aria-label={row.expanded ? 'Collapse parameter' : 'Expand parameter'}
            aria-expanded={row.expanded}
            onClick={() => toggleParam(row)}
          >
            <svg
              className={classNames(styles.icon, !row.expanded && styles.caretClosed)}
              width="10"
              height="10"
              viewBox="0 0 12 12"
              aria-hidden="true"
            >
              <path d="M2.5 4.5 6 8l3.5-3.5" />
            </svg>
          </button>
        ) : (
          <span className={styles.iconSlot} />
        )}
        <span className={styles.iconSlot} />
        {/* Duration and invocations sit left of the value, the way the old
            renderSimpleTag laid tags out (duration, count, then the value). */}
        <span className={`pf-mono ${styles.numbers}`}>
          {formatDurationMs(row.group.durationMs)} ×{formatCount(row.group.executions)}
        </span>
        <span className={`pf-mono ${styles.badge} ${styles.keyBadge}`}>{key}</span>
        <span
          className={classNames('pf-mono', styles.paramValue, isOther && styles.paramOther)}
          title={row.group.value}
        >
          {language === null ? row.group.value : <InlineHighlight language={language} value={row.group.value} />}
        </span>
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
      <div className={styles.toolbar}>
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
        <Typography.Text type="secondary">
          {search !== null
            ? `${search.matchCount} match${search.matchCount === 1 ? '' : 'es'}`
            : 'Ctrl+hover a row for its stats.'}
        </Typography.Text>
        <span className={`${styles.legend} ${styles.toolbarEnd}`}>
          <span className={`${styles.swatch} ${styles.shareSelf}`} />
          Self time
        </span>
        <span className={styles.legend}>
          <span className={`${styles.swatch} ${styles.shareChildren}`} />
          Time in children
        </span>
        <label className={styles.switchLabel} title="Draw connectors from a method to the methods it calls">
          <Switch size="small" checked={prefs.treeLines} onChange={(on) => setTreePref('treeLines', on)} />
          Tree lines
        </label>
        <label className={styles.switchLabel} title="Draw a line between rows">
          <Switch size="small" checked={prefs.rowLines} onChange={(on) => setTreePref('rowLines', on)} />
          Row lines
        </label>
        <Button
          size="small"
          icon={<PlusSquareOutlined />}
          title={`Expand every node, up to ${formatCount(ROW_BUDGET)} rows`}
          onClick={expandEverything}
        >
          Expand all
        </Button>
        <Button size="small" icon={<MinusSquareOutlined />} title="Collapse every node under the root" onClick={collapseEverything}>
          Collapse all
        </Button>
        <Popover trigger="click" placement="bottomRight" content={<TreeHelp />}>
          <Button size="small" icon={<QuestionCircleOutlined />} title="How to read this page">
            Help
          </Button>
        </Popover>
      </div>
      {search !== null && search.matchCount === 0 ? (
        <Alert type="info" showIcon title="Nothing in the tree matches the search." />
      ) : null}
      <div className={styles.listHeader}>
        <span className={styles.listHeaderMain}>Total (self) ×calls · Method</span>
        <span>Share of the call</span>
      </div>
      <div ref={containerRef} className={classNames(styles.listContainer, !prefs.rowLines && styles.noRowLines)}>
        <VirtualList
          items={rows}
          rowHeight={ROW_HEIGHT}
          height={height}
          itemKey={(row: TreeRow) => (row.kind === 'node' ? `n${row.node.id}` : row.pathKey)}
          renderRow={(row) => (row.kind === 'node' ? renderNodeRow(row) : renderParamRow(row))}
          ensureVisible={ensureVisible}
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
