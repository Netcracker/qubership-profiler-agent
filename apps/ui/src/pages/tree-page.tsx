import { CloseOutlined, DownloadOutlined, ExportOutlined, EyeOutlined, FieldTimeOutlined, TagsOutlined } from '@ant-design/icons';
import {
  App,
  Alert,
  Button,
  Empty,
  Input,
  Layout,
  Modal,
  Result,
  Space,
  Spin,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import { useMemo, useRef, useState } from 'react';
import { Link, useParams, useSearchParams } from 'react-router';

import { httpTitleFromNodeParams } from '../api/http-title';
import { parsePkPath, pkToPath } from '../api/pk';
import type { CallPK, RetentionClass } from '../api/types';
import { useConfig } from '../api/use-config';
import { buildExportHtml, downloadHtml } from '../export/build-export';
import { bytesToBase64, RESTORE_VERSION } from '../export/restore';
import type { SerializedTab } from '../export/restore';
import type { TreeWire } from '../msgpack/tree-wire';
import type { TreeState } from '../tree/use-tree';
import { formatCount, formatDurationMs, formatTs } from '../calls/format';
import { getThemeMode } from '../ui/theme-mode';
import { useZone } from '../ui/timezone';
import { FlameView } from '../tree/flame-view';
import { HotspotsView } from '../tree/hotspots-view';
import { callLinks, findTraceId } from '../tree/links';
import { parseMethod } from '../tree/method-info';
import { buildTreeModel, findNodeById } from '../tree/model';
import type { TreeModel, TreeNode } from '../tree/model';
import { detectLanguage, InlineHighlight, ParamValueModal } from '../tree/param-value-viewer';
import type { ParamValueTarget } from '../tree/param-value-viewer';
import type { ParamValueStat } from '../tree/params-summary';
import { summariseParams } from '../tree/params-summary';
import { applyAdjustments, factorByMethod, invalidAdjustLines, parseAdjustConfig } from '../tree/transforms/adjust';
import { applyCategories, invalidCategoryLines, parseCategoryConfig } from '../tree/transforms/categories';
import { computeFlatProfile } from '../tree/transforms/flat-profile';
import type { CategoryProfile } from '../tree/transforms/flat-profile';
import { findUsages, incomingCalls, localHotspots, outgoingCalls } from '../tree/transforms/merge';
import { TreeView } from '../tree/tree-view';
import type { TreeDirection, TreeViewOps } from '../tree/tree-view';
import { useTree } from '../tree/use-tree';
import { isTreeTab, parseTreeSearch, withTreeTab } from '../url/search-params';
import { ThemeSwitch } from '../shell/theme-switch';
import styles from './tree-page.module.css';

// Call Tree route (09 §3): the context header, the fixed tabs Call Tree ·
// Flame graph · Hotspots · Parameters with the toolbar beside them, the per-node operations
// backed by the 5.3 transforms, and the Adjust duration / Setup categories
// configs. The model rebuilds from the decoded wire on every config change, so
// the transforms stay pure and re-applicable.

/**
 * A derived view lives as a closeable tab (old dynamic_tabs). The tab holds
 * the recipe, not the result: the view re-derives from the current model, so
 * Adjust/Setup changes flow into open tabs like they did in the old UI.
 */
interface OpTabSpec {
  key: string;
  op: 'incoming' | 'outgoing' | 'usages' | 'local';
  methodIdx: number;
  category?: string;
  // The selected node's stable id, so Local Hotspots scopes to that exact
  // subtree instead of every occurrence of the method (PR 708 review #7).
  nodeId?: number;
}

const OP_META: Record<OpTabSpec['op'], { label: string; direction: TreeDirection }> = {
  incoming: { label: 'Incoming', direction: 'bottom-up' },
  outgoing: { label: 'Outgoing', direction: 'top-down' },
  usages: { label: 'Usages', direction: 'top-down' },
  local: { label: 'Hotspots', direction: 'top-down' },
};

interface OpView {
  spec: OpTabSpec;
  /** Bumps per (re-)derivation — keys the view so a rebuild remounts it. */
  seq: number;
  label: string;
  fullTitle: string;
  derived: TreeModel;
  profiles: CategoryProfile[] | null;
  direction: TreeDirection;
}

const ADJUST_PLACEHOLDER = `# <factor> <method pattern>, '*' wildcards, e.g.:
# 1/10 com.acme.billing.InvoiceService.createInvoice
# 2 *PgPreparedStatement.execute*`;

const CATEGORY_PLACEHOLDER = `# <category> <method pattern>; '>' assigns the children, e.g.:
# create_order com.acme.orders.CheckoutFlow.placeOrder
# db >*.PgPreparedStatement.*`;

/** A Parameters-tab table row: metadata and SQL shapes are top-level, binds nest under their SQL (09 §3.3). */
interface ParamSummaryRow extends ParamValueStat {
  key: string;
  kind: 'metadata' | 'sql' | 'bind';
  children?: ParamSummaryRow[];
}

function paramSummaryRows(model: TreeModel): ParamSummaryRow[] {
  const summary = summariseParams(model);
  const metadataRows = summary.metadata.map(
    (m): ParamSummaryRow => ({ ...m, key: `m:${m.keyIdx}:${m.value}`, kind: 'metadata' }),
  );
  const sqlRows = summary.sql.map((s): ParamSummaryRow => {
    const { binds, ...stat } = s;
    const rowKey = `s:${s.keyIdx}:${s.value}`;
    return {
      ...stat,
      key: rowKey,
      kind: 'sql',
      children:
        binds.length > 0
          ? binds.map((b): ParamSummaryRow => ({ ...b, key: `${rowKey}:b:${b.keyIdx}:${b.value}`, kind: 'bind' }))
          : undefined,
    };
  });
  return [...metadataRows, ...sqlRows];
}

/**
 * When present, the page renders an exported offline copy (10b): it skips the
 * fetch and the router and drives everything off this embedded wire and view
 * state, so a downloaded HTML reopens exactly as it was saved.
 */
export interface TreeRestore {
  wire: TreeWire;
  pk: CallPK;
  tsMs: number | null;
  retentionClass: RetentionClass | null;
  adjustText: string;
  categoryText: string;
  tabs: SerializedTab[];
  activeTab: string;
}

export function TreePage({ restore }: { restore?: TreeRestore } = {}) {
  const { message } = App.useApp();
  const zone = useZone();
  const { pk: pkRaw } = useParams<{ pk: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const routedHints = parseTreeSearch(searchParams);

  let pk: CallPK | null;
  let parseError: string | null = null;
  if (restore !== undefined) {
    pk = restore.pk;
  } else {
    pk = null;
    try {
      pk = parsePkPath(pkRaw ?? '');
    } catch (e) {
      parseError = e instanceof Error ? e.message : String(e);
    }
  }
  const tsMs = restore !== undefined ? restore.tsMs : routedHints.tsMs;
  const retentionClass = restore !== undefined ? restore.retentionClass : routedHints.retentionClass;

  // An exported copy skips the fetch: pk=null keeps useTree idle, and the
  // embedded wire drives the page instead.
  const { state: fetchedState, refetch } = useTree(restore !== undefined ? null : pk, {
    tsMs: tsMs ?? undefined,
    retentionClass: retentionClass ?? undefined,
  });
  const state: TreeState =
    restore !== undefined ? { kind: 'ready', wire: restore.wire, bytes: new Uint8Array() } : fetchedState;
  const [capped, setCapped] = useState(false);

  // Applied configs; the modals edit drafts. An exported copy seeds them so it
  // reopens with the same Adjust/Setup and derived tabs the user had.
  const [adjustText, setAdjustText] = useState(restore?.adjustText ?? '');
  const [categoryText, setCategoryText] = useState(restore?.categoryText ?? '');
  const [adjustModal, setAdjustModal] = useState<string | null>(null);
  const [categoryModal, setCategoryModal] = useState<string | null>(null);
  const [opTabs, setOpTabs] = useState<OpTabSpec[]>(() =>
    (restore?.tabs ?? []).map((t, i) => ({ key: `r${i}`, ...t })),
  );
  // The fixed tab lives in the URL (09 §6). A derived tab is browser state: it
  // holds a recipe that no link can carry, so it shadows the URL's tab while
  // it is open and active.
  const [derivedTab, setDerivedTab] = useState<string | null>(restore?.activeTab ?? null);
  const activeTab = derivedTab !== null && opTabs.some((t) => t.key === derivedTab) ? derivedTab : routedHints.tab;
  const selectTab = (key: string): void => {
    if (!isTreeTab(key)) {
      setDerivedTab(key);
      return;
    }
    setDerivedTab(null);
    // Replace, not push: a tab switch is not a step the Back button should undo.
    setSearchParams((prev) => withTreeTab(prev, key), { replace: true });
  };
  const [paramValue, setParamValue] = useState<ParamValueTarget | null>(null);
  // One selection for the Call Tree and the Flame graph tabs, by node id. The
  // id survives a model rebuild, so it also survives Adjust and Setup edits.
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [exporting, setExporting] = useState(false);
  const nextTabId = useRef(1);

  // Raw bytes only exist for a freshly fetched tree; the exported copy hides the
  // download button, so it needs none.
  const treeBytes = restore === undefined && fetchedState.kind === 'ready' ? fetchedState.bytes : null;

  const wire = state.kind === 'ready' ? state.wire : null;
  const model = useMemo(() => {
    if (wire === null) return null;
    const m = buildTreeModel(wire);
    const adjustRules = parseAdjustConfig(adjustText);
    if (adjustRules.length > 0) applyAdjustments(m, factorByMethod(m, adjustRules));
    applyCategories(m, categoryText.trim() === '' ? null : parseCategoryConfig(categoryText));
    return m;
  }, [wire, adjustText, categoryText]);
  // Whether the applied config actually adjusted anything — invalid-only
  // text (e.g. every line rejected) must not claim durations changed
  // (PR 708 review #13).
  const hasAdjustRules = useMemo(() => parseAdjustConfig(adjustText).length > 0, [adjustText]);
  const adjustModalInvalidLines = useMemo(() => invalidAdjustLines(adjustModal ?? ''), [adjustModal]);
  const categoryModalInvalidLines = useMemo(() => invalidCategoryLines(categoryModal ?? ''), [categoryModal]);

  const profiles = useMemo(() => (model === null ? [] : computeFlatProfile(model)), [model]);
  const paramRows = useMemo(() => (model === null ? [] : paramSummaryRows(model)), [model]);
  // The root call's HTTP context (web.method/web.url), when the call carries
  // one — the header otherwise shows only the technical root method, which
  // for Tomcat/Reactor entry points is not the primary thing a reader wants
  // (PR 708 review #6).
  const httpContext = useMemo(
    () => (model === null ? null : httpTitleFromNodeParams(model.root.params, model.paramKeys)),
    [model],
  );

  // The call's trace ID (null when it was not traced) and the link-outs that
  // /config offers (02 §2.8). An exported copy has no backend to ask for them.
  const configState = useConfig(restore === undefined);
  const traceId = useMemo(() => (model === null ? null : findTraceId(model)), [model]);
  const links = useMemo(() => {
    const configured = configState.kind === 'ready' ? (configState.config.links ?? []) : [];
    if (model === null || pk === null || configured.length === 0) return [];
    return callLinks(configured, {
      trace_id: traceId,
      // The call's window: its start, which travels as the ts_ms hint, plus its duration.
      from_ms: tsMs === null ? null : String(tsMs),
      to_ms: tsMs === null ? null : String(tsMs + model.root.durationMs),
      namespace: pk.pod_namespace,
      service: pk.pod_service,
      pod: pk.pod_name,
    });
  }, [configState, model, pk, traceId, tsMs]);

  const openOpTab = (op: OpTabSpec['op'], methodIdx: number, category?: string, nodeId?: number): void => {
    const key = `z${nextTabId.current++}`;
    setOpTabs((prev) => [...prev, { key, op, methodIdx, category, nodeId }]);
    setDerivedTab(key);
  };

  const closeOpTab = (key: string): void => {
    setOpTabs((prev) => prev.filter((t) => t.key !== key));
    // Closing the active derived tab returns to the fixed tab the URL names.
    setDerivedTab((cur) => (cur === key ? null : cur));
  };

  const ops: TreeViewOps = {
    incoming: (node) => openOpTab('incoming', node.methodIdx),
    outgoing: (node) => openOpTab('outgoing', node.methodIdx),
    findUsages: (node) => openOpTab('usages', node.methodIdx),
    localHotspots: (node) => openOpTab('local', node.methodIdx, undefined, node.id),
    adjust: (node) => {
      if (model === null) return;
      const method = model.methods[node.methodIdx] ?? '';
      const factor = node.selfExecutions > 1 ? node.selfExecutions : 10000;
      setAdjustModal(`${adjustText === '' ? '' : `${adjustText}\n`}1/${factor} ${method}`);
    },
    addCategory: (node) => {
      if (model === null) return;
      const method = model.methods[node.methodIdx] ?? '';
      const bare = parseMethod(method).bareSignature || method;
      const name = bare.split('(')[0]!.split('.').slice(-2).join('_').replace(/\s/g, '');
      setCategoryModal(`${categoryText === '' ? '' : `${categoryText}\n`}${name} ${method}`);
    },
  };

  // Re-derived when the model changes, so an open tab follows Adjust/Setup
  // edits. Cached per tab otherwise: re-deriving on every tab open/close
  // would mint fresh node ids and wipe the other tabs' expansion state.
  const deriveCache = useRef(new Map<string, { model: TreeModel; view: OpView }>());
  const viewSeq = useRef(0);
  const opViews = useMemo(() => {
    if (model === null) return [];
    const views = opTabs.map((spec) => {
      const cached = deriveCache.current.get(spec.key);
      if (cached !== undefined && cached.model === model) return cached.view;
      const meta = OP_META[spec.op];
      const word = model.methods[spec.methodIdx] ?? '';
      const info = parseMethod(word);
      const short = info.bareSignature === '' ? word : info.bareSignature.replace(/\(.*$/, '');
      // Local Hotspots scopes to the selected node's subtree; the node id is
      // stable across model rebuilds, so it resolves after Adjust/Setup edits.
      // A missing node (id not found) falls back to the whole-method merge.
      const localNode = spec.nodeId === undefined ? null : findNodeById(model.root, spec.nodeId);
      const derived =
        spec.op === 'incoming'
          ? incomingCalls(model, spec.methodIdx, spec.category)
          : spec.op === 'usages'
            ? findUsages(model, spec.methodIdx)
            : spec.op === 'local' && localNode !== null
              ? localHotspots(model, localNode)
              : outgoingCalls(model, spec.methodIdx);
      const view: OpView = {
        spec,
        seq: viewSeq.current++,
        label: `${meta.direction === 'bottom-up' ? '↖' : '↘'} ${meta.label} · ${short}`,
        fullTitle: word,
        derived,
        profiles: spec.op === 'local' ? computeFlatProfile(derived) : null,
        direction: meta.direction,
      };
      deriveCache.current.set(spec.key, { model, view });
      return view;
    });
    // Closed tabs leave the cache with them.
    const alive = new Set(opTabs.map((t) => t.key));
    for (const key of [...deriveCache.current.keys()]) {
      if (!alive.has(key)) deriveCache.current.delete(key);
    }
    return views;
  }, [model, opTabs]);

  if (parseError !== null || pk === null) {
    return (
      <Layout className={styles.errorLayout}>
        <Layout.Content className={styles.errorContent}>
          <Alert type="error" showIcon title="Malformed call reference" description={parseError} />
        </Layout.Content>
      </Layout>
    );
  }

  // Bake the tree the page already holds into a self-contained HTML file that
  // reopens offline (10b, option 2). The old "Raw trace" download handed back
  // opaque bytes the browser cannot render; this hands back the rendered page.
  const handleExport = async (): Promise<void> => {
    if (treeBytes === null || pk === null) return;
    setExporting(true);
    try {
      const html = await buildExportHtml({
        v: RESTORE_VERSION,
        pkPath: pkToPath(pk),
        tsMs,
        retentionClass,
        treeB64: bytesToBase64(treeBytes),
        adjustText,
        categoryText,
        tabs: opTabs.map((t): SerializedTab => ({ op: t.op, methodIdx: t.methodIdx, category: t.category, nodeId: t.nodeId })),
        // Derived tabs get new keys on restore (`r<index>`), so the active one is saved by position.
        activeTab: isTreeTab(activeTab) ? activeTab : `r${opTabs.findIndex((t) => t.key === activeTab)}`,
        theme: getThemeMode(),
      });
      downloadHtml(html, `profiler-tree-${pkToPath(pk).replace(/[^\w.-]+/g, '_')}.html`);
    } catch (e) {
      message.error(`Could not build the download. ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setExporting(false);
    }
  };

  const identity = `${pk.pod_namespace} / ${pk.pod_service} / ${pk.pod_name}`;

  // The toolbar sits at the right end of the tab bar. Without a tree there are
  // no tabs, so it renders on a row of its own, with its buttons disabled.
  const toolbar = (
    <div className={styles.toolbar}>
      <Button
        icon={<FieldTimeOutlined />}
        disabled={model === null}
        title={model === null ? 'No tree loaded yet, so there is nothing to adjust' : undefined}
        onClick={() => setAdjustModal(adjustText)}
      >
        Adjust duration
      </Button>
      <Button
        icon={<TagsOutlined />}
        disabled={model === null}
        title={model === null ? 'No tree loaded yet, so there is nothing to categorize' : undefined}
        onClick={() => setCategoryModal(categoryText)}
      >
        Setup categories
      </Button>
      {/* Reserved for "Explain" (the assistant is deferred). It goes here, before "Download HTML". */}
      {restore === undefined ? (
        <Button
          icon={<DownloadOutlined />}
          loading={exporting}
          disabled={model === null}
          title={
            model === null
              ? 'No tree loaded yet, so there is nothing to download'
              : 'Download a self-contained HTML copy that reopens offline'
          }
          onClick={() => void handleExport()}
        >
          Download HTML
        </Button>
      ) : null}
      {links.length > 0 ? <span className={styles.toolbarDivider} /> : null}
      {links.map((link, i) =>
        link.url !== null ? (
          <Tooltip key={i} title="Opens in a new tab">
            <Button href={link.url} target="_blank" rel="noopener noreferrer" icon={<ExportOutlined />} iconPlacement="end">
              {link.label}
            </Button>
          </Tooltip>
        ) : (
          // A link the call has no value for stays in place, disabled, and says
          // why (02 §2.8): hidden, it would look like a deployment without one.
          // A disabled button fires no pointer events, so the wrapper carries the
          // tooltip, and its tab stop lets the keyboard reach the reason too.
          <Tooltip key={i} title={link.reason}>
            <span className={styles.disabledLink} tabIndex={0}>
              <Button disabled icon={<ExportOutlined />} iconPlacement="end">
                {link.label}
              </Button>
            </span>
          </Tooltip>
        ),
      )}
    </div>
  );

  return (
    <Layout className={styles.layout}>
      <header className={styles.header}>
        {/* An exported copy has no Calls screen to go back to. */}
        {restore === undefined ? (
          <Link to="/calls" className={styles.back} title="Back to Calls" aria-label="Back to Calls">
            <svg className={styles.backIcon} width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
              <path d="M9.5 3.5 5 8l4.5 4.5" />
            </svg>
          </Link>
        ) : null}
        <div className={styles.product}>Profiler</div>
        <div className={styles.identity} title={identity}>
          <span className={styles.identityScope}>
            {pk.pod_namespace} / {pk.pod_service} /{' '}
          </span>
          {pk.pod_name}
        </div>
        {httpContext !== null ? (
          <span className={`${styles.chip} pf-mono`} title="From the root call's web.method and web.url parameters">
            {httpContext}
          </span>
        ) : null}
        {tsMs !== null ? (
          <span
            className={`${styles.chip} pf-mono`}
            title={retentionClass !== null ? `Start time. Retention class: ${retentionClass}` : 'Start time'}
          >
            {formatTs(tsMs, zone)}
          </span>
        ) : null}
        {model !== null ? (
          <span className={`${styles.chip} ${styles.chipStrong} pf-mono`} title="Duration">
            {formatDurationMs(model.root.durationMs)}
          </span>
        ) : null}
        {model === null ? null : traceId !== null ? (
          <span className={`${styles.chip} pf-mono`} title={`trace_id ${traceId}`}>
            trace {traceId.length > 12 ? `${traceId.slice(0, 12)}…` : traceId}
          </span>
        ) : (
          <span
            className={`${styles.chip} ${styles.chipMuted}`}
            title="This call carries no trace ID, so the links that need one are unavailable."
          >
            not traced
          </span>
        )}
        {restore !== undefined ? <span className={styles.chip}>offline copy</span> : null}
        <div className={styles.headerRight}>
          <ThemeSwitch />
        </div>
      </header>
      <Layout.Content className={styles.content}>
        {model === null ? <div className={styles.toolbarRow}>{toolbar}</div> : null}
        {state.kind === 'loading' ? (
          <div className={styles.centerBox}>
            <Spin description="Fetching and decoding the call tree." />
          </div>
        ) : state.kind === 'cold' ? (
          <Result
            status="info"
            title="This call is outside the hot window"
            subTitle={
              <>
                <Typography.Paragraph>
                  Reopen the call from its row in the calls list, so the ts_ms and retention_class hints travel
                  with it (02 §2.2).
                </Typography.Paragraph>
                <Typography.Text type="secondary">{state.detail}</Typography.Text>
              </>
            }
          />
        ) : state.kind === 'truncated' ? (
          <Result
            status="warning"
            title="The trace for this call was dropped under load — no tree to show"
            subTitle={state.detail}
          />
        ) : state.kind === 'error' ? (
          <Alert
            type="error"
            showIcon
            title="Loading the tree failed"
            description={state.message}
            action={
              <Button size="small" onClick={refetch}>
                Retry
              </Button>
            }
          />
        ) : model !== null ? (
          <>
            <Space orientation="vertical" className={styles.fullWidth} size={4}>
              {model.hasUnresolvedParams ? (
                <Alert
                  type="warning"
                  showIcon
                  title="Some big parameters could not be resolved"
                  description="Their value segments were evicted before the seal; the affected groups are tagged 'unresolved' and shown as references, not values."
                />
              ) : null}
              {capped ? (
                <Alert
                  type="warning"
                  showIcon
                  title="Large tree — automatic expansion was limited"
                  description="Branches are still expandable by hand."
                />
              ) : null}
              {adjustText.trim() !== '' && hasAdjustRules ? (
                <Alert
                  type="info"
                  showIcon
                  title="Durations are adjusted — this is a what-if view"
                  action={
                    <Button size="small" onClick={() => setAdjustText('')}>
                      Reset
                    </Button>
                  }
                />
              ) : null}
            </Space>
            <Tabs
              className={styles.tabs}
              activeKey={activeTab}
              onChange={selectTab}
              tabBarExtraContent={toolbar}
              items={[
                {
                  key: 'tree',
                  label: 'Call Tree',
                  children: (
                    <div className={styles.tabPane}>
                      <TreeView
                        model={model}
                        onCapped={setCapped}
                        ops={ops}
                        selectedId={selectedId}
                        onSelect={setSelectedId}
                      />
                    </div>
                  ),
                },
                {
                  key: 'flame',
                  label: 'Flame graph',
                  children: (
                    <div className={styles.tabPane}>
                      <FlameView model={model} selectedId={selectedId} onSelect={setSelectedId} />
                    </div>
                  ),
                },
                {
                  key: 'hotspots',
                  label: 'Hotspots',
                  children: (
                    <div className={styles.tabPane}>
                      <HotspotsView model={model} profiles={profiles} />
                    </div>
                  ),
                },
                {
                  key: 'params',
                  label: 'Parameters',
                  children: (
                    <Table
                      size="small"
                      rowKey="key"
                      dataSource={paramRows}
                      pagination={{ pageSize: 50, showSizeChanger: false }}
                      columns={[
                        {
                          title: 'Key',
                          width: 160,
                          render: (_, r) => (
                            <Tag color={r.kind === 'sql' ? 'processing' : undefined}>
                              {model.paramKeys[r.keyIdx] ?? r.keyIdx}
                            </Tag>
                          ),
                        },
                        {
                          title: 'Value',
                          render: (_, r) => {
                            const key = model.paramKeys[r.keyIdx] ?? `param ${r.keyIdx}`;
                            const isOther = r.value === '::other';
                            const language = isOther ? null : detectLanguage(key, r.value);
                            return (
                              <Space size={4}>
                                <Typography.Text
                                  type={isOther ? 'secondary' : undefined}
                                  italic={isOther}
                                  ellipsis={language === null}
                                  className={styles.paramValueCell}
                                  title={r.value}
                                >
                                  {language === null ? r.value : <InlineHighlight language={language} value={r.value} />}
                                </Typography.Text>
                                {r.unresolved ? <Tag color="warning">unresolved</Tag> : null}
                                {!isOther ? (
                                  <Button
                                    size="small"
                                    type="text"
                                    icon={<EyeOutlined />}
                                    title="View full value"
                                    aria-label="View full value"
                                    onClick={() => setParamValue({ key, value: r.value })}
                                  />
                                ) : null}
                              </Space>
                            );
                          },
                        },
                        {
                          title: 'Time',
                          width: 120,
                          align: 'right',
                          sorter: (a, b) => a.durationMs - b.durationMs,
                          render: (_, r) => formatDurationMs(r.durationMs),
                        },
                        {
                          title: 'Count',
                          width: 100,
                          align: 'right',
                          sorter: (a, b) => a.executions - b.executions,
                          render: (_, r) => formatCount(r.executions),
                        },
                        { title: 'Nodes', width: 90, align: 'right', render: (_, r) => formatCount(r.nodes) },
                      ]}
                    />
                  ),
                },
                ...opViews.map((v) => ({
                  key: v.spec.key,
                  // Line tabs have no close control of their own (antd draws one on card tabs only).
                  label: (
                    <span className={styles.derivedTab}>
                      <span title={v.fullTitle}>{v.label}</span>
                      <button
                        type="button"
                        className={styles.tabClose}
                        title="Close tab"
                        aria-label="Close tab"
                        onClick={(e) => {
                          // Without this the click also selects the tab that is going away.
                          e.stopPropagation();
                          closeOpTab(v.spec.key);
                        }}
                      >
                        <CloseOutlined />
                      </button>
                    </span>
                  ),
                  children: hasNoOccurrences(v.derived.root) ? (
                    <Empty description="The method does not occur in this tree." />
                  ) : v.profiles !== null ? (
                    <div className={styles.tabPane}>
                      <HotspotsView key={v.seq} model={v.derived} profiles={v.profiles} />
                    </div>
                  ) : (
                    <div className={styles.tabPane}>
                      <TreeView key={v.seq} model={v.derived} direction={v.direction} />
                    </div>
                  ),
                })),
              ]}
            />
          </>
        ) : null}

        <Modal
          open={adjustModal !== null}
          title="Adjust duration"
          okText="Apply"
          onOk={() => {
            setAdjustText(adjustModal ?? '');
            setAdjustModal(null);
          }}
          onCancel={() => setAdjustModal(null)}
          width={720}
        >
          <Typography.Paragraph type="secondary">
            One rule per line: <Typography.Text code>&lt;factor&gt; &lt;method pattern&gt;</Typography.Text>. A
            factor of <Typography.Text code>1/10</Typography.Text> means “what if this were 10× faster”; the
            factor cascades down the matched subtree and ancestor totals recompute.
          </Typography.Paragraph>
          <Input.TextArea
            rows={10}
            value={adjustModal ?? ''}
            onChange={(e) => setAdjustModal(e.target.value)}
            placeholder={ADJUST_PLACEHOLDER}
          />
          {adjustModalInvalidLines.length > 0 ? (
            <Alert
              type="warning"
              showIcon
              className={styles.alertGap}
              title={`Line${adjustModalInvalidLines.length === 1 ? '' : 's'} ${adjustModalInvalidLines.join(', ')} will be ignored`}
              description="Expected <factor> <method pattern>, for example 1/10 *Query.run*."
            />
          ) : null}
        </Modal>

        <Modal
          open={categoryModal !== null}
          title="Setup categories"
          okText="Apply"
          onOk={() => {
            setCategoryText(categoryModal ?? '');
            setCategoryModal(null);
          }}
          onCancel={() => setCategoryModal(null)}
          width={720}
        >
          <Typography.Paragraph type="secondary">
            One rule per line: <Typography.Text code>&lt;category&gt; &lt;method pattern&gt;</Typography.Text>.
            The category colours the matched subtree (a deeper match overrides) and Hotspots groups by it. A
            pattern starting with <Typography.Text code>&gt;</Typography.Text> assigns the method's children.
          </Typography.Paragraph>
          <Input.TextArea
            rows={10}
            value={categoryModal ?? ''}
            onChange={(e) => setCategoryModal(e.target.value)}
            placeholder={CATEGORY_PLACEHOLDER}
          />
          {categoryModalInvalidLines.length > 0 ? (
            <Alert
              type="warning"
              showIcon
              className={styles.alertGap}
              title={`Line${categoryModalInvalidLines.length === 1 ? '' : 's'} ${categoryModalInvalidLines.join(', ')} will be ignored`}
              description="Expected <category> <method pattern>, for example db >s.Dao.select*."
            />
          ) : null}
        </Modal>

        <ParamValueModal target={paramValue} onClose={() => setParamValue(null)} />
      </Layout.Content>
    </Layout>
  );
}

function hasNoOccurrences(root: TreeNode): boolean {
  return root.selfExecutions === 0 && root.childExecutions === 0 && root.children.length === 0;
}
