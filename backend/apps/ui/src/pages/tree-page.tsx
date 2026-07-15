import { DownloadOutlined } from '@ant-design/icons';
import { Alert, Button, Empty, Input, Layout, Modal, Result, Space, Spin, Table, Tabs, Tag, Typography } from 'antd';
import { useMemo, useRef, useState } from 'react';
import { useParams, useSearchParams } from 'react-router';

import { httpTitleFromNodeParams } from '../api/http-title';
import { parsePkPath, pkToPath } from '../api/pk';
import type { CallPK } from '../api/types';
import { formatCount, formatDurationMs, formatTs } from '../calls/format';
import { HotspotsView } from '../tree/hotspots-view';
import { parseMethod } from '../tree/method-info';
import { buildTreeModel } from '../tree/model';
import type { TreeModel, TreeNode } from '../tree/model';
import { summariseParams } from '../tree/params-summary';
import { applyAdjustments, factorByMethod, invalidAdjustLines, parseAdjustConfig } from '../tree/transforms/adjust';
import { applyCategories, invalidCategoryLines, parseCategoryConfig } from '../tree/transforms/categories';
import { computeFlatProfile } from '../tree/transforms/flat-profile';
import type { CategoryProfile } from '../tree/transforms/flat-profile';
import { findUsages, incomingCalls, outgoingCalls } from '../tree/transforms/merge';
import { TreeView } from '../tree/tree-view';
import type { TreeDirection, TreeViewOps } from '../tree/tree-view';
import { useTree } from '../tree/use-tree';
import { parseTreeSearch } from '../url/search-params';

// Call Tree route (09 §3): tabs Call Tree · Hotspots · Parameters, the
// per-node operations backed by the 5.3 transforms, and the Adjust duration
// / Setup categories configs. The model rebuilds from the decoded wire on
// every config change, so the transforms stay pure and re-applicable.

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

export function TreePage() {
  const { pk: pkRaw } = useParams<{ pk: string }>();
  const [searchParams] = useSearchParams();
  const hints = parseTreeSearch(searchParams);

  let pk: CallPK | null = null;
  let parseError: string | null = null;
  try {
    pk = parsePkPath(pkRaw ?? '');
  } catch (e) {
    parseError = e instanceof Error ? e.message : String(e);
  }

  const { state, refetch } = useTree(pk, {
    tsMs: hints.tsMs ?? undefined,
    retentionClass: hints.retentionClass ?? undefined,
  });
  const [capped, setCapped] = useState(false);

  // Applied configs; the modals edit drafts.
  const [adjustText, setAdjustText] = useState('');
  const [categoryText, setCategoryText] = useState('');
  const [adjustModal, setAdjustModal] = useState<string | null>(null);
  const [categoryModal, setCategoryModal] = useState<string | null>(null);
  const [opTabs, setOpTabs] = useState<OpTabSpec[]>([]);
  const [activeTab, setActiveTab] = useState('tree');
  const nextTabId = useRef(1);

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
  const paramStats = useMemo(() => (model === null ? [] : summariseParams(model)), [model]);
  // The root call's HTTP context (web.method/web.url), when the call carries
  // one — the header otherwise shows only the technical root method, which
  // for Tomcat/Reactor entry points is not the primary thing a reader wants
  // (PR 708 review #6).
  const httpContext = useMemo(
    () => (model === null ? null : httpTitleFromNodeParams(model.root.params, model.paramKeys)),
    [model],
  );

  const openOpTab = (op: OpTabSpec['op'], methodIdx: number, category?: string): void => {
    const key = `z${nextTabId.current++}`;
    setOpTabs((prev) => [...prev, { key, op, methodIdx, category }]);
    setActiveTab(key);
  };

  const closeOpTab = (key: string): void => {
    setOpTabs((prev) => prev.filter((t) => t.key !== key));
    setActiveTab((cur) => (cur === key ? 'tree' : cur));
  };

  const ops: TreeViewOps = {
    incoming: (node) => openOpTab('incoming', node.methodIdx),
    outgoing: (node) => openOpTab('outgoing', node.methodIdx),
    findUsages: (node) => openOpTab('usages', node.methodIdx),
    localHotspots: (node) => openOpTab('local', node.methodIdx),
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
      const derived =
        spec.op === 'incoming'
          ? incomingCalls(model, spec.methodIdx, spec.category)
          : spec.op === 'usages'
            ? findUsages(model, spec.methodIdx)
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
      <Layout style={{ minHeight: '100vh' }}>
        <Layout.Content style={{ padding: 24 }}>
          <Alert type="error" showIcon title="Malformed call reference" description={parseError} />
        </Layout.Content>
      </Layout>
    );
  }

  const traceHref = (() => {
    const sp = new URLSearchParams();
    if (hints.tsMs !== null) sp.set('ts_ms', String(hints.tsMs));
    if (hints.retentionClass !== null) sp.set('retention_class', hints.retentionClass);
    const qs = sp.toString();
    return `/api/v1/calls/${encodeURIComponent(pkToPath(pk))}/trace${qs === '' ? '' : `?${qs}`}`;
  })();

  return (
    <Layout style={{ height: '100vh' }}>
      <Layout.Header
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 16,
          color: '#fff',
          height: 'auto',
          minHeight: 64,
          paddingTop: 12,
          paddingBottom: 12,
        }}
      >
        <Space wrap style={{ minWidth: 0, flex: '1 1 auto' }}>
          <Typography.Title
            level={5}
            title={`${pk.pod_namespace} / ${pk.pod_service} / ${pk.pod_name}`}
            style={{
              margin: 0,
              color: '#fff',
              maxWidth: 'min(60vw, 520px)',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {pk.pod_namespace} / {pk.pod_service} / {pk.pod_name}
          </Typography.Title>
          {httpContext !== null ? (
            <Tag color="green" title="From the root call's web.method/web.url params">
              {httpContext}
            </Tag>
          ) : null}
          {hints.tsMs !== null ? <Tag>{formatTs(hints.tsMs)}</Tag> : null}
          {model !== null ? <Tag color="blue">{formatDurationMs(model.root.durationMs)}</Tag> : null}
          {hints.retentionClass !== null ? <Tag>{hints.retentionClass}</Tag> : null}
        </Space>
        <Space wrap style={{ flex: '0 0 auto' }}>
          <Button
            size="small"
            disabled={model === null}
            title={model === null ? 'No tree loaded yet — nothing to adjust' : undefined}
            onClick={() => setAdjustModal(adjustText)}
          >
            Adjust duration
          </Button>
          <Button
            size="small"
            disabled={model === null}
            title={model === null ? 'No tree loaded yet — nothing to categorize' : undefined}
            onClick={() => setCategoryModal(categoryText)}
          >
            Setup categories
          </Button>
          <Button
            size="small"
            icon={<DownloadOutlined />}
            disabled={model === null}
            title={model === null ? 'No trace located for this call yet' : undefined}
            href={model === null ? undefined : traceHref}
            download
          >
            Raw trace
          </Button>
        </Space>
      </Layout.Header>
      <Layout.Content style={{ padding: '8px 16px', display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {state.kind === 'loading' ? (
          <div style={{ display: 'grid', placeItems: 'center', flex: 1 }}>
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
            <Space orientation="vertical" style={{ width: '100%' }} size={4}>
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
                  description="Branches are still expandable by hand; the raw trace download carries full fidelity."
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
              style={{ flex: 1, minHeight: 0 }}
              type="editable-card"
              hideAdd
              activeKey={activeTab}
              onChange={setActiveTab}
              onEdit={(key, action) => {
                if (action === 'remove' && typeof key === 'string') closeOpTab(key);
              }}
              items={[
                {
                  key: 'tree',
                  label: 'Call Tree',
                  closable: false,
                  children: (
                    <div style={{ height: 'calc(100vh - 200px)' }}>
                      <TreeView model={model} onCapped={setCapped} ops={ops} />
                    </div>
                  ),
                },
                {
                  key: 'hotspots',
                  label: 'Hotspots',
                  closable: false,
                  children: (
                    <div style={{ height: 'calc(100vh - 200px)' }}>
                      <HotspotsView model={model} profiles={profiles} />
                    </div>
                  ),
                },
                {
                  key: 'params',
                  label: 'Parameters',
                  closable: false,
                  children: (
                    <Table
                      size="small"
                      rowKey={(r) => `${r.keyIdx}:${r.value}`}
                      dataSource={paramStats}
                      pagination={{ pageSize: 50, showSizeChanger: false }}
                      columns={[
                        {
                          title: 'Key',
                          width: 160,
                          render: (_, r) => <Tag>{model.paramKeys[r.keyIdx] ?? r.keyIdx}</Tag>,
                        },
                        {
                          title: 'Value',
                          render: (_, r) => (
                            <Typography.Text ellipsis style={{ maxWidth: 640 }} title={r.value}>
                              {r.value}
                              {r.unresolved ? (
                                <Tag color="orange" style={{ marginLeft: 8 }}>
                                  unresolved
                                </Tag>
                              ) : null}
                            </Typography.Text>
                          ),
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
                  label: <span title={v.fullTitle}>{v.label}</span>,
                  children: hasNoOccurrences(v.derived.root) ? (
                    <Empty description="The method does not occur in this tree." />
                  ) : v.profiles !== null ? (
                    <div style={{ height: 'calc(100vh - 200px)' }}>
                      <HotspotsView key={v.seq} model={v.derived} profiles={v.profiles} />
                    </div>
                  ) : (
                    <div style={{ height: 'calc(100vh - 200px)' }}>
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
              style={{ marginTop: 8 }}
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
              style={{ marginTop: 8 }}
              title={`Line${categoryModalInvalidLines.length === 1 ? '' : 's'} ${categoryModalInvalidLines.join(', ')} will be ignored`}
              description="Expected <category> <method pattern>, for example db >s.Dao.select*."
            />
          ) : null}
        </Modal>
      </Layout.Content>
    </Layout>
  );
}

function hasNoOccurrences(root: TreeNode): boolean {
  return root.selfExecutions === 0 && root.childExecutions === 0 && root.children.length === 0;
}
