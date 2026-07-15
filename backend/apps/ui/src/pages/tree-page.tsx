import { DownloadOutlined } from '@ant-design/icons';
import { Alert, Button, Drawer, Empty, Input, Layout, Modal, Result, Space, Spin, Table, Tabs, Tag, Typography } from 'antd';
import { useMemo, useState } from 'react';
import { useParams, useSearchParams } from 'react-router';

import { parsePkPath, pkToPath } from '../api/pk';
import type { CallPK } from '../api/types';
import { formatCount, formatDurationMs, formatTs } from '../calls/format';
import { HotspotsView, nodeTitle } from '../tree/hotspots-view';
import { buildTreeModel } from '../tree/model';
import type { TreeModel, TreeNode } from '../tree/model';
import { summariseParams } from '../tree/params-summary';
import { applyAdjustments, factorByMethod, parseAdjustConfig } from '../tree/transforms/adjust';
import { applyCategories, parseCategoryConfig } from '../tree/transforms/categories';
import { computeFlatProfile } from '../tree/transforms/flat-profile';
import type { CategoryProfile } from '../tree/transforms/flat-profile';
import { findUsages, incomingCalls, outgoingCalls } from '../tree/transforms/merge';
import { TreeView } from '../tree/tree-view';
import type { TreeViewOps } from '../tree/tree-view';
import { useTree } from '../tree/use-tree';
import { parseTreeSearch } from '../url/search-params';

// Call Tree route (09 §3): tabs Call Tree · Hotspots · Parameters, the
// per-node operations backed by the 5.3 transforms, and the Adjust duration
// / Setup categories configs. The model rebuilds from the decoded wire on
// every config change, so the transforms stay pure and re-applicable.

type OpResult =
  | { kind: 'tree'; title: string; model: TreeModel }
  | { kind: 'hotspots'; title: string; model: TreeModel; profiles: CategoryProfile[] };

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
  const [op, setOp] = useState<OpResult | null>(null);

  const wire = state.kind === 'ready' ? state.wire : null;
  const model = useMemo(() => {
    if (wire === null) return null;
    const m = buildTreeModel(wire);
    const adjustRules = parseAdjustConfig(adjustText);
    if (adjustRules.length > 0) applyAdjustments(m, factorByMethod(m, adjustRules));
    applyCategories(m, categoryText.trim() === '' ? null : parseCategoryConfig(categoryText));
    return m;
  }, [wire, adjustText, categoryText]);

  const profiles = useMemo(() => (model === null ? [] : computeFlatProfile(model)), [model]);
  const paramStats = useMemo(() => (model === null ? [] : summariseParams(model)), [model]);

  const openIncoming = (methodIdx: number, category?: string): void => {
    if (model === null) return;
    const result = incomingCalls(model, methodIdx, category);
    setOp({ kind: 'tree', title: `Incoming calls · ${nodeTitle(result, result.root)}`, model: result });
  };

  const ops: TreeViewOps = {
    incoming: (node) => openIncoming(node.methodIdx),
    outgoing: (node) => {
      if (model === null) return;
      const result = outgoingCalls(model, node.methodIdx);
      setOp({ kind: 'tree', title: `Outgoing calls · ${nodeTitle(model, node)}`, model: result });
    },
    findUsages: (node) => {
      if (model === null) return;
      const result = findUsages(model, node.methodIdx);
      setOp({ kind: 'tree', title: `Find usages · ${nodeTitle(model, node)}`, model: result });
    },
    localHotspots: (node) => {
      if (model === null) return;
      const scoped = outgoingCalls(model, node.methodIdx);
      setOp({
        kind: 'hotspots',
        title: `Local hotspots · ${nodeTitle(model, node)}`,
        model: scoped,
        profiles: computeFlatProfile(scoped),
      });
    },
    adjust: (node) => {
      if (model === null) return;
      const method = model.methods[node.methodIdx] ?? '';
      const factor = node.selfExecutions > 1 ? node.selfExecutions : 10000;
      setAdjustModal(`${adjustText === '' ? '' : `${adjustText}\n`}1/${factor} ${method}`);
    },
    addCategory: (node) => {
      if (model === null) return;
      const method = model.methods[node.methodIdx] ?? '';
      const name = nodeTitle(model, node).split('(')[0]!.split('.').slice(-2).join('_').replace(/\s/g, '');
      setCategoryModal(`${categoryText === '' ? '' : `${categoryText}\n`}${name} ${method}`);
    },
  };

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
      <Layout.Header style={{ display: 'flex', alignItems: 'center', gap: 16, color: '#fff' }}>
        <Typography.Title level={5} style={{ margin: 0, color: '#fff', whiteSpace: 'nowrap' }}>
          {pk.pod_namespace} / {pk.pod_service} / {pk.pod_name}
        </Typography.Title>
        <Space>
          {hints.tsMs !== null ? <Tag>{formatTs(hints.tsMs)}</Tag> : null}
          {model !== null ? <Tag color="blue">{formatDurationMs(model.root.durationMs)}</Tag> : null}
          {hints.retentionClass !== null ? <Tag>{hints.retentionClass}</Tag> : null}
        </Space>
        <span style={{ flex: 1 }} />
        <Space>
          <Button size="small" onClick={() => setAdjustModal(adjustText)}>
            Adjust duration
          </Button>
          <Button size="small" onClick={() => setCategoryModal(categoryText)}>
            Setup categories
          </Button>
          <Button size="small" icon={<DownloadOutlined />} href={traceHref} download>
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
              {adjustText.trim() !== '' ? (
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
              defaultActiveKey="tree"
              items={[
                {
                  key: 'tree',
                  label: 'Call Tree',
                  children: (
                    <div style={{ height: 'calc(100vh - 200px)' }}>
                      <TreeView model={model} onCapped={setCapped} ops={ops} />
                    </div>
                  ),
                },
                {
                  key: 'hotspots',
                  label: 'Hotspots',
                  children: (
                    <div style={{ height: 'calc(100vh - 200px)', overflow: 'auto' }}>
                      <HotspotsView model={model} profiles={profiles} onIncoming={(idx, cat) => openIncoming(idx, cat)} />
                    </div>
                  ),
                },
                {
                  key: 'params',
                  label: 'Parameters',
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
              ]}
            />
          </>
        ) : null}

        <Drawer
          open={op !== null}
          onClose={() => setOp(null)}
          title={op?.title}
          width="75%"
          destroyOnHidden
        >
          {op === null ? null : hasNoOccurrences(op.model.root) ? (
            <Empty description="The method does not occur in this tree." />
          ) : op.kind === 'tree' ? (
            <div style={{ height: 'calc(100vh - 140px)' }}>
              <TreeView model={op.model} />
            </div>
          ) : (
            <HotspotsView model={op.model} profiles={op.profiles} />
          )}
        </Drawer>

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
        </Modal>
      </Layout.Content>
    </Layout>
  );
}

function hasNoOccurrences(root: TreeNode): boolean {
  return root.selfExecutions === 0 && root.childExecutions === 0 && root.children.length === 0;
}
