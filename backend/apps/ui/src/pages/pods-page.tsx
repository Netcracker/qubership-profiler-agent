import { Alert, Badge, Empty, Layout, Space, Table, Typography } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';

import { useConfig } from '../api/use-config';
import { formatTs } from '../calls/format';
import { PeriodControls } from '../controls/period-controls';
import type { DraftWindow } from '../controls/period-controls';
import { DiscoveryRail } from '../discovery/discovery-rail';
import type { RailSelection } from '../discovery/discovery-rail';
import { usePods } from '../discovery/use-pods';
import { callsSearchToParams, parseCallsSearch } from '../url/search-params';
import type { CallsSearchState } from '../url/search-params';

// dumps-collector (07 §3.4) is a separate deployment with its own ingress;
// its base URL only exists when the operator configured it (values.yaml
// query.dumpsCollectorUrl). Only td/top dumps have a download-by-window
// route — heap dumps need a listing/handle-discovery step that dumps-
// collector does not expose, so they stay out of this link-out (PR 708
// review #18).
// isHttpUrl gates the dump link-out on an absolute http(s) base. The backend
// already drops anything else from /api/v1/config (PR 708 review #10); this is
// the client-side backstop so a tampered config can never turn into a
// clickable javascript: href.
function isHttpUrl(raw: string): boolean {
  try {
    const u = new URL(raw);
    return u.protocol === 'http:' || u.protocol === 'https:';
  } catch {
    return false;
  }
}

function dumpDownloadUrl(dumpsCollectorUrl: string, type: 'td' | 'top', row: PodRow): string {
  const sp = new URLSearchParams({
    dateFrom: String(row.timeMinMs),
    dateTo: String(row.timeMaxMs),
    type,
    namespace: row.namespace,
    service: row.service,
    podName: row.pod,
  });
  return `${dumpsCollectorUrl}/cdt/v2/download?${sp.toString()}`;
}

// Pods Info (09 §4): the pod-restarts behind the current selection. Opening
// this screen directly must not require a detour through Calls first — it
// carries the same period picker + discovery rail (09 §2.1-2.2), sharing the
// URL scheme with Calls (09 §6). Dumps stay in dumps-collector; the per-row
// link-out lands with the link-template seam (07 §3.4).

interface PodRow {
  key: string;
  namespace: string;
  service: string;
  pod: string;
  restartTimeMs: number;
  timeMinMs: number;
  timeMaxMs: number;
  live: boolean;
}

export function PodsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const search = useMemo(() => parseCallsSearch(searchParams), [searchParams]);

  const [draftWindow, setDraftWindow] = useState<DraftWindow>({ fromMs: search.fromMs, toMs: search.toMs });
  const [draftSelection, setDraftSelection] = useState<RailSelection>({
    services: search.services,
    pods: search.pods,
  });

  // Resync drafts when the committed state changes under us (back/forward,
  // a shared link), mirroring CallsPage.
  const selectionKey = [...search.services, '|', ...search.pods].join(',');
  useEffect(() => {
    setDraftWindow({ fromMs: search.fromMs, toMs: search.toMs });
    setDraftSelection({ services: search.services, pods: search.pods });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search.fromMs, search.toMs, selectionKey]);

  // The rail follows the draft window, so services are selectable before the
  // first Apply; the pods table stays keyed on the committed URL.
  const { state: railPodsState } = usePods(draftWindow.fromMs, draftWindow.toMs);
  const { state } = usePods(search.fromMs, search.toMs);
  const configState = useConfig();
  const dumpsCollectorUrl = configState.kind === 'ready' ? configState.config.dumps_collector_url : '';
  const dumpsEnabled = isHttpUrl(dumpsCollectorUrl);

  const commitSearch = (next: CallsSearchState): void => setSearchParams(callsSearchToParams(next));
  const apply = (): void =>
    commitSearch({
      ...search,
      fromMs: draftWindow.fromMs,
      toMs: draftWindow.toMs,
      services: draftSelection.services,
      pods: draftSelection.pods,
    });

  const rows = useMemo<PodRow[]>(() => {
    if (state.kind !== 'ready') return [];
    // A selection filters the table; with none, every pod-restart shows. The
    // service and pod selections are disjoint by construction (the rail routes
    // a pod under a fully selected service into `services`, a lone pod into
    // `pods`), so a row survives when its service is selected or the pod tuple
    // itself is selected (PR 708 review #6).
    const filterActive = search.services.length > 0 || search.pods.length > 0;
    const out: PodRow[] = [];
    for (const ns of state.namespaces) {
      for (const svc of ns.services) {
        const svcSelected = search.services.includes(svc.key);
        for (const pod of svc.pods) {
          if (filterActive && !svcSelected && !search.pods.includes(pod.tuple)) continue;
          for (const restart of pod.restarts) {
            out.push({
              key: `${pod.tuple}@${restart.restart_time_ms}`,
              namespace: ns.namespace,
              service: svc.service,
              pod: pod.pod,
              restartTimeMs: restart.restart_time_ms,
              timeMinMs: restart.time_min_ms,
              timeMaxMs: restart.time_max_ms,
              live: pod.live && restart.restart_time_ms === pod.restarts[pod.restarts.length - 1]!.restart_time_ms,
            });
          }
        }
      }
    }
    return out;
  }, [state, search.services, search.pods]);

  return (
    <Layout style={{ flex: 1, minHeight: 0 }}>
      <Layout.Sider
        width={320}
        theme="light"
        breakpoint="lg"
        collapsedWidth={0}
        zeroWidthTriggerStyle={{ top: 0, left: 0, zIndex: 20 }}
        style={{ borderRight: '1px solid #f0f0f0' }}
      >
        <DiscoveryRail pods={railPodsState} selection={draftSelection} onSelectionChange={setDraftSelection} />
      </Layout.Sider>
      <Layout.Content style={{ padding: '12px 16px', display: 'flex', flexDirection: 'column', minWidth: 0 }}>
        <PeriodControls
          window={draftWindow}
          onWindowChange={setDraftWindow}
          onApply={apply}
          applying={state.kind === 'loading'}
        />
        {search.fromMs === null || search.toMs === null ? (
          <Empty style={{ marginTop: 48 }} description="Pick a period and Apply to see pods." />
        ) : (
          <>
            {state.kind === 'error' ? (
              <Alert type="error" showIcon title="Cannot load pods" description={state.message} />
            ) : null}
            {state.kind === 'ready' && state.partial ? (
              <Alert type="warning" showIcon title="Pod list may be incomplete" description={state.partialReasons.join('; ')} />
            ) : null}
            <Table<PodRow>
              size="small"
              loading={state.kind === 'loading'}
              dataSource={rows}
              pagination={false}
              scroll={{ x: 'max-content' }}
              columns={[
                {
                  title: 'Pod',
                  key: 'pod',
                  render: (_, r) => (
                    <span>
                      <Badge status={r.live ? 'success' : 'default'} />{' '}
                      <Typography.Text>
                        {r.namespace}/{r.service}/{r.pod}
                      </Typography.Text>
                    </span>
                  ),
                },
                // "Session start"/"Data freshness" name what these columns
                // actually carry — a profiler pod-restart and call-data
                // recency in the window — not a Kubernetes restart count or
                // pod phase (PR 708 review #17).
                { title: 'Session start', key: 'restart', width: 200, render: (_, r) => formatTs(r.restartTimeMs) },
                {
                  title: 'Data range',
                  key: 'range',
                  width: 380,
                  render: (_, r) => `${formatTs(r.timeMinMs)} — ${formatTs(r.timeMaxMs)}`,
                },
                {
                  title: 'Data freshness',
                  key: 'state',
                  width: 140,
                  render: (_, r) => (r.live ? 'recent data' : 'no recent data'),
                },
                // Only rendered when dumps-collector is configured for this
                // deployment (values.yaml query.dumpsCollectorUrl); otherwise
                // the link-out has nowhere to point (PR 708 review #18).
                ...(dumpsEnabled
                  ? [
                      {
                        title: 'Dumps',
                        key: 'dumps',
                        width: 160,
                        render: (_: unknown, r: PodRow) => (
                          <Space size="small">
                            <Typography.Link href={dumpDownloadUrl(dumpsCollectorUrl, 'td', r)} target="_blank" rel="noreferrer">
                              Thread dumps
                            </Typography.Link>
                            <Typography.Link href={dumpDownloadUrl(dumpsCollectorUrl, 'top', r)} target="_blank" rel="noreferrer">
                              Top dumps
                            </Typography.Link>
                          </Space>
                        ),
                      },
                    ]
                  : []),
              ]}
            />
          </>
        )}
      </Layout.Content>
    </Layout>
  );
}
