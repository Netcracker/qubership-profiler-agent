import { Alert, Badge, Empty, Layout, Space, Table, Typography, message } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';

import { useConfig } from '../api/use-config';
import { formatTs } from '../calls/format';
import { PeriodControls } from '../controls/period-controls';
import type { DraftWindow } from '../controls/period-controls';
import { resolveUrlTime } from '../controls/time-range';
import { DiscoveryRail, selectionEquals } from '../discovery/discovery-rail';
import type { RailSelection } from '../discovery/discovery-rail';
import { usePods } from '../discovery/use-pods';
import { useZone } from '../ui/timezone';
import { callsSearchToParams, freezeWindow, hasRelativeWindow, parseCallsSearch } from '../url/search-params';
import type { CallsSearchState } from '../url/search-params';
import { usePermalinkHotkey } from '../url/use-permalink-hotkey';
import styles from './pods-page.module.css';

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
  // One `now` per URL change anchors relative tokens; the memo freezes it.
  const search = useMemo(() => parseCallsSearch(searchParams, Date.now()), [searchParams]);
  // The app-wide display zone: the pod table's timestamps follow it like the
  // calls table and the call tree do (the URL stays Unix ms).
  const zone = useZone();

  const [draftWindow, setDraftWindow] = useState<DraftWindow>({ fromMs: search.fromMs, toMs: search.toMs });
  const [draftSelection, setDraftSelection] = useState<RailSelection>({
    services: search.services,
    pods: search.pods,
  });
  // Reserve a gutter for the collapsed rail's top-left trigger, as CallsPage
  // does (PR 708 review #8).
  const [railCollapsed, setRailCollapsed] = useState(false);

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

  // A range change stores its raw tokens and applies at once; the rail follows
  // via the draft window without waiting for the URL round-trip.
  const commitRange = (tokens: { from: string; to: string }): void => {
    const now = Date.now();
    setDraftWindow({ fromMs: resolveUrlTime(tokens.from, now), toMs: resolveUrlTime(tokens.to, now) });
    commitSearch({
      ...search,
      from: tokens.from || null,
      to: tokens.to || null,
      services: draftSelection.services,
      pods: draftSelection.pods,
    });
  };

  // The sibling Apply commits rail-selection edits against the committed window.
  const apply = (): void =>
    commitSearch({ ...search, services: draftSelection.services, pods: draftSelection.pods });
  // Apply has work only when the draft selection diverges from the committed one.
  const selectionDirty = !selectionEquals(draftSelection, search);

  // `y` freezes a live relative range into an absolute permalink (GitHub-style).
  usePermalinkHotkey(hasRelativeWindow(search), () => {
    commitSearch(freezeWindow(search));
    message.success('Time range pinned to a permalink');
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
    <Layout className={styles.page}>
      <Layout.Sider
        width={320}
        theme="light"
        breakpoint="lg"
        collapsedWidth={0}
        onCollapse={setRailCollapsed}
        zeroWidthTriggerStyle={{ top: 0, left: 0, zIndex: 20 }}
        className={styles.sider}
      >
        <DiscoveryRail pods={railPodsState} selection={draftSelection} onSelectionChange={setDraftSelection} />
      </Layout.Sider>
      <Layout.Content className={`${styles.content}${railCollapsed ? ` ${styles.railGutter}` : ''}`}>
        <PeriodControls
          value={{ from: search.from, to: search.to, fromMs: search.fromMs, toMs: search.toMs }}
          onCommitRange={commitRange}
          onApply={apply}
          canApply={selectionDirty}
          applying={state.kind === 'loading'}
        />
        {search.fromMs === null || search.toMs === null ? (
          <Empty className={styles.emptyTop} description="Pick a period and Apply to see pods." />
        ) : (
          <>
            {state.kind === 'error' ? (
              <Alert type="error" showIcon title="Cannot load pods" description={state.message} />
            ) : null}
            {state.kind === 'ready' && state.partial ? (
              <Alert type="warning" showIcon title="Pod list may be incomplete" description={state.partialReasons.join('; ')} />
            ) : null}
            {/* Say why the dump links are missing rather than leaving a silent
                gap: the link-out only exists when the operator points this
                deployment at a dumps-collector (values.yaml query.dumpsCollectorUrl). */}
            {state.kind === 'ready' && !dumpsEnabled && rows.length > 0 ? (
              <Alert
                type="info"
                showIcon
                title="Thread and top dump links are hidden"
                description="This deployment has no dumps-collector configured. Set query.dumpsCollectorUrl in the chart values to link a thread and top dump per pod-restart."
              />
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
                { title: 'Session start', key: 'restart', width: 200, render: (_, r) => formatTs(r.restartTimeMs, zone) },
                {
                  title: 'Data range',
                  key: 'range',
                  width: 380,
                  render: (_, r) => `${formatTs(r.timeMinMs, zone)} — ${formatTs(r.timeMaxMs, zone)}`,
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
