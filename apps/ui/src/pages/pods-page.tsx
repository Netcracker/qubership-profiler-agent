import { Alert, App, Badge, Button, Empty, Layout, Space, Table, Typography } from 'antd';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';

import { useConfig } from '../api/use-config';
import { FilterBar } from '../calls/filter-bar/filter-bar';
import { selectionTokens } from '../calls/filter-bar/selection-token';
import { formatTs } from '../calls/format';
import { PeriodControls } from '../controls/period-controls';
import { podValueSource } from '../discovery/pod-values';
import { usePods } from '../discovery/use-pods';
import type { Condition, Operator } from '../filter/condition';
import { formatCondition } from '../filter/grammar';
import type { PodSelection } from '../filter/legacy';
import { UNSUPPORTED_REASON, translateConditions } from '../filter/translate';
import { HeaderSlot } from '../shell/header-slot';
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

// Pods Info (09 §4): the pod-restarts behind the current filters. Opening
// this screen directly must not require a detour through Calls first — it
// carries the same period picker and a filter bar limited to namespace,
// service, and pod, sharing the URL scheme with Calls (09 §6). Dumps stay in
// dumps-collector; the per-row link-out lands with the link-template seam
// (07 §3.4).

/** The fields Pods Info filters by; the other conditions in the URL belong to Calls and stay untouched. */
const POD_FIELDS: readonly string[] = ['namespace', 'service', 'pod'];
const isPodCondition = (c: Condition): boolean => POD_FIELDS.includes(c.field);

/** The table filters by pod tuples, so only `=` and `in` apply. */
function podFieldReason(field: string, op?: Operator): string | null {
  if (!POD_FIELDS.includes(field)) return 'Pods Info filters by namespace, service, and pod only.';
  return op === undefined || op === '=' || op === 'in' ? null : UNSUPPORTED_REASON;
}

const filtersKey = (cs: readonly Condition[], selection: PodSelection | null): string =>
  JSON.stringify([cs.map(formatCondition), selection]);

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
  // The App's toast renders in the app's own React tree and unmounts with it.
  // The static `message` mounts a separate root that nothing unmounts.
  const { message } = App.useApp();
  const [searchParams, setSearchParams] = useSearchParams();
  // One `now` per URL change anchors relative tokens; the memo freezes it.
  const search = useMemo(() => parseCallsSearch(searchParams, Date.now()), [searchParams]);
  // The app-wide display zone: the pod table's timestamps follow it like the
  // calls table and the call tree do (the URL stays Unix ms).
  const zone = useZone();

  const applied = useMemo(() => search.conditions.filter(isPodCondition), [search.conditions]);
  const [draft, setDraft] = useState<Condition[]>(applied);
  // The selection of an old link shows as a token until it is removed.
  const [draftSelection, setDraftSelection] = useState<PodSelection | null>(search.selection);
  // Resync the draft when the applied filters change under it (back/forward, a shared link).
  const appliedKey = filtersKey(applied, search.selection);
  useEffect(() => {
    setDraft(applied);
    setDraftSelection(search.selection);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [appliedKey]);
  // Apply has work only when the draft differs from the applied filters.
  const dirty = filtersKey(draft, draftSelection) !== appliedKey;

  const { state, refetch } = usePods(search.fromMs, search.toMs);
  const namespaces = state.kind === 'ready' ? state.namespaces : null;
  const values = useMemo(() => podValueSource(namespaces), [namespaces]);
  const configState = useConfig();
  const dumpsCollectorUrl = configState.kind === 'ready' ? configState.config.dumps_collector_url : '';
  const dumpsEnabled = isHttpUrl(dumpsCollectorUrl);

  const draftReasons = useMemo(
    () => new Map(translateConditions(draft, namespaces).skipped.map((s) => [s.condition, s.reason])),
    [draft, namespaces],
  );
  const tokenReason = useCallback((c: Condition) => draftReasons.get(c) ?? null, [draftReasons]);

  const commitSearch = (next: CallsSearchState): void => setSearchParams(callsSearchToParams(next));

  // A range change stores its raw tokens and applies at once.
  const commitRange = (tokens: { from: string; to: string }): void =>
    commitSearch({ ...search, from: tokens.from || null, to: tokens.to || null });

  // Apply writes the draft in place of the applied pod filters and keeps the conditions only Calls uses.
  const apply = (): void => {
    commitSearch({
      ...search,
      conditions: [...search.conditions.filter((c) => !isPodCondition(c)), ...draft],
      selection: draftSelection,
    });
    refetch();
  };

  // `y` freezes a live relative range into an absolute permalink (GitHub-style).
  usePermalinkHotkey(hasRelativeWindow(search), () => {
    commitSearch(freezeWindow(search));
    message.success('Time range pinned to a permalink');
  });

  const rows = useMemo<PodRow[]>(() => {
    if (state.kind !== 'ready') return [];
    // The pod filters expand to pod tuples the same way Calls sends them; with none, every pod-restart shows.
    const selected = translateConditions(applied, state.namespaces, search.selection).params?.pods;
    const tuples = selected === undefined ? null : new Set(selected);
    const out: PodRow[] = [];
    for (const ns of state.namespaces) {
      for (const svc of ns.services) {
        for (const pod of svc.pods) {
          if (tuples !== null && !tuples.has(pod.tuple)) continue;
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
  }, [state, applied, search.selection]);

  return (
    <Layout className={styles.page}>
      <Layout.Content className={styles.content}>
        <HeaderSlot>
          <PeriodControls
            value={{ from: search.from, to: search.to, fromMs: search.fromMs, toMs: search.toMs }}
            onCommitRange={commitRange}
          />
        </HeaderSlot>
        <div className={styles.controls}>
          <div className={styles.bar}>
            <FilterBar
              conditions={draft}
              onChange={setDraft}
              presets={selectionTokens(draftSelection, () => setDraftSelection(null))}
              fields={POD_FIELDS}
              supported={podFieldReason}
              tokenReason={tokenReason}
              values={values}
              onRun={apply}
              placeholder="Filter pods: pick namespace, service, or pod"
            />
          </div>
          {/* Enabled only when there is unapplied work, so grey means "everything is applied". */}
          <Button
            type="primary"
            onClick={apply}
            loading={state.kind === 'loading'}
            disabled={search.fromMs === null || search.toMs === null || !dirty}
          >
            Apply
          </Button>
        </div>
        {search.fromMs === null || search.toMs === null ? (
          <Empty className={styles.emptyTop} description="Pick a time range to see pods." />
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
