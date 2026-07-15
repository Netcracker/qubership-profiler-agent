import { Alert, Badge, Empty, Layout, Table, Typography } from 'antd';
import { useMemo } from 'react';
import { useSearchParams } from 'react-router';

import { formatTs } from '../calls/format';
import { usePods } from '../discovery/use-pods';
import { parseCallsSearch } from '../url/search-params';

// Pods Info (09 §4): the pod-restarts behind the current selection. Dumps
// stay in dumps-collector; the per-row link-out lands with the link-template
// seam (07 §3.4).

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
  const [searchParams] = useSearchParams();
  const search = useMemo(() => parseCallsSearch(searchParams), [searchParams]);
  const { state } = usePods(search.fromMs, search.toMs);

  const rows = useMemo<PodRow[]>(() => {
    if (state.kind !== 'ready') return [];
    const out: PodRow[] = [];
    for (const ns of state.namespaces) {
      for (const svc of ns.services) {
        if (search.services.length > 0 && !search.services.includes(svc.key)) continue;
        for (const pod of svc.pods) {
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
  }, [state, search.services]);

  if (search.fromMs === null || search.toMs === null) {
    return (
      <Layout.Content style={{ display: 'grid', placeItems: 'center', padding: 24 }}>
        <Empty description="Pick a period on the Calls tab first — the pod list needs a window." />
      </Layout.Content>
    );
  }

  return (
    <Layout.Content style={{ padding: '12px 16px' }}>
      {state.kind === 'error' ? <Alert type="error" showIcon title="Cannot load pods" description={state.message} /> : null}
      {state.kind === 'ready' && state.partial ? (
        <Alert type="warning" showIcon title="Pod list may be incomplete" description={state.partialReasons.join('; ')} />
      ) : null}
      <Table<PodRow>
        size="small"
        loading={state.kind === 'loading'}
        dataSource={rows}
        pagination={false}
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
          { title: 'Restart', key: 'restart', width: 200, render: (_, r) => formatTs(r.restartTimeMs) },
          {
            title: 'Data range',
            key: 'range',
            width: 380,
            render: (_, r) => `${formatTs(r.timeMinMs)} — ${formatTs(r.timeMaxMs)}`,
          },
          {
            title: 'State',
            key: 'state',
            width: 100,
            render: (_, r) => (r.live ? 'live' : 'closed'),
          },
        ]}
      />
    </Layout.Content>
  );
}
