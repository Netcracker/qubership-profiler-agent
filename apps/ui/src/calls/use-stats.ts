import { useCallback, useEffect, useState } from 'react';

import { ApiError, isWideQueryRejection } from '../api/client';
import { fetchStats } from '../api/endpoints';
import type { StatsQuery } from '../api/endpoints';
import type { StatsResponse } from '../api/types';

// One /stats request for the applied query (02 §2.9). It runs beside the /calls fetch and never blocks the table:
// a failure here is the chart's own state (redesign plan §4.8).

export type StatsState =
  | { kind: 'idle' }
  | { kind: 'loading' }
  | { kind: 'ready'; data: StatsResponse }
  /** No chart: the backend has no /stats (404), or the query is too wide, which the table's banner explains. */
  | { kind: 'hidden' }
  | { kind: 'error'; message: string };

export interface UseStatsResult {
  state: StatsState;
  /** Requests the same query again. */
  refetch: () => void;
}

function queryKey(q: StatsQuery | null): string | null {
  return q === null ? null : JSON.stringify([q.fromMs, q.toMs, q.filters ?? [], q.pods ?? [], q.groupBy, q.buckets, q.limit, q.order]);
}

export function useStats(query: StatsQuery | null): UseStatsResult {
  const [state, setState] = useState<StatsState>({ kind: 'idle' });
  const [epoch, setEpoch] = useState(0);
  const key = queryKey(query);

  useEffect(() => {
    if (query === null) {
      setState({ kind: 'idle' });
      return;
    }
    const controller = new AbortController();
    setState({ kind: 'loading' });
    fetchStats(query, controller.signal)
      .then((data) => setState({ kind: 'ready', data }))
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        if ((e instanceof ApiError && e.status === 404) || isWideQueryRejection(e)) setState({ kind: 'hidden' });
        else setState({ kind: 'error', message: e instanceof Error ? e.message : String(e) });
      });
    return () => controller.abort();
    // `key` serializes every field the fetch reads; `query` is a fresh object each render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, epoch]);

  const refetch = useCallback(() => setEpoch((n) => n + 1), []);
  return { state, refetch };
}
