import { useCallback, useEffect, useRef, useState } from 'react';

import { fetchPods } from '../api/endpoints';
import { groupPods } from './group-pods';
import type { NamespaceNode } from './group-pods';

export type PodsState =
  | { kind: 'idle' }
  | { kind: 'loading' }
  | { kind: 'ready'; namespaces: NamespaceNode[]; partial: boolean; partialReasons: string[] }
  | { kind: 'error'; message: string };

/**
 * Loads /pods for the draft window as soon as the window changes: the rail
 * must be selectable before the first Apply, and /pods reads manifests, not
 * parquet — the expensive /calls fan-out stays Apply-gated (09 §2.2).
 */
export function usePods(fromMs: number | null, toMs: number | null): { state: PodsState; refetch: () => void } {
  const [state, setState] = useState<PodsState>({ kind: 'idle' });
  const [epoch, setEpoch] = useState(0);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    abortRef.current?.abort();
    if (fromMs === null || toMs === null) {
      setState({ kind: 'idle' });
      return;
    }
    const controller = new AbortController();
    abortRef.current = controller;
    setState({ kind: 'loading' });
    fetchPods(fromMs, toMs, controller.signal)
      .then((res) => {
        setState({
          kind: 'ready',
          namespaces: groupPods(res.pods, Date.now()),
          partial: res.partial,
          partialReasons: res.partial_reasons,
        });
      })
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setState({ kind: 'error', message: e instanceof Error ? e.message : String(e) });
      });
    return () => controller.abort();
  }, [fromMs, toMs, epoch]);

  const refetch = useCallback(() => setEpoch((n) => n + 1), []);
  return { state, refetch };
}
