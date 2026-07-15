import { useCallback, useEffect, useRef, useState } from 'react';

import { ApiError } from '../api/client';
import { fetchTree } from '../api/endpoints';
import type { TreeHints } from '../api/endpoints';
import type { CallPK } from '../api/types';
import type { TreeWire } from '../msgpack/tree-wire';

// Tree loading with the 09 §5 point-fetch states told apart by the backend's
// problem titles (tree.go pointProblem): "call not found" is the cold miss,
// "trace blob unavailable" is a truncated blob. The page derives the model
// from the wire itself, so adjust/category configs rebuild it cheaply.

export type TreeState =
  | { kind: 'loading' }
  | { kind: 'cold'; detail: string }
  | { kind: 'truncated'; detail: string }
  | { kind: 'error'; message: string }
  | { kind: 'ready'; wire: TreeWire };

export function useTree(pk: CallPK | null, hints: TreeHints): { state: TreeState; refetch: () => void } {
  const [state, setState] = useState<TreeState>({ kind: 'loading' });
  const [epoch, setEpoch] = useState(0);
  const abortRef = useRef<AbortController | null>(null);

  const hintsKey = `${hints.tsMs ?? ''}:${hints.retentionClass ?? ''}`;
  useEffect(() => {
    abortRef.current?.abort();
    if (pk === null) return;
    const controller = new AbortController();
    abortRef.current = controller;
    setState({ kind: 'loading' });
    fetchTree(pk, hints, controller.signal)
      .then((wire) => setState({ kind: 'ready', wire }))
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        if (e instanceof ApiError && e.status === 404) {
          if (e.problem?.title === 'trace blob unavailable') {
            setState({ kind: 'truncated', detail: e.problem.detail ?? '' });
            return;
          }
          setState({ kind: 'cold', detail: e.problem?.detail ?? '' });
          return;
        }
        setState({ kind: 'error', message: e instanceof Error ? e.message : String(e) });
      });
    return () => controller.abort();
    // hintsKey covers hints; pk identity is stable per route.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pk === null ? '' : JSON.stringify(pk), hintsKey, epoch]);

  const refetch = useCallback(() => setEpoch((n) => n + 1), []);
  return { state, refetch };
}
