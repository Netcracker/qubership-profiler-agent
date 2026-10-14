import { useCallback, useEffect, useRef, useState } from 'react';

export type Loadable<T> = { kind: 'loading' } | { kind: 'ready'; data: T } | { kind: 'error'; message: string };

export interface LoadableResult<T> {
  state: Loadable<T>;
  /** Loads again and keeps showing the current data until the answer arrives. */
  refetch: () => void;
  /** Replaces the data with an answer the caller already has, such as the body of a save. */
  set: (data: T) => void;
}

/**
 * Loads one resource of the agent-configuration API and reloads it when `key`
 * changes. A new key shows `loading` at once, so the data of the previous key
 * never appears under the new one. A refetch replaces the data in place, which
 * lets a poll run without a flicker.
 */
export function useLoadable<T>(key: string, load: (signal: AbortSignal) => Promise<T>): LoadableResult<T> {
  const [stored, setStored] = useState<{ key: string; state: Loadable<T> }>({ key, state: { kind: 'loading' } });
  const [epoch, setEpoch] = useState(0);
  // The caller passes a new closure on every render; only `key` and a refetch
  // start a load.
  const loadRef = useRef(load);
  loadRef.current = load;

  useEffect(() => {
    const controller = new AbortController();
    loadRef
      .current(controller.signal)
      .then((data) => setStored({ key, state: { kind: 'ready', data } }))
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setStored({ key, state: { kind: 'error', message: e instanceof Error ? e.message : String(e) } });
      });
    return () => controller.abort();
  }, [key, epoch]);

  const refetch = useCallback(() => setEpoch((n) => n + 1), []);
  const set = useCallback((data: T) => setStored({ key, state: { kind: 'ready', data } }), [key]);
  return { state: stored.key === key ? stored.state : { kind: 'loading' }, refetch, set };
}
