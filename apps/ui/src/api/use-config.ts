import { createContext, createElement, useCallback, useContext, useEffect, useState } from 'react';
import type { ReactNode } from 'react';

import { fetchConfig } from './endpoints';
import type { ConfigResponse } from './types';

export type ConfigState =
  | { kind: 'loading' }
  | { kind: 'ready'; config: ConfigResponse }
  /** `retry` fetches /config again, for every reader of this state. */
  | { kind: 'error'; message: string; retry: () => void };

const ConfigContext = createContext<ConfigState | undefined>(undefined);

function useFetchedConfig(enabled: boolean): ConfigState {
  const [state, setState] = useState<ConfigState>({ kind: 'loading' });
  const [epoch, setEpoch] = useState(0);
  const retry = useCallback(() => setEpoch((n) => n + 1), []);

  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    setState({ kind: 'loading' });
    fetchConfig(controller.signal)
      .then((config) => setState({ kind: 'ready', config }))
      .catch((e: unknown) => {
        if (controller.signal.aborted) return;
        setState({ kind: 'error', message: e instanceof Error ? e.message : String(e), retry });
      });
    return () => controller.abort();
  }, [enabled, epoch, retry]);

  return state;
}

/**
 * Fetches /config once for everything below it. The routes, the navigation
 * rail, and the pages all read the same answer through {@link useConfig}.
 */
export function ConfigProvider({ children }: { children: ReactNode }) {
  return createElement(ConfigContext.Provider, { value: useFetchedConfig(true) }, children);
}

/**
 * The deployment-specific /config values. Under a {@link ConfigProvider} this
 * reads the shared answer. Without one, as in a page test, it fetches its own.
 * With `enabled` false and no provider it fetches nothing and stays in
 * `loading`; an exported offline copy has no backend to ask.
 */
export function useConfig(enabled = true): ConfigState {
  const shared = useContext(ConfigContext);
  const own = useFetchedConfig(enabled && shared === undefined);
  return shared ?? own;
}
