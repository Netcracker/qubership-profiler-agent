import { createContext, createElement, useContext, useEffect, useState } from 'react';
import type { ReactNode } from 'react';

import { fetchConfig } from './endpoints';
import type { ConfigResponse } from './types';

export type ConfigState = { kind: 'loading' } | { kind: 'ready'; config: ConfigResponse } | { kind: 'error' };

const ConfigContext = createContext<ConfigState | undefined>(undefined);

function useFetchedConfig(enabled: boolean): ConfigState {
  const [state, setState] = useState<ConfigState>({ kind: 'loading' });

  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    fetchConfig(controller.signal)
      .then((config) => setState({ kind: 'ready', config }))
      .catch(() => {
        if (controller.signal.aborted) return;
        setState({ kind: 'error' });
      });
    return () => controller.abort();
  }, [enabled]);

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
 */
export function useConfig(): ConfigState {
  const shared = useContext(ConfigContext);
  const own = useFetchedConfig(shared === undefined);
  return shared ?? own;
}
