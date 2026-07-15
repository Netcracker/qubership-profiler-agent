import { useEffect, useState } from 'react';

import { fetchConfig } from './endpoints';
import type { ConfigResponse } from './types';

export type ConfigState = { kind: 'loading' } | { kind: 'ready'; config: ConfigResponse } | { kind: 'error' };

/** Fetches the deployment-specific /config values once per app session. */
export function useConfig(): ConfigState {
  const [state, setState] = useState<ConfigState>({ kind: 'loading' });

  useEffect(() => {
    const controller = new AbortController();
    fetchConfig(controller.signal)
      .then((config) => setState({ kind: 'ready', config }))
      .catch(() => {
        if (controller.signal.aborted) return;
        setState({ kind: 'error' });
      });
    return () => controller.abort();
  }, []);

  return state;
}
