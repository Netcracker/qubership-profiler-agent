import { useCallback, useEffect, useMemo, useState } from 'react';

import { fetchAgentConfig } from './api';
import type { AgentConfigResponse, AgentConfigScopeId, AgentConfigSettings } from './types';
import { useLoadable } from './use-loadable';
import type { Loadable } from './use-loadable';

/** The unsaved edits of one scope. An absent field has no edit. */
interface Edits {
  scope: AgentConfigScopeId;
  settings?: AgentConfigSettings;
}

export interface ConfigEditor {
  /** The config as the server last answered it. */
  config: Loadable<AgentConfigResponse>;
  /** Loads the config again and drops the edits. */
  reload: () => void;
  /** The loaded settings with the edits on top; null until the config loads. */
  settings: AgentConfigSettings | null;
  /** Whether an apply would change what the server has. */
  dirty: boolean;
  editSettings: (next: AgentConfigSettings) => void;
  /** Drops the edits and shows the loaded config again. */
  discard: () => void;
}

/**
 * Loads the config in effect for a scope and holds the edits made to it. The
 * edits belong to the scope: opening another scope starts from its loaded
 * config, and the page asks before it lets that happen with edits pending.
 */
export function useConfigEditor(scope: AgentConfigScopeId): ConfigEditor {
  const { state: config, refetch } = useLoadable(`config:${scope}`, (signal) => fetchAgentConfig(scope, signal));
  const [stored, setStored] = useState<Edits>({ scope });
  // Edits of another scope are dropped for good, so they do not come back when
  // that scope is opened again.
  const edits = useMemo<Edits>(() => (stored.scope === scope ? stored : { scope }), [stored, scope]);
  useEffect(() => setStored((old) => (old.scope === scope ? old : { scope })), [scope]);

  const loaded = config.kind === 'ready' ? config.data : null;
  const settings = loaded === null ? null : (edits.settings ?? loaded.settings);
  // Both sides come from the same object shape, so the key order matches and
  // an edit that is undone compares equal again.
  const dirty =
    loaded !== null &&
    edits.settings !== undefined &&
    JSON.stringify(edits.settings) !== JSON.stringify(loaded.settings);

  const editSettings = useCallback((next: AgentConfigSettings) => setStored({ ...edits, settings: next }), [edits]);
  const discard = useCallback(() => setStored({ scope }), [scope]);
  const reload = useCallback(() => {
    setStored({ scope });
    refetch();
  }, [scope, refetch]);

  return { config, reload, settings, dirty, editSettings, discard };
}
