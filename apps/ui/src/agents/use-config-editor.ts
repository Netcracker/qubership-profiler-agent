import { useCallback, useEffect, useMemo, useState } from 'react';

import { deleteAgentConfig, fetchAgentConfig, isRevisionConflict, saveAgentConfig } from './api';
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
  /** True while an apply or a return to the inherited config is on its way. */
  saving: boolean;
  /** Why the last apply or return to the inherited config failed, as a sentence. */
  error: string | null;
  clearError: () => void;
  /**
   * True after an apply met a newer revision: someone else saved the scope
   * first. The edits stay, and {@link reload} or {@link dismissConflict} ends it.
   */
  conflict: boolean;
  dismissConflict: () => void;
  /**
   * Saves the edits as the scope's own config, against the revision the page
   * loaded. Resolves to whether the save went through.
   */
  apply: () => Promise<boolean>;
  /** Deletes the scope's own config. Resolves to whether the delete went through. */
  revertToInherited: () => Promise<boolean>;
}

const messageOf = (e: unknown): string => (e instanceof Error ? e.message : String(e));

/**
 * Loads the config in effect for a scope and holds the edits made to it. The
 * edits belong to the scope: opening another scope starts from its loaded
 * config, and the page asks before it lets that happen with edits pending.
 */
export function useConfigEditor(scope: AgentConfigScopeId): ConfigEditor {
  const { state: config, refetch, set } = useLoadable(`config:${scope}`, (signal) => fetchAgentConfig(scope, signal));
  const [stored, setStored] = useState<Edits>({ scope });
  // Edits of another scope are dropped for good, so they do not come back when
  // that scope is opened again.
  const edits = useMemo<Edits>(() => (stored.scope === scope ? stored : { scope }), [stored, scope]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [conflict, setConflict] = useState(false);
  useEffect(() => {
    setStored((old) => (old.scope === scope ? old : { scope }));
    setError(null);
    setConflict(false);
  }, [scope]);

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
    setError(null);
    setConflict(false);
    refetch();
  }, [scope, refetch]);

  const apply = async (): Promise<boolean> => {
    if (loaded === null || settings === null) return false;
    setSaving(true);
    setError(null);
    try {
      // raw_xml travels unchanged: leaving it out would return the scope to generated XML.
      set(await saveAgentConfig(scope, { expected_revision: loaded.revision, settings, raw_xml: loaded.raw_xml }));
      setStored({ scope });
      return true;
    } catch (e) {
      if (isRevisionConflict(e)) setConflict(true);
      else setError(`Could not apply the changes: ${messageOf(e)}`);
      return false;
    } finally {
      setSaving(false);
    }
  };

  const revertToInherited = async (): Promise<boolean> => {
    setSaving(true);
    setError(null);
    try {
      set(await deleteAgentConfig(scope));
      setStored({ scope });
      return true;
    } catch (e) {
      setError(`Could not return to the inherited config: ${messageOf(e)}`);
      return false;
    } finally {
      setSaving(false);
    }
  };

  return {
    config,
    reload,
    settings,
    dirty,
    editSettings,
    discard,
    saving,
    error,
    clearError: useCallback(() => setError(null), []),
    conflict,
    dismissConflict: useCallback(() => setConflict(false), []),
    apply,
    revertToInherited,
  };
}
