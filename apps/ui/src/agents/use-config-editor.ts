import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { deleteAgentConfig, fetchAgentConfig, isRevisionConflict, saveAgentConfig } from './api';
import type { AgentConfigResponse, AgentConfigScopeId, AgentConfigSettings } from './types';
import { useLoadable } from './use-loadable';
import type { Loadable } from './use-loadable';

/** The unsaved edits of one scope. An absent field has no edit. */
interface Edits {
  scope: AgentConfigScopeId;
  settings?: AgentConfigSettings;
  /** The hand-edited XML, or null once the hand edits are discarded. */
  rawXml?: string | null;
}

export interface ConfigEditor {
  /** The config as the server last answered it. */
  config: Loadable<AgentConfigResponse>;
  /** Loads the config again and drops the edits. */
  reload: () => void;
  /** The loaded settings with the edits on top; null until the config loads. */
  settings: AgentConfigSettings | null;
  /**
   * The hand-edited XML that overrides the settings: the saved one, or the
   * unsaved edit on top of it. Null while the XML is generated from the settings.
   */
  rawXml: string | null;
  /** Whether an apply would change what the server has. */
  dirty: boolean;
  editSettings: (next: AgentConfigSettings) => void;
  /** Takes a hand edit of the XML, which from then on overrides the settings. */
  editRawXml: (xml: string) => void;
  /** Returns to the XML generated from the settings. */
  discardRawXml: () => void;
  /** Drops the edits and shows the loaded config again. */
  discard: () => void;
  /** True while an apply or a return to the inherited config of this scope is on its way. */
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
   * loaded. Edits made while the save is on its way stay as edits on top of the
   * saved config. Resolves to whether the save went through with the scope
   * still open: once another scope is open, the answer changes nothing on the page.
   */
  apply: () => Promise<boolean>;
  /**
   * Deletes the scope's own config. Resolves to whether the delete went through
   * with the scope still open, as for {@link apply}.
   */
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
  // The scope whose write is on its way, so that another scope does not show it as saving.
  const [savingScope, setSavingScope] = useState<AgentConfigScopeId | null>(null);
  // The scope open now, read when a write answers: the answer of a scope the
  // user has left must not touch the page.
  const currentScope = useRef(scope);
  currentScope.current = scope;
  const [error, setError] = useState<string | null>(null);
  const [conflict, setConflict] = useState(false);
  useEffect(() => {
    setStored((old) => (old.scope === scope ? old : { scope }));
    setError(null);
    setConflict(false);
  }, [scope]);

  const loaded = config.kind === 'ready' ? config.data : null;
  const settings = loaded === null ? null : (edits.settings ?? loaded.settings);
  const rawXml = loaded === null ? null : edits.rawXml === undefined ? loaded.raw_xml : edits.rawXml;
  // Both sides come from the same object shape, so the key order matches and
  // an edit that is undone compares equal again.
  const dirty =
    loaded !== null &&
    ((edits.settings !== undefined && JSON.stringify(edits.settings) !== JSON.stringify(loaded.settings)) ||
      rawXml !== loaded.raw_xml);

  const editSettings = useCallback((next: AgentConfigSettings) => setStored({ ...edits, settings: next }), [edits]);
  const editRawXml = useCallback((xml: string) => setStored({ ...edits, rawXml: xml }), [edits]);
  const discardRawXml = useCallback(() => setStored({ ...edits, rawXml: null }), [edits]);
  const discard = useCallback(() => setStored({ scope }), [scope]);
  const reload = useCallback(() => {
    setStored({ scope });
    setError(null);
    setConflict(false);
    refetch();
  }, [scope, refetch]);

  /**
   * Sends one write and takes its answer as the loaded config. Clears the edits
   * only if they are still the ones the write sent.
   */
  const write = async (send: () => Promise<AgentConfigResponse>, failure: string): Promise<boolean> => {
    const sent = edits;
    setSavingScope(scope);
    setError(null);
    try {
      const saved = await send();
      if (currentScope.current !== scope) return false;
      set(saved);
      setStored((old) => (old === sent ? { scope } : old));
      return true;
    } catch (e) {
      if (currentScope.current !== scope) return false;
      if (isRevisionConflict(e)) setConflict(true);
      else setError(`${failure}: ${messageOf(e)}`);
      return false;
    } finally {
      setSavingScope((old) => (old === scope ? null : old));
    }
  };

  const apply = async (): Promise<boolean> => {
    if (loaded === null || settings === null) return false;
    // Null returns the scope to the XML generated from the settings.
    const body = { expected_revision: loaded.revision, settings, raw_xml: rawXml };
    return write(() => saveAgentConfig(scope, body), 'Could not apply the changes');
  };

  const revertToInherited = (): Promise<boolean> =>
    write(() => deleteAgentConfig(scope), 'Could not return to the inherited config');

  return {
    config,
    reload,
    settings,
    rawXml,
    dirty,
    editSettings,
    editRawXml,
    discardRawXml,
    discard,
    saving: savingScope === scope,
    error,
    clearError: useCallback(() => setError(null), []),
    conflict,
    dismissConflict: useCallback(() => setConflict(false), []),
    apply,
    revertToInherited,
  };
}
