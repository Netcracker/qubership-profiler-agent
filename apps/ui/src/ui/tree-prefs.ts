import { useSyncExternalStore } from 'react';

// Display switches of the call tree. They are a reader's taste, not part of a
// view, so they live in localStorage and not in the URL: one choice holds for
// every call and every tab, and a shared link does not carry it.

export interface TreePrefs {
  /** Draw connectors from a method to the methods it calls. */
  treeLines: boolean;
  /** Draw a line between rows. */
  rowLines: boolean;
}

export const DEFAULT_TREE_PREFS: TreePrefs = { treeLines: true, rowLines: true };

const STORAGE_KEY = 'profiler.tree.prefs';

function load(): TreePrefs {
  try {
    const raw: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? 'null');
    if (raw === null || typeof raw !== 'object') return DEFAULT_TREE_PREFS;
    const stored = raw as Partial<Record<keyof TreePrefs, unknown>>;
    // Field by field: a stored value of the wrong type falls back to its default.
    return {
      treeLines: typeof stored.treeLines === 'boolean' ? stored.treeLines : DEFAULT_TREE_PREFS.treeLines,
      rowLines: typeof stored.rowLines === 'boolean' ? stored.rowLines : DEFAULT_TREE_PREFS.rowLines,
    };
  } catch {
    return DEFAULT_TREE_PREFS;
  }
}

let prefs = load();
const listeners = new Set<() => void>();

export function getTreePrefs(): TreePrefs {
  return prefs;
}

export function setTreePref(name: keyof TreePrefs, value: boolean): void {
  if (prefs[name] === value) return;
  prefs = { ...prefs, [name]: value };
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(prefs));
  } catch {
    // Storage may be unavailable (private mode, quota); the in-memory value still applies.
  }
  listeners.forEach((notify) => notify());
}

/** Re-read the stored switches. Tests call this after they stage or clear the storage. */
export function syncTreePrefs(): void {
  prefs = load();
  listeners.forEach((notify) => notify());
}

function subscribe(notify: () => void): () => void {
  listeners.add(notify);
  return () => listeners.delete(notify);
}

/** Subscribe a component to the tree's display switches. */
export function useTreePrefs(): TreePrefs {
  return useSyncExternalStore(subscribe, getTreePrefs, getTreePrefs);
}
