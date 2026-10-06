// Self-contained HTML export (design 10b, option 2). The running SPA bakes the
// call tree it already holds into one HTML file: the built JS/CSS inlined, the
// tree's MessagePack bytes base64-encoded, and the view state. On open, the
// bundle re-runs, finds window.__PROFILER_RESTORE__, and boots straight into
// the tree — no router, no backend, no fetch. This module is the shared
// contract between the builder (build-export.ts) and the boot path (main.tsx,
// restored-app.tsx).

export const RESTORE_VERSION = 1;

/** The global the exported HTML sets and the boot path reads. */
export const RESTORE_GLOBAL = '__PROFILER_RESTORE__';

/**
 * A restored derived-view tab: the recipe, not the result (mirrors tree-page's
 * OpTabSpec without its runtime key). methodIdx and nodeId resolve because the
 * export embeds the exact same wire bytes, so a re-decode yields the same
 * dictionary order and node ids.
 */
export interface SerializedTab {
  op: 'incoming' | 'outgoing' | 'usages' | 'local';
  methodIdx: number;
  category?: string;
  nodeId?: number;
}

/** The window.__PROFILER_RESTORE__ payload an exported HTML file carries. */
export interface RestorePayload {
  v: number;
  /** pkToPath(pk); parsed back to the 7-component PK on restore. */
  pkPath: string;
  tsMs: number | null;
  retentionClass: string | null;
  /** base64 of the tree's MessagePack bytes, exactly as the server sent them. */
  treeB64: string;
  adjustText: string;
  categoryText: string;
  tabs: SerializedTab[];
  activeTab: string;
}

/** base64-encode raw bytes (btoa needs a binary string, so chunk to stay under the arg limit). */
export function bytesToBase64(bytes: Uint8Array): string {
  let binary = '';
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

/** Inverse of bytesToBase64. */
export function base64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

/**
 * Reads and validates the restore global. Returns null when it is absent or
 * shaped wrong (a normal, non-exported page load), so the caller falls back to
 * the router boot.
 */
export function readRestorePayload(): RestorePayload | null {
  const raw = (globalThis as Record<string, unknown>)[RESTORE_GLOBAL];
  if (raw === null || typeof raw !== 'object') return null;
  const p = raw as Partial<RestorePayload>;
  if (p.v !== RESTORE_VERSION || typeof p.pkPath !== 'string' || typeof p.treeB64 !== 'string') return null;
  return p as RestorePayload;
}
