import { useSyncExternalStore } from 'react';

import { BROWSER_ZONE } from '../controls/time-range';

// The app-wide display timezone (Grafana-style). It governs how every timestamp
// is rendered — the period picker, the call-list "Start" column, the call tree,
// and tooltips — and is persisted so it survives a reload. The URL and the wire
// stay Unix ms; only the display shifts.

const STORAGE_KEY = 'profiler.timezone';

function load(): string {
  try {
    return localStorage.getItem(STORAGE_KEY) ?? BROWSER_ZONE;
  } catch {
    return BROWSER_ZONE;
  }
}

let current = load();
const listeners = new Set<() => void>();

export function getZone(): string {
  return current;
}

export function setZone(zone: string): void {
  if (zone === current) return;
  current = zone;
  try {
    localStorage.setItem(STORAGE_KEY, zone);
  } catch {
    // Storage may be unavailable (private mode, quota); the in-memory value still applies.
  }
  listeners.forEach((notify) => notify());
}

function subscribe(notify: () => void): () => void {
  listeners.add(notify);
  return () => listeners.delete(notify);
}

/** Subscribe a component to the global display timezone. */
export function useZone(): string {
  return useSyncExternalStore(subscribe, getZone, getZone);
}
