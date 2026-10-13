import { useSyncExternalStore } from 'react';

// The app-wide color theme. The operating system's preference decides until the
// user flips the switch; from then on the stored choice wins, on every screen
// and across reloads. The store also stamps the mode on <html>, which is what
// theme.css keys its dark variables on.

export type ThemeMode = 'light' | 'dark';

const STORAGE_KEY = 'profiler.theme';
const DARK_QUERY = '(prefers-color-scheme: dark)';

function isThemeMode(value: unknown): value is ThemeMode {
  return value === 'light' || value === 'dark';
}

function loadOverride(): ThemeMode | null {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    return isThemeMode(stored) ? stored : null;
  } catch {
    return null;
  }
}

function systemMode(): ThemeMode {
  return typeof window !== 'undefined' && window.matchMedia(DARK_QUERY).matches ? 'dark' : 'light';
}

let override = loadOverride();
let system = systemMode();
const listeners = new Set<() => void>();

function stamp(): void {
  if (typeof document === 'undefined') return;
  const mode = getThemeMode();
  document.documentElement.dataset['theme'] = mode;
  // Native widgets (scrollbars, form controls) follow the same mode.
  document.documentElement.style.colorScheme = mode;
}

function publish(): void {
  stamp();
  listeners.forEach((notify) => notify());
}

/** The mode in effect: the stored override, or the system preference without one. */
export function getThemeMode(): ThemeMode {
  return override ?? system;
}

/** Store the user's choice; it outranks the system preference from now on. */
export function setThemeMode(mode: ThemeMode): void {
  const changed = mode !== getThemeMode();
  override = mode;
  try {
    localStorage.setItem(STORAGE_KEY, mode);
  } catch {
    // Storage may be unavailable (private mode, quota); the in-memory value still applies.
  }
  if (changed) publish();
}

/**
 * Use a mode for this page load without storing it. An exported copy opens in
 * the theme it was saved in, whatever the browser prefers or an earlier page
 * stored. The switch still works afterwards, and stores as usual.
 */
export function pinThemeMode(mode: ThemeMode): void {
  const changed = mode !== getThemeMode();
  override = mode;
  if (changed) publish();
  else stamp();
}

export function toggleThemeMode(): void {
  setThemeMode(getThemeMode() === 'dark' ? 'light' : 'dark');
}

/**
 * Re-read the override and the system preference. The store calls this when
 * the system preference changes; tests call it after they stage either source.
 */
export function syncThemeMode(): void {
  const before = getThemeMode();
  override = loadOverride();
  system = systemMode();
  if (getThemeMode() === before) stamp();
  else publish();
}

if (typeof window !== 'undefined') {
  window.matchMedia(DARK_QUERY).addEventListener('change', syncThemeMode);
  stamp();
}

function subscribe(notify: () => void): () => void {
  listeners.add(notify);
  return () => listeners.delete(notify);
}

/** Subscribe a component to the color theme. */
export function useThemeMode(): ThemeMode {
  return useSyncExternalStore(subscribe, getThemeMode, getThemeMode);
}
