import { useSyncExternalStore } from 'react';

// The app-wide color theme. The user picks Light, Dark, or Auto; Auto follows
// the operating system's preference, live, and is the default. A Light or
// Dark choice is stored and wins on every screen and across reloads; Auto
// stores nothing. The store also stamps the resolved mode on <html>, which is
// what theme.css keys its dark variables on.

/** The theme in effect: what the page draws in. */
export type ThemeMode = 'light' | 'dark';
/** What the user picked. `auto` resolves to the system preference. */
export type ThemePreference = ThemeMode | 'auto';

const STORAGE_KEY = 'profiler.theme';
const DARK_QUERY = '(prefers-color-scheme: dark)';

export function isThemeMode(value: unknown): value is ThemeMode {
  return value === 'light' || value === 'dark';
}

/** Reads the stored choice. A file written before Auto existed holds `light` or `dark` and reads as is. */
function loadPreference(): ThemePreference {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    return isThemeMode(stored) ? stored : 'auto';
  } catch {
    return 'auto';
  }
}

function systemMode(): ThemeMode {
  return typeof window !== 'undefined' && window.matchMedia(DARK_QUERY).matches ? 'dark' : 'light';
}

let preference = loadPreference();
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

/** Applies a new preference and system mode, and tells subscribers when either output changed. */
function apply(nextPreference: ThemePreference, nextSystem: ThemeMode): void {
  const beforeMode = getThemeMode();
  const beforePreference = preference;
  preference = nextPreference;
  system = nextSystem;
  if (getThemeMode() !== beforeMode || preference !== beforePreference) publish();
  else stamp();
}

/** The mode in effect: the stored choice, or the system preference under Auto. */
export function getThemeMode(): ThemeMode {
  return preference === 'auto' ? system : preference;
}

/** The user's choice, as the theme switch shows it. */
export function getThemePreference(): ThemePreference {
  return preference;
}

/** Stores the user's choice. Auto removes the stored value, so the system preference decides again. */
export function setThemePreference(next: ThemePreference): void {
  try {
    if (next === 'auto') localStorage.removeItem(STORAGE_KEY);
    else localStorage.setItem(STORAGE_KEY, next);
  } catch {
    // Storage may be unavailable (private mode, quota); the in-memory value still applies.
  }
  apply(next, system);
}

/**
 * Use a mode for this page load without storing it. An exported copy opens in
 * the theme it was saved in, whatever the browser prefers or an earlier page
 * stored. The switch still works afterwards, and stores as usual.
 */
export function pinThemeMode(mode: ThemeMode): void {
  apply(mode, system);
}

/**
 * Re-reads the stored choice and the system preference. Tests call it after
 * they stage either source, and to return the module singleton to its default.
 */
export function syncThemeMode(): void {
  apply(loadPreference(), systemMode());
}

if (typeof window !== 'undefined') {
  // Only the system side changes here: a pinned mode (an exported copy) or a
  // stored choice stays as it is, and Auto follows the new preference at once.
  window.matchMedia(DARK_QUERY).addEventListener('change', () => apply(preference, systemMode()));
  stamp();
}

function subscribe(notify: () => void): () => void {
  listeners.add(notify);
  return () => listeners.delete(notify);
}

/** Subscribe a component to the mode in effect. */
export function useThemeMode(): ThemeMode {
  return useSyncExternalStore(subscribe, getThemeMode, getThemeMode);
}

/** Subscribe a component to the user's choice. */
export function useThemePreference(): ThemePreference {
  return useSyncExternalStore(subscribe, getThemePreference, getThemePreference);
}
