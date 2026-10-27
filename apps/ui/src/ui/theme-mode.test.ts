import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  getThemeMode,
  getThemePreference,
  pinThemeMode,
  setThemePreference,
  syncThemeMode,
} from './theme-mode';

const STORAGE_KEY = 'profiler.theme';

/** Stage the system preference that `prefers-color-scheme` reports. */
function stageSystem(dark: boolean): void {
  vi.spyOn(window, 'matchMedia').mockImplementation(
    (query: string) => ({ matches: dark, media: query }) as MediaQueryList,
  );
}

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  vi.restoreAllMocks();
  localStorage.clear();
  syncThemeMode(); // the store is a module singleton; return it to the default
});

describe('theme mode', () => {
  it('defaults to Auto, which is light when the system has no dark preference', () => {
    stageSystem(false);
    syncThemeMode();
    expect(getThemePreference()).toBe('auto');
    expect(getThemeMode()).toBe('light');
  });

  it('follows the system preference under Auto', () => {
    stageSystem(true);
    syncThemeMode();
    expect(getThemeMode()).toBe('dark');
  });

  it('prefers a stored Light or Dark to the system preference', () => {
    stageSystem(true);
    localStorage.setItem(STORAGE_KEY, 'light');
    syncThemeMode();
    expect(getThemePreference()).toBe('light');
    expect(getThemeMode()).toBe('light');

    stageSystem(false);
    localStorage.setItem(STORAGE_KEY, 'dark');
    syncThemeMode();
    expect(getThemeMode()).toBe('dark');
  });

  it('reads a value stored before Auto existed, and ignores one that is not a mode', () => {
    // The two-way switch stored `light` or `dark` under the same key.
    stageSystem(false);
    localStorage.setItem(STORAGE_KEY, 'dark');
    syncThemeMode();
    expect(getThemePreference()).toBe('dark');

    stageSystem(true);
    localStorage.setItem(STORAGE_KEY, 'sepia');
    syncThemeMode();
    expect(getThemePreference()).toBe('auto');
    expect(getThemeMode()).toBe('dark');
  });

  it('stores Light and Dark, and stores nothing for Auto', () => {
    stageSystem(false);
    syncThemeMode();

    setThemePreference('dark');
    expect(localStorage.getItem(STORAGE_KEY)).toBe('dark');
    expect(getThemeMode()).toBe('dark');

    setThemePreference('light');
    expect(localStorage.getItem(STORAGE_KEY)).toBe('light');
    expect(getThemeMode()).toBe('light');

    setThemePreference('auto');
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    expect(getThemePreference()).toBe('auto');

    // Each choice survives a reload, which re-reads storage.
    for (const choice of ['light', 'dark', 'auto'] as const) {
      setThemePreference(choice);
      syncThemeMode();
      expect(getThemePreference()).toBe(choice);
    }
  });

  it('keeps a stored choice when the system preference changes', () => {
    stageSystem(false);
    syncThemeMode();
    setThemePreference('light');
    stageSystem(true);
    syncThemeMode();
    expect(getThemeMode()).toBe('light');
  });

  it('stamps the mode on <html> for theme.css', () => {
    stageSystem(false);
    syncThemeMode();
    expect(document.documentElement.dataset['theme']).toBe('light');

    setThemePreference('dark');
    expect(document.documentElement.dataset['theme']).toBe('dark');
    expect(document.documentElement.style.colorScheme).toBe('dark');
  });

  it('pins a mode for the page load without storing it', () => {
    stageSystem(false);
    localStorage.setItem(STORAGE_KEY, 'light');
    syncThemeMode();

    pinThemeMode('dark');
    expect(getThemeMode()).toBe('dark');
    expect(document.documentElement.dataset['theme']).toBe('dark');
    expect(localStorage.getItem(STORAGE_KEY)).toBe('light');

    // The switch works from the pinned mode and stores the choice as usual.
    setThemePreference('light');
    expect(getThemeMode()).toBe('light');
    expect(localStorage.getItem(STORAGE_KEY)).toBe('light');
  });

  it('falls back to Auto and keeps the choice in memory when storage is unavailable', () => {
    stageSystem(true);
    for (const method of ['getItem', 'setItem', 'removeItem'] as const) {
      vi.spyOn(Storage.prototype, method).mockImplementation(() => {
        throw new DOMException('denied', 'SecurityError');
      });
    }
    syncThemeMode();
    expect(getThemePreference()).toBe('auto');
    expect(getThemeMode()).toBe('dark');

    setThemePreference('light');
    expect(getThemePreference()).toBe('light');
    expect(document.documentElement.dataset['theme']).toBe('light');
  });

  it('resolves Auto to a concrete mode, which is what an export saves', () => {
    stageSystem(true);
    syncThemeMode();
    expect(getThemePreference()).toBe('auto');
    expect(getThemeMode()).toBe('dark');
  });
});

// The store registers its `prefers-color-scheme` listener when the module
// loads, so this suite loads a fresh copy over a media query it controls.
describe('theme mode, live system changes', () => {
  interface FakeQuery {
    matches: boolean;
    listeners: Set<() => void>;
  }

  async function loadWithSystem(dark: boolean): Promise<{ query: FakeQuery; store: typeof import('./theme-mode') }> {
    const query: FakeQuery = { matches: dark, listeners: new Set() };
    vi.spyOn(window, 'matchMedia').mockImplementation(
      (media: string) =>
        ({
          get matches() {
            return query.matches;
          },
          media,
          addEventListener: (_type: string, listener: () => void) => query.listeners.add(listener),
          removeEventListener: (_type: string, listener: () => void) => query.listeners.delete(listener),
        }) as unknown as MediaQueryList,
    );
    vi.resetModules();
    const store = await import('./theme-mode');
    return { query, store };
  }

  function flip(query: FakeQuery, dark: boolean): void {
    query.matches = dark;
    query.listeners.forEach((listener) => listener());
  }

  it('follows the system under Auto while the page is open', async () => {
    const { query, store } = await loadWithSystem(false);
    expect(store.getThemeMode()).toBe('light');

    flip(query, true);
    expect(store.getThemeMode()).toBe('dark');
    expect(document.documentElement.dataset['theme']).toBe('dark');

    flip(query, false);
    expect(store.getThemeMode()).toBe('light');
  });

  it('ignores the system change under a stored choice or a pinned mode', async () => {
    localStorage.setItem(STORAGE_KEY, 'light');
    const { query, store } = await loadWithSystem(false);
    flip(query, true);
    expect(store.getThemeMode()).toBe('light');

    localStorage.clear();
    store.syncThemeMode();
    store.pinThemeMode('light');
    flip(query, false);
    flip(query, true);
    expect(store.getThemeMode()).toBe('light');
  });
});
