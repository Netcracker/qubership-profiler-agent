import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { getThemeMode, pinThemeMode, setThemeMode, syncThemeMode, toggleThemeMode } from './theme-mode';

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
  it('defaults to light when the system has no dark preference', () => {
    stageSystem(false);
    syncThemeMode();
    expect(getThemeMode()).toBe('light');
  });

  it('follows the system preference without an override', () => {
    stageSystem(true);
    syncThemeMode();
    expect(getThemeMode()).toBe('dark');
  });

  it('prefers the stored override to the system preference', () => {
    stageSystem(true);
    localStorage.setItem(STORAGE_KEY, 'light');
    syncThemeMode();
    expect(getThemeMode()).toBe('light');

    stageSystem(false);
    localStorage.setItem(STORAGE_KEY, 'dark');
    syncThemeMode();
    expect(getThemeMode()).toBe('dark');
  });

  it('ignores a stored value that is not a mode', () => {
    stageSystem(true);
    localStorage.setItem(STORAGE_KEY, 'sepia');
    syncThemeMode();
    expect(getThemeMode()).toBe('dark');
  });

  it('stores the choice, and the choice survives a later system change', () => {
    stageSystem(false);
    syncThemeMode();

    toggleThemeMode();
    expect(getThemeMode()).toBe('dark');
    expect(localStorage.getItem(STORAGE_KEY)).toBe('dark');

    setThemeMode('light');
    stageSystem(true);
    syncThemeMode();
    expect(getThemeMode()).toBe('light');
  });

  it('stamps the mode on <html> for theme.css', () => {
    stageSystem(false);
    syncThemeMode();
    expect(document.documentElement.dataset['theme']).toBe('light');

    setThemeMode('dark');
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
    toggleThemeMode();
    expect(getThemeMode()).toBe('light');
    expect(localStorage.getItem(STORAGE_KEY)).toBe('light');
    toggleThemeMode();
    expect(localStorage.getItem(STORAGE_KEY)).toBe('dark');
  });
});
