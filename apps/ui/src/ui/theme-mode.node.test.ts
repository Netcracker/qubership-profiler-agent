// @vitest-environment node
import { describe, expect, it } from 'vitest';

import { getThemeMode, getThemePreference, pinThemeMode, setThemePreference } from './theme-mode';

// The store guards every DOM access, so a module that imports it, such as the
// export code, still loads and runs where there is no window or document.

describe('theme mode without a DOM', () => {
  it('loads as light under Auto and applies a choice in memory', () => {
    expect(typeof window).toBe('undefined');
    expect(getThemePreference()).toBe('auto');
    expect(getThemeMode()).toBe('light');

    setThemePreference('dark');
    expect(getThemeMode()).toBe('dark');

    pinThemeMode('light');
    expect(getThemeMode()).toBe('light');
  });
});
