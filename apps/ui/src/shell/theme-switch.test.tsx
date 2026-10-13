import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { getThemeMode, setThemeMode, syncThemeMode } from '../ui/theme-mode';
import { ThemeSwitch } from './theme-switch';

afterEach(() => {
  cleanup();
  localStorage.clear();
  syncThemeMode(); // the theme store is a module singleton; return it to the default
});

describe('ThemeSwitch', () => {
  it('names the theme a click selects, and selects it', () => {
    setThemeMode('light');
    render(<ThemeSwitch />);

    fireEvent.click(screen.getByRole('button', { name: 'Switch to dark theme' }));
    expect(getThemeMode()).toBe('dark');

    fireEvent.click(screen.getByRole('button', { name: 'Switch to light theme' }));
    expect(getThemeMode()).toBe('light');
  });
});
