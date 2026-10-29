import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { getThemeMode, getThemePreference, setThemePreference, syncThemeMode } from '../ui/theme-mode';
import { ThemeSwitch } from './theme-switch';

afterEach(() => {
  cleanup();
  localStorage.clear();
  syncThemeMode(); // the theme store is a module singleton; return it to the default
});

describe('ThemeSwitch', () => {
  it('offers Light, Dark, and Auto, and shows the current choice', () => {
    render(<ThemeSwitch />);
    expect(screen.getByRole('radiogroup', { name: 'Color theme' })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Auto' })).toBeChecked();
    expect(screen.getByRole('radio', { name: 'Light' })).not.toBeChecked();
    expect(screen.getByRole('radio', { name: 'Dark' })).not.toBeChecked();
  });

  it('selects and stores each choice', () => {
    render(<ThemeSwitch />);

    fireEvent.click(screen.getByRole('radio', { name: 'Dark' }));
    expect(getThemePreference()).toBe('dark');
    expect(getThemeMode()).toBe('dark');
    expect(localStorage.getItem('profiler.theme')).toBe('dark');

    fireEvent.click(screen.getByRole('radio', { name: 'Light' }));
    expect(getThemeMode()).toBe('light');
    expect(localStorage.getItem('profiler.theme')).toBe('light');

    fireEvent.click(screen.getByRole('radio', { name: 'Auto' }));
    expect(getThemePreference()).toBe('auto');
    expect(localStorage.getItem('profiler.theme')).toBeNull();
  });

  it('follows a choice made elsewhere', () => {
    render(<ThemeSwitch />);
    act(() => setThemePreference('dark'));
    expect(screen.getByRole('radio', { name: 'Dark' })).toBeChecked();
  });
});
