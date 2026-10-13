import { theme } from 'antd';
import type { ThemeConfig } from 'antd';

import type { ThemeMode } from '../ui/theme-mode';

// Local woff2 files: the UI ships inside the query binary and must not fetch a
// font from another host. The variable builds carry every weight in one file
// per script, and the browser loads only the scripts a page uses.
import '@fontsource-variable/inter/wght.css';
import '@fontsource-variable/jetbrains-mono/wght.css';
import './theme.css';

/**
 * The part of the redesign palette that antd takes as tokens. theme.css repeats
 * these values as `--pf-color-*` for markup outside an antd component, and adds
 * the colors antd has no token for; profiler-theme.test.ts keeps the two in
 * step.
 */
export interface Palette {
  ground: string;
  surface: string;
  border: string;
  borderSoft: string;
  text: string;
  dim: string;
  faint: string;
  accent: string;
  accentText: string;
  ok: string;
  warn: string;
  crit: string;
}

export const lightPalette: Palette = {
  ground: '#f5f5fa',
  surface: '#ffffff',
  border: '#d5dce3',
  borderSoft: '#e6ebf1',
  text: '#353c4e',
  dim: '#626d82',
  faint: '#8f9eb4',
  accent: '#0068ff',
  accentText: '#005dcf',
  ok: '#00a356',
  warn: '#b8760c',
  crit: '#d10049',
};

export const darkPalette: Palette = {
  ground: '#171b24',
  surface: '#1e232e',
  border: '#333b4a',
  borderSoft: '#2a313e',
  text: '#e6e9ef',
  dim: '#a4aec1',
  faint: '#7d879b',
  accent: '#4d9bff',
  accentText: '#6fb0ff',
  ok: '#3fd07f',
  warn: '#ffb02e',
  crit: '#ff6b78',
};

export const FONT_SANS = "'Inter Variable', Inter, 'Segoe UI', system-ui, sans-serif";
export const FONT_MONO = "'JetBrains Mono Variable', 'JetBrains Mono', ui-monospace, Menlo, monospace";

function themeFromPalette(palette: Palette, mode: ThemeMode): ThemeConfig {
  return {
    // The algorithm derives the ramps (hover, fills, disabled) for the mode;
    // the palette then pins the surfaces, borders, and text.
    algorithm: mode === 'dark' ? theme.darkAlgorithm : theme.defaultAlgorithm,
    // Emit `--ant-color-*` CSS variables the modules read (calls-page.module.css et al.).
    cssVar: {},
    token: {
      colorPrimary: palette.accent,
      // antd derives the info ramp separately; seed it with the accent so
      // processing tags and info alerts stay in the same blue.
      colorInfo: palette.accent,
      colorLink: palette.accentText,
      colorSuccess: palette.ok,
      colorWarning: palette.warn,
      colorError: palette.crit,
      colorTextBase: palette.text,
      colorBgBase: palette.surface,
      colorBgLayout: palette.ground,
      colorBgContainer: palette.surface,
      colorBgElevated: palette.surface,
      colorBorder: palette.border,
      colorBorderSecondary: palette.borderSoft,
      colorText: palette.text,
      colorTextSecondary: palette.dim,
      colorTextTertiary: palette.faint,
      fontFamily: FONT_SANS,
      fontFamilyCode: FONT_MONO,
      fontSize: 13,
      borderRadius: 6,
    },
    // The dark accent is light, so white text on a primary button is hard to
    // read. The mockup uses a near-black label there.
    components: mode === 'dark' ? { Button: { primaryColor: '#0b1220' } } : {},
  };
}

export const profilerThemes: Record<ThemeMode, ThemeConfig> = {
  light: themeFromPalette(lightPalette, 'light'),
  dark: themeFromPalette(darkPalette, 'dark'),
};
