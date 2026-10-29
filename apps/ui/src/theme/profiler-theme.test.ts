import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { theme } from 'antd';
import { describe, expect, it } from 'vitest';

import { CATEGORY_COLOR_COUNT, categoryColor } from '../tree/transforms/categories';
import { FONT_MONO, FONT_SANS, darkPalette, lightPalette, profilerThemes } from './profiler-theme';
import type { Palette } from './profiler-theme';

// Read from disk: vitest does not process plain CSS, so an import yields nothing.
const themeCss = readFileSync(join(process.cwd(), 'src/theme/theme.css'), 'utf8');

/** The declarations of the first rule in theme.css whose selector is `selector`. */
function declarations(selector: string): Map<string, string> {
  const start = themeCss.indexOf(`${selector} {`);
  if (start < 0) throw new Error(`theme.css has no "${selector}" rule`);
  const body = themeCss.slice(themeCss.indexOf('{', start) + 1, themeCss.indexOf('}', start));
  const out = new Map<string, string>();
  for (const match of body.replace(/\/\*[\s\S]*?\*\//g, '').matchAll(/(--[\w-]+)\s*:\s*([^;]+);/g)) {
    out.set(match[1]!, match[2]!.trim());
  }
  return out;
}

/** `borderSoft` → `--pf-color-border-soft`. */
function variableName(key: string): string {
  return `--pf-color-${key.replace(/[A-Z]/g, (upper) => `-${upper.toLowerCase()}`)}`;
}

function expectCssMatches(selector: string, palette: Palette): void {
  const vars = declarations(selector);
  for (const [key, value] of Object.entries(palette)) {
    expect(vars.get(variableName(key)), variableName(key)).toBe(value);
  }
}

describe('profilerThemes', () => {
  it('maps the light palette onto antd tokens', () => {
    expect(profilerThemes.light.algorithm).toBe(theme.defaultAlgorithm);
    expect(profilerThemes.light.components).toEqual({});
    expect(profilerThemes.light.token).toMatchInlineSnapshot(`
      {
        "borderRadius": 6,
        "colorBgBase": "#ffffff",
        "colorBgContainer": "#ffffff",
        "colorBgElevated": "#ffffff",
        "colorBgLayout": "#f5f5fa",
        "colorBorder": "#d5dce3",
        "colorBorderSecondary": "#e6ebf1",
        "colorError": "#d10049",
        "colorInfo": "#0068ff",
        "colorLink": "#005dcf",
        "colorPrimary": "#0068ff",
        "colorSuccess": "#00a356",
        "colorText": "#353c4e",
        "colorTextBase": "#353c4e",
        "colorTextSecondary": "#626d82",
        "colorTextTertiary": "#8f9eb4",
        "colorWarning": "#b8760c",
        "fontFamily": "'Inter Variable', Inter, 'Segoe UI', system-ui, sans-serif",
        "fontFamilyCode": "'JetBrains Mono Variable', 'JetBrains Mono', ui-monospace, Menlo, monospace",
        "fontSize": 13,
      }
    `);
  });

  it('maps the dark palette onto antd tokens under the dark algorithm', () => {
    expect(profilerThemes.dark.algorithm).toBe(theme.darkAlgorithm);
    expect(profilerThemes.dark.components).toEqual({ Button: { primaryColor: '#0b1220' } });
    expect(profilerThemes.dark.token).toMatchInlineSnapshot(`
      {
        "borderRadius": 6,
        "colorBgBase": "#1e232e",
        "colorBgContainer": "#1e232e",
        "colorBgElevated": "#1e232e",
        "colorBgLayout": "#171b24",
        "colorBorder": "#333b4a",
        "colorBorderSecondary": "#2a313e",
        "colorError": "#ff6b78",
        "colorInfo": "#4d9bff",
        "colorLink": "#6fb0ff",
        "colorPrimary": "#4d9bff",
        "colorSuccess": "#3fd07f",
        "colorText": "#e6e9ef",
        "colorTextBase": "#e6e9ef",
        "colorTextSecondary": "#a4aec1",
        "colorTextTertiary": "#7d879b",
        "colorWarning": "#ffb02e",
        "fontFamily": "'Inter Variable', Inter, 'Segoe UI', system-ui, sans-serif",
        "fontFamilyCode": "'JetBrains Mono Variable', 'JetBrains Mono', ui-monospace, Menlo, monospace",
        "fontSize": 13,
      }
    `);
  });
});

describe('theme.css', () => {
  it('repeats the light palette under :root', () => {
    expectCssMatches(':root', lightPalette);
  });

  it("repeats the dark palette under :root[data-theme='dark']", () => {
    expectCssMatches(":root[data-theme='dark']", darkPalette);
  });

  it('gives every color variable a dark value', () => {
    const light = [...declarations(':root').keys()].filter((name) => !name.startsWith('--pf-font-'));
    expect([...declarations(":root[data-theme='dark']").keys()].sort()).toEqual(light.sort());
  });

  it('defines every category color in both themes', () => {
    const light = declarations(':root');
    const dark = declarations(":root[data-theme='dark']");
    for (let i = 0; i < CATEGORY_COLOR_COUNT; i += 1) {
      const name = /^var\((--[\w-]+)\)$/.exec(categoryColor(i))![1]!;
      expect(light.has(name), name).toBe(true);
      expect(dark.has(name), name).toBe(true);
    }
    expect(light.has(`--pf-color-cat-${CATEGORY_COLOR_COUNT + 1}`)).toBe(false);
  });

  it('declares the same font stacks as the antd tokens', () => {
    const vars = declarations(':root');
    expect(vars.get('--pf-font-sans')).toBe(FONT_SANS);
    expect(vars.get('--pf-font-mono')).toBe(FONT_MONO);
  });
});
