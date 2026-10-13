import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { FONT_MONO, FONT_SANS, lightPalette, profilerTheme } from './profiler-theme';
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

describe('profilerTheme', () => {
  it('maps the light palette onto antd tokens', () => {
    expect(profilerTheme).toMatchInlineSnapshot(`
      {
        "cssVar": {},
        "token": {
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
        },
      }
    `);
  });
});

describe('theme.css', () => {
  it('repeats the light palette under :root', () => {
    expectCssMatches(':root', lightPalette);
  });

  it('declares the same font stacks as the antd tokens', () => {
    const vars = declarations(':root');
    expect(vars.get('--pf-font-sans')).toBe(FONT_SANS);
    expect(vars.get('--pf-font-mono')).toBe(FONT_MONO);
  });
});
