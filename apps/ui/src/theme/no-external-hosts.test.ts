import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

// The UI is embedded in the query binary and often runs where the browser has
// no route to the internet, so no page may load a font, a stylesheet, or a
// script from another host.

/** Absolute and protocol-relative URLs that `src` and `href` attributes load. */
function externalHtmlRefs(html: string): string[] {
  return [...html.matchAll(/\b(?:src|href)\s*=\s*["']((?:https?:)?\/\/[^"']+)["']/gi)].map((m) => m[1]!);
}

/** Absolute and protocol-relative URLs that `url()` and `@import` load. */
function externalCssRefs(css: string): string[] {
  const urls = [...css.matchAll(/url\(\s*["']?((?:https?:)?\/\/[^"')]+)/gi)].map((m) => m[1]!);
  const imports = [...css.matchAll(/@import\s+["']((?:https?:)?\/\/[^"']+)/gi)].map((m) => m[1]!);
  return [...urls, ...imports];
}

// Read from disk: vitest does not process plain CSS, so an import yields nothing.
const read = (path: string): string => readFileSync(join(process.cwd(), path), 'utf8');
const indexHtml = read('index.html');
const themeCss = read('src/theme/theme.css');
const interCss = read('node_modules/@fontsource-variable/inter/wght.css');
const monoCss = read('node_modules/@fontsource-variable/jetbrains-mono/wght.css');

describe('external hosts', () => {
  it('finds the references it is meant to catch', () => {
    expect(externalHtmlRefs('<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter">')).toEqual([
      'https://fonts.googleapis.com/css2?family=Inter',
    ]);
    expect(externalHtmlRefs('<script src="//cdn.example/x.js"></script>')).toEqual(['//cdn.example/x.js']);
    expect(externalCssRefs("src: url('https://fonts.gstatic.com/inter.woff2')")).toEqual([
      'https://fonts.gstatic.com/inter.woff2',
    ]);
    expect(externalCssRefs('@import "https://fonts.googleapis.com/css2";')).toEqual([
      'https://fonts.googleapis.com/css2',
    ]);
    expect(externalHtmlRefs('<script type="module" src="/src/main.tsx"></script>')).toEqual([]);
    expect(externalCssRefs('src: url(./files/inter-latin-wght-normal.woff2)')).toEqual([]);
  });

  it('index.html loads nothing from another host', () => {
    expect(externalHtmlRefs(indexHtml)).toEqual([]);
  });

  it('the theme and font stylesheets load nothing from another host', () => {
    expect(externalCssRefs(themeCss)).toEqual([]);
    expect(externalCssRefs(interCss)).toEqual([]);
    expect(externalCssRefs(monoCss)).toEqual([]);
  });

  it('the bundled fonts are woff2 only', () => {
    for (const css of [interCss, monoCss]) {
      const files = [...css.matchAll(/url\(([^)]+)\)/g)].map((m) => m[1]!);
      expect(files.length).toBeGreaterThan(0);
      expect(files.filter((file) => !file.endsWith('.woff2'))).toEqual([]);
    }
  });

  // `npm run build` output, when present: the same check on what the binary embeds.
  const dist = join(process.cwd(), 'dist');
  it.runIf(existsSync(join(dist, 'index.html')))('the built bundle loads nothing from another host', () => {
    expect(externalHtmlRefs(readFileSync(join(dist, 'index.html'), 'utf8'))).toEqual([]);
    const assets = join(dist, 'assets');
    for (const name of readdirSync(assets).filter((file) => file.endsWith('.css'))) {
      expect(externalCssRefs(readFileSync(join(assets, name), 'utf8')), name).toEqual([]);
    }
  });
});
