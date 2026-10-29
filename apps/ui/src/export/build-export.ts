import { bytesToBase64, RESTORE_GLOBAL } from './restore';
import type { RestorePayload } from './restore';

// Builds the self-contained HTML file (design 10b, option 2): fetch the app's
// own built JS + CSS, inline them along with the fonts the CSS loads, and embed
// the restore payload. The result opens and renders offline with no backend.
//
// The bundle is inlined, so a working file only comes out of a *built* app (a
// deployment or `vite preview`). Under `vite dev` the index points at source
// modules, so an export made there is not self-contained — the button targets
// a deployed build.
//
// Fonts: the stylesheet names the bundled woff2 files by URL, and a URL does
// not load from a file on disk. The Latin subsets of Inter and JetBrains Mono
// are inlined as data: URLs (about 120 KB of base64). The other subsets are
// dropped, so text in another script falls back to the system fonts that the
// font stacks end in. Inlining all 13 subsets would add about 410 KB to every
// export for glyphs that a Java stack trace rarely contains.

async function fetchText(url: string): Promise<string> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`cannot read ${url}: ${res.status} ${res.statusText}`);
  return res.text();
}

function escapeHtml(s: string): string {
  return s.replace(/[&<>]/g, (c) => (c === '&' ? '&amp;' : c === '<' ? '&lt;' : '&gt;'));
}

// JSON in an inline <script> must not carry a raw '<' (could open a tag) or a
// U+2028/U+2029 line separator (illegal raw in a JS string), or the embedded
// payload can break out of the element or the parse. Built from char codes so
// no literal separator ever appears in this source file.
function escapeForInlineScript(json: string): string {
  return json
    .split('<')
    .join('\\u003c')
    .split(String.fromCharCode(0x2028))
    .join('\\u2028')
    .split(String.fromCharCode(0x2029))
    .join('\\u2029');
}

async function fetchBytes(url: string): Promise<Uint8Array> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`cannot read ${url}: ${res.status} ${res.statusText}`);
  return new Uint8Array(await res.arrayBuffer());
}

const FONT_FACE = /@font-face\s*\{[^}]*\}/g;
const FONT_URL = /url\(\s*(['"]?)([^'")]+)\1\s*\)/g;

/**
 * Whether an @font-face rule covers Basic Latin: its `unicode-range` holds
 * U+0041, or it has no range and so covers everything.
 */
function coversBasicLatin(fontFace: string): boolean {
  const declared = /unicode-range\s*:\s*([^;}]+)/i.exec(fontFace);
  if (declared === null) return true;
  return declared[1]!.split(',').some((part) => {
    const range = /U\+([0-9a-f?]+)(?:-([0-9a-f]+))?/i.exec(part.trim());
    if (range === null) return false;
    const from = parseInt(range[1]!.replace(/\?/g, '0'), 16);
    const to = range[2] !== undefined ? parseInt(range[2], 16) : parseInt(range[1]!.replace(/\?/g, 'F'), 16);
    return from <= 0x41 && 0x41 <= to;
  });
}

/**
 * Makes the fonts of a stylesheet work in a single offline file: an @font-face
 * rule that covers Basic Latin gets its font inlined as a data: URL, and every
 * other @font-face rule is removed. A font that cannot be fetched is removed
 * too. Either way the file never points at a font it cannot load, and the
 * font stack's system fallback takes over for what is missing.
 */
async function inlineFonts(css: string, cssUrl: string): Promise<string> {
  const rules = [...new Set(css.match(FONT_FACE) ?? [])];
  const replacements = await Promise.all(
    rules.map(async (rule): Promise<[string, string]> => {
      if (!coversBasicLatin(rule)) return [rule, ''];
      try {
        const urls = [...rule.matchAll(FONT_URL)].map((m) => m[2]!).filter((u) => !u.startsWith('data:'));
        let inlined = rule;
        for (const url of new Set(urls)) {
          const bytes = await fetchBytes(new URL(url, cssUrl).href);
          const type = url.endsWith('.woff') ? 'font/woff' : 'font/woff2';
          inlined = inlined.split(url).join(`data:${type};base64,${bytesToBase64(bytes)}`);
        }
        return [rule, inlined];
      } catch {
        return [rule, ''];
      }
    }),
  );
  let out = css;
  for (const [rule, replacement] of replacements) out = out.split(rule).join(replacement);
  return out;
}

function toDataUrl(js: string): string {
  return `data:text/javascript;base64,${bytesToBase64(new TextEncoder().encode(js))}`;
}

/**
 * Inlines a code-split module graph into stand-alone <script> tags. Vite emits
 * one entry <script src> plus a <link rel=modulepreload> per vendor chunk, and
 * the chunks import each other by relative path (`import "./antd-abc.js"`) —
 * paths that resolve against a data: URL to nothing, so a naive inline of the
 * entry alone would break. Instead, every chunk's cross-chunk import is rewritten
 * to the imported chunk's own data: URL. Encoding runs dependency-first (a chunk
 * is encoded only once all chunks it imports are encoded), so each rewritten
 * data: URL already carries its dependencies. Only the entries need a <script>
 * tag; the rest are pulled in transitively by the rewritten imports.
 *
 * Falls back to a plain inline when there is a single chunk and nothing to
 * rewrite, matching the pre-split output.
 */
async function inlineModuleGraph(doc: Document, indexUrl: string): Promise<string[]> {
  const entryUrls = [...doc.querySelectorAll<HTMLScriptElement>('script[type="module"][src]')].map(
    (s) => new URL(s.getAttribute('src')!, indexUrl).href,
  );
  if (entryUrls.length === 0) {
    // A dev index points at source modules, not a bundle; the export would open
    // to a blank page. Fail loudly instead.
    throw new Error('no built module bundle to inline — export needs a deployed build, not the dev server');
  }
  const preloadUrls = [...doc.querySelectorAll<HTMLLinkElement>('link[rel="modulepreload"][href]')].map(
    (l) => new URL(l.getAttribute('href')!, indexUrl).href,
  );

  // Fetch every chunk once, keyed by both its absolute URL and its bare file
  // name — cross-chunk imports reference the file name (`./antd-abc.js`).
  const chunkUrls = [...new Set([...entryUrls, ...preloadUrls])];
  const sources = new Map<string, string>();
  const nameToUrl = new Map<string, string>();
  await Promise.all(
    chunkUrls.map(async (url) => {
      sources.set(url, await fetchText(url));
      nameToUrl.set(url.slice(url.lastIndexOf('/') + 1), url);
    }),
  );

  // Edges: which chunks each chunk imports, by scanning for the sibling file
  // names it references. Rewrite happens against these exact quoted specifiers.
  const specifiersOf = (url: string): string[] =>
    [...nameToUrl.keys()].filter((name) => sources.get(url)!.includes(name) && name !== url.slice(url.lastIndexOf('/') + 1));

  const dataUrls = new Map<string, string>();
  // Dependency-first encoding: repeat until every chunk is encoded, taking any
  // chunk whose imports are all already encoded. A chunk-level import cycle
  // (which Vite's entry/vendor splits never produce) would stall this loop.
  let progress = true;
  while (dataUrls.size < chunkUrls.length && progress) {
    progress = false;
    for (const url of chunkUrls) {
      if (dataUrls.has(url)) continue;
      const deps = specifiersOf(url).map((name) => nameToUrl.get(name)!);
      if (!deps.every((dep) => dataUrls.has(dep))) continue;
      let js = sources.get(url)!;
      for (const name of specifiersOf(url)) {
        js = js.split(`"./${name}"`).join(`"${dataUrls.get(nameToUrl.get(name)!)!}"`);
        js = js.split(`'./${name}'`).join(`'${dataUrls.get(nameToUrl.get(name)!)!}'`);
      }
      dataUrls.set(url, toDataUrl(js));
      progress = true;
    }
  }
  if (dataUrls.size < chunkUrls.length) {
    throw new Error('cannot inline the module bundle — its chunks form an import cycle');
  }

  return entryUrls.map((url) => `<script type="module" src="${dataUrls.get(url)!}"></script>`);
}

/**
 * Assembles the export HTML. Styles inline as <style>, with their fonts made
 * to work offline (see inlineFonts); the module bundle inlines
 * as base64 data: URLs rather than inline text, because the minified bundle
 * contains `<script` and `<!--` substrings that would derail the HTML parser's
 * script-data states — a data: URL sidesteps all of them.
 */
export async function buildExportHtml(payload: RestorePayload): Promise<string> {
  const base = import.meta.env.BASE_URL; // the build-time UI base: '/' by default, or '/ui/' under a sub-path
  const indexUrl = new URL(`${base}index.html`, location.origin).href;
  const doc = new DOMParser().parseFromString(await fetchText(indexUrl), 'text/html');

  const styles: string[] = [];
  for (const link of doc.querySelectorAll<HTMLLinkElement>('link[rel="stylesheet"][href]')) {
    const cssUrl = new URL(link.getAttribute('href')!, indexUrl).href;
    styles.push(`<style>${await inlineFonts(await fetchText(cssUrl), cssUrl)}</style>`);
  }

  const scripts = await inlineModuleGraph(doc, indexUrl);

  const json = escapeForInlineScript(JSON.stringify(payload));

  return [
    '<!doctype html>',
    '<html lang="en"><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
    `<title>Profiler — ${escapeHtml(payload.pkPath)}</title>`,
    ...styles,
    `<script>window.${RESTORE_GLOBAL} = ${json};</script>`,
    '</head><body><div id="root"></div>',
    ...scripts,
    '</body></html>',
  ].join('\n');
}

/** Triggers a browser download of the built HTML, no backend involved. */
export function downloadHtml(html: string, filename: string): void {
  const url = URL.createObjectURL(new Blob([html], { type: 'text/html' }));
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
