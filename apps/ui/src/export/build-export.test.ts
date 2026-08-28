import { afterEach, describe, expect, it, vi } from 'vitest';

import { buildExportHtml } from './build-export';
import { RESTORE_VERSION } from './restore';
import type { RestorePayload } from './restore';

const INDEX = `<!doctype html><html><head>
<script type="module" crossorigin src="/ui/assets/app.js"></script>
<link rel="stylesheet" crossorigin href="/ui/assets/app.css">
</head><body><div id="root"></div></body></html>`;

// The bundle deliberately carries a <script substring, the exact case a base64
// data: URL is meant to neutralise.
const APP_JS = 'console.log("</script><!-- app --><script>")';

function stubFetch(index: string, js = APP_JS, css = '.hljs{color:red}'): void {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string) => {
      const u = String(url);
      const body = u.endsWith('index.html') ? index : u.endsWith('.css') ? css : js;
      return { ok: true, status: 200, statusText: 'OK', text: async () => body } as Response;
    }),
  );
}

function payload(overrides: Partial<RestorePayload> = {}): RestorePayload {
  return {
    v: RESTORE_VERSION,
    pkPath: 'ns:svc:pod:1:2:3:0',
    tsMs: 5,
    retentionClass: 'long_clean',
    treeB64: 'AQID',
    adjustText: '',
    categoryText: '',
    tabs: [{ op: 'incoming', methodIdx: 3 }],
    activeTab: 'tree',
    ...overrides,
  };
}

describe('buildExportHtml', () => {
  afterEach(() => vi.restoreAllMocks());

  it('inlines the CSS, inlines the JS as a data: URL, and embeds the payload', async () => {
    stubFetch(INDEX);
    const html = await buildExportHtml(payload());

    expect(html).toContain('<style>.hljs{color:red}</style>');
    expect(html).toContain('<script type="module" src="data:text/javascript;base64,');
    // The raw bundle text — with its <script substring — never lands in the
    // document; only its base64 form does.
    expect(html).not.toContain(APP_JS);
    expect(html).toContain('window.__PROFILER_RESTORE__ =');
    expect(html).toContain('ns:svc:pod:1:2:3:0');
  });

  it("escapes '<' in the embedded payload so it cannot open a tag", async () => {
    stubFetch(INDEX);
    const html = await buildExportHtml(payload({ categoryText: 'db >*.Foo<bar>.*' }));
    expect(html).toContain('\\u003c');
    expect(html).not.toContain('Foo<bar>');
  });

  it('fails loudly when the index has no module bundle (dev server)', async () => {
    stubFetch('<!doctype html><html><head></head><body></body></html>');
    await expect(buildExportHtml(payload())).rejects.toThrow(/deployed build/);
  });

  it('inlines a code-split chunk graph, rewriting cross-chunk imports to data: URLs', async () => {
    // Entry imports a vendor chunk by relative path, exactly as Vite emits it.
    // The vendor chunk must end up inlined and reachable, not left as a broken
    // relative import against the entry's data: URL.
    const SPLIT_INDEX = `<!doctype html><html><head>
<script type="module" crossorigin src="/ui/assets/index.js"></script>
<link rel="modulepreload" crossorigin href="/ui/assets/vendor.js">
<link rel="stylesheet" crossorigin href="/ui/assets/app.css">
</head><body><div id="root"></div></body></html>`;
    const ENTRY = 'import{x}from"./vendor.js";console.log(x)';
    const VENDOR = 'export const x=42';
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string) => {
        const u = String(url);
        const body = u.endsWith('index.html')
          ? SPLIT_INDEX
          : u.endsWith('.css')
            ? '.hljs{color:red}'
            : u.endsWith('vendor.js')
              ? VENDOR
              : ENTRY;
        return { ok: true, status: 200, statusText: 'OK', text: async () => body } as Response;
      }),
    );

    const html = await buildExportHtml(payload());

    // Exactly one <script> tag (the entry); the vendor chunk rides inside it.
    expect(html.match(/<script type="module" src="data:/g)).toHaveLength(1);
    // No relative './vendor.js' specifier survives — it was rewritten to a data: URL.
    expect(html).not.toContain('./vendor.js');
    // The entry's data: URL decodes back to an import of the vendor's data: URL.
    const entryB64 = html.match(/src="data:text\/javascript;base64,([^"]+)"/)![1]!;
    const entryJs = new TextDecoder().decode(Uint8Array.from(atob(entryB64), (c) => c.charCodeAt(0)));
    expect(entryJs).toContain('from"data:text/javascript;base64,');
    expect(entryJs).not.toContain('./vendor.js');
  });
});
