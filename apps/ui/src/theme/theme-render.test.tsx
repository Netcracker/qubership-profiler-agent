import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp, theme } from 'antd';
import type { ReactNode } from 'react';
import { MemoryRouter, Route, Routes } from 'react-router';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { pkToPath } from '../api/pk';
import { server } from '../mocks/node';
import { callsPage } from '../mocks/synthetic';
import { CallsPage } from '../pages/calls-page';
import { PodsPage } from '../pages/pods-page';
import { TreePage } from '../pages/tree-page';
import { setThemeMode, syncThemeMode } from '../ui/theme-mode';
import type { ThemeMode } from '../ui/theme-mode';
import { darkPalette, lightPalette } from './profiler-theme';
import { ThemeProvider } from './theme-provider';

// Each page mounts and loads its data under both token sets. jsdom applies no
// stylesheet, so this covers what a unit test can: the page renders, antd
// receives the palette of the mode, and <html> carries the mode for theme.css.
// How the result looks is a check for a browser.

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  cleanup();
  localStorage.clear();
  syncThemeMode(); // the theme store is a module singleton; return it to the default
});
afterAll(() => server.close());

const PALETTES = { light: lightPalette, dark: darkPalette };

/** Publishes the tokens antd resolved, so a test can read them from the DOM. */
function TokenProbe() {
  const { token } = theme.useToken();
  return <span data-testid="tokens" data-ground={token.colorBgLayout} data-text={token.colorText} />;
}

function renderThemed(path: string, routePath: string, page: ReactNode) {
  return render(
    <ThemeProvider>
      <AntdApp>
        <TokenProbe />
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route path={routePath} element={page} />
          </Routes>
        </MemoryRouter>
      </AntdApp>
    </ThemeProvider>,
  );
}

function expectMode(mode: ThemeMode): void {
  const probe = screen.getByTestId('tokens');
  expect(probe.dataset['ground']).toBe(PALETTES[mode].ground);
  expect(probe.dataset['text']).toBe(PALETTES[mode].text);
  expect(document.documentElement.dataset['theme']).toBe(mode);
}

async function expectRows(): Promise<void> {
  await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });
}

describe.each<ThemeMode>(['light', 'dark'])('pages in the %s theme', (mode) => {
  it('renders the calls page', async () => {
    setThemeMode(mode);
    const to = Date.now();
    renderThemed(`/calls?from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0`, '/calls', <CallsPage />);
    await expectRows();
    expectMode(mode);
  });

  it('renders the pods page', async () => {
    setThemeMode(mode);
    const to = Date.now();
    renderThemed(`/pods?from=${to - 15 * 60 * 1000}&to=${to}`, '/pods', <PodsPage />);
    await expectRows();
    expectMode(mode);
  });

  it('renders the tree page', async () => {
    setThemeMode(mode);
    const toMs = Date.now();
    const query = {
      fromMs: toMs - 30 * 60 * 1000,
      toMs,
      pods: [],
      method: '',
      durationMinMs: 0,
      durationMaxMs: 0,
      errorOnly: false,
      retentionClasses: [],
    };
    const call = callsPage(query, null, 500, toMs).calls.find((c) => c.truncated_reason === null);
    if (call === undefined) throw new Error('synthetic dataset produced no complete call');
    renderThemed(
      `/tree/${encodeURIComponent(pkToPath(call.pk))}?ts_ms=${call.ts_ms}&retention_class=${call.retention_class}`,
      '/tree/:pk',
      <TreePage />,
    );
    // The synthetic tree always roots at CoyoteAdapter.service.
    expect(await screen.findByText(/CoyoteAdapter\.service/, undefined, { timeout: 5000 })).toBeInTheDocument();
    expectMode(mode);
  });
});
