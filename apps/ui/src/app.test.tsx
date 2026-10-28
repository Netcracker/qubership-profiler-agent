import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { RouterProvider, createMemoryRouter } from 'react-router';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import { ConfigProvider } from './api/use-config';
import { APP_ROUTES, App } from './app';
import { server } from './mocks/node';

// The route table under the app's own providers.

beforeAll(() => server.listen({ onUnhandledFrame: 'error' }));
afterEach(() => {
  cleanup();
  server.resetHandlers();
  server.events.removeAllListeners();
  localStorage.clear();
  vi.unstubAllEnvs();
  window.history.pushState({}, '', '/');
});
afterAll(() => server.close());

function renderApp(path: string) {
  const router = createMemoryRouter(APP_ROUTES, { initialEntries: [path] });
  render(
    <AntdApp>
      <ConfigProvider>
        <RouterProvider router={router} />
      </ConfigProvider>
    </AntdApp>,
  );
  return router;
}

describe('the route table', () => {
  it('sends the base path to Calls', async () => {
    const router = renderApp('/');
    await waitFor(() => expect(router.state.location.pathname).toBe('/calls'));
    expect(screen.getByRole('link', { name: 'Calls' })).toHaveAttribute('aria-current', 'page');
  });

  it('renders the shell around Pods Info', () => {
    renderApp('/pods');
    expect(screen.getByRole('banner')).toHaveTextContent('Pods Info');
    expect(screen.getByRole('link', { name: 'Pods Info' })).toHaveAttribute('aria-current', 'page');
  });

  it('renders nothing for an unknown path', () => {
    renderApp('/nowhere');
    expect(screen.queryByRole('banner')).not.toBeInTheDocument();
    expect(document.body.textContent).toBe('');
  });

  it('asks for /config once, however many readers it has', async () => {
    let requests = 0;
    server.events.on('request:start', ({ request }) => {
      if (new URL(request.url).pathname === '/api/v1/config') requests += 1;
    });
    renderApp('/pods');
    await waitFor(() => expect(requests).toBe(1));
    // Give a second, unshared request the time to show up.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(requests).toBe(1);
  });
});

describe('App', () => {
  it('routes the browser URL through the shell', async () => {
    window.history.pushState({}, '', '/');
    render(<App />);
    await waitFor(() => expect(window.location.pathname).toBe('/calls'));
    expect(screen.getByRole('link', { name: 'Calls' })).toHaveAttribute('aria-current', 'page');
  });

  it('mounts the routes under a non-root build base', async () => {
    // The router is created once per module load, so a fresh copy reads the stubbed base.
    vi.stubEnv('BASE_URL', '/ui/');
    vi.resetModules();
    const { App: AppUnderBase } = await import('./app');
    window.history.pushState({}, '', '/ui/');
    render(<AppUnderBase />);
    await waitFor(() => expect(window.location.pathname).toBe('/ui/calls'));
    // The rail keeps each screen's query string, so match the path only.
    expect(screen.getByRole('link', { name: 'Pods Info' })).toHaveAttribute(
      'href',
      expect.stringMatching(/^\/ui\/pods(\?|$)/),
    );
  });
});
