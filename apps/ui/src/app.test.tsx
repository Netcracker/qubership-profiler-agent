import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { HttpResponse, http } from 'msw';
import { RouterProvider, createMemoryRouter } from 'react-router';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigProvider } from './api/use-config';
import { APP_ROUTES, App } from './app';
import { resetAgentConfigMock } from './mocks/agent-config';
import { server } from './mocks/node';

// The route table under the app's own providers: /agents exists only when
// /config reports the agent-configuration feature.

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
beforeEach(() => resetAgentConfigMock());
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

/** Resolves once the page has the answer to its /config request. */
function configAnswered(): Promise<void> {
  return new Promise((resolve) => {
    server.events.on('response:mocked', ({ request }) => {
      if (new URL(request.url).pathname === '/api/v1/config') setTimeout(resolve, 20);
    });
  });
}

describe('the /agents route', () => {
  it('renders the agent-configuration page when /config reports the feature', async () => {
    renderApp('/agents?scope=prod-billing');
    expect(await screen.findByRole('button', { name: /^Scope/ })).toHaveTextContent('prod-billing');
    expect(screen.getByRole('banner')).toHaveTextContent('Agent configuration');
    expect(screen.getByRole('link', { name: 'Agents' })).toHaveAttribute('aria-current', 'page');
  });

  it.each([
    ['a backend that predates the flag', { dumps_collector_url: '' }],
    ['the flag turned off', { dumps_collector_url: '', features: { agent_config: false } }],
  ])('is absent with %s', async (_, config) => {
    server.use(http.get('/api/v1/config', () => HttpResponse.json(config)));
    const answered = configAnswered();
    renderApp('/agents');
    await answered;

    // No route matches, so not even the shell renders, as for any unknown path.
    expect(screen.queryByRole('button', { name: /^Scope/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('banner')).not.toBeInTheDocument();
    expect(document.body.textContent).toBe('');
  });

  it('asks for /config once, for the routes, the rail, and the page', async () => {
    let requests = 0;
    server.events.on('request:start', ({ request }) => {
      if (new URL(request.url).pathname === '/api/v1/config') requests += 1;
    });
    renderApp('/pods');
    expect(await screen.findByRole('link', { name: 'Agents' })).toBeInTheDocument();
    await waitFor(() => expect(requests).toBe(1));
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
