import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { RouterProvider, createMemoryRouter } from 'react-router';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { AGENT_APPLY_DELAY_MS, resetAgentConfigMock } from '../mocks/agent-config';
import { server } from '../mocks/node';
import { AgentsPage } from './agents-page';

// The page against the mock of the draft API (plan 2026-10-13-ui-redesign
// §4.7). The mock derives agent statuses from Date.now(), so the tests fake
// Date only: timers stay real and the page polls on a short interval.

const POLL_MS = 20;

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  resetAgentConfigMock();
});
afterEach(() => {
  cleanup();
  server.resetHandlers();
  server.events.removeAllListeners();
  vi.useRealTimers();
});
afterAll(() => server.close());

/** A data router, as in the app: the page holds navigations with `useBlocker`. */
function renderAgentsPage(path: string) {
  const router = createMemoryRouter(
    [
      { path: '/agents', element: <AgentsPage pollIntervalMs={POLL_MS} /> },
      { path: '/calls', element: <div>Calls screen</div> },
    ],
    { initialEntries: [path] },
  );
  render(
    <AntdApp>
      <RouterProvider router={router} />
    </AntdApp>,
  );
  return router;
}

/** Counts the requests for the agents of a scope. */
function countAgentRequests(): () => number {
  let count = 0;
  server.events.on('request:start', ({ request }) => {
    if (new URL(request.url).pathname.endsWith('/agents')) count += 1;
  });
  return () => count;
}

const scopeButton = (): HTMLElement => screen.getByRole('button', { name: /^Scope/ });

async function openScopeList(): Promise<HTMLElement> {
  fireEvent.click(scopeButton());
  const list = await screen.findByRole('listbox', { name: 'Scope' });
  await within(list).findByText('Default');
  return list;
}

function option(list: HTMLElement, name: string, index = 0): HTMLElement {
  const match = within(list).getAllByText(name)[index]?.closest('[role="option"]');
  if (!(match instanceof HTMLElement)) throw new Error(`no scope option named ${name}`);
  return match;
}

describe('AgentsPage frame', () => {
  it('opens the Default scope on the Settings tab without a query string', async () => {
    renderAgentsPage('/agents');
    expect(scopeButton()).toHaveTextContent('Default');
    expect(screen.getByText('Applies to every agent without a more specific config')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Settings' })).toHaveAttribute('aria-selected', 'true');
    expect(await screen.findByText('revision 14')).toBeInTheDocument();
    expect(await screen.findByRole('tab', { name: 'Agents (13)' })).toBeInTheDocument();
  });

  it('summarizes the agents of the scope by status', async () => {
    renderAgentsPage('/agents?scope=staging-orders');
    expect(screen.getByText('Namespace')).toBeInTheDocument();
    expect(await screen.findByText('1 applied')).toBeInTheDocument();
    expect(screen.getByText('1 not managed')).toBeInTheDocument();
    expect(screen.queryByText(/failed$/)).not.toBeInTheDocument();
  });

  it('names the scope that an inheriting scope takes its config from', async () => {
    renderAgentsPage('/agents?scope=prod-orders%2Forder-api-gateway');
    expect(scopeButton()).toHaveTextContent('prod-orders / order-api-gateway');
    const banner = await screen.findByText(/This scope has no config of its own/);
    expect(within(banner).getByText('Default')).toBeInTheDocument();
  });

  it('shows no inheritance note on a scope with its own config', async () => {
    renderAgentsPage('/agents?scope=prod-orders%2Forder-processing');
    expect(await screen.findByText('revision 14')).toBeInTheDocument();
    expect(screen.queryByText(/This scope has no config of its own/)).not.toBeInTheDocument();
  });

  it('reports an unknown scope and offers a retry', async () => {
    renderAgentsPage('/agents?scope=no-such-namespace');
    expect(await screen.findByText('Could not load the config of no-such-namespace')).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: 'Retry' }).length).toBeGreaterThan(0);
  });
});

describe('scope picker', () => {
  it('marks each scope with the worst status of its agents', async () => {
    renderAgentsPage('/agents');
    const list = await openScopeList();
    const dot = (name: string, index = 0): string | null =>
      within(option(list, name, index)).getByRole('img').getAttribute('aria-label');

    expect(dot('Default')).toBe('Failed');
    expect(dot('prod-orders')).toBe('Failed');
    expect(dot('prod-billing')).toBe('Applied');
    expect(dot('staging-orders')).toBe('Not managed');
    // Two services share the name; the first is the one under prod-orders.
    expect(dot('order-api-gateway', 0)).toBe('Applied');
    expect(dot('order-api-gateway', 1)).toBe('Not managed');
  });

  it('puts the worst status of the open scope on the button', async () => {
    renderAgentsPage('/agents?scope=prod-orders%2Forder-processing');
    await screen.findByText('1 failed');
    expect(scopeButton().querySelector('[data-status]')).toHaveAttribute('data-status', 'failed');
  });

  it('shows the own-config badge and the agent count of a scope', async () => {
    renderAgentsPage('/agents');
    const list = await openScopeList();
    expect(option(list, 'order-processing')).toHaveTextContent('own config');
    expect(option(list, 'order-processing')).toHaveTextContent(/3$/);
    expect(option(list, 'inventory-sync')).not.toHaveTextContent('own config');
    // Default always has a config, so the badge would say nothing there.
    expect(option(list, 'Default')).not.toHaveTextContent('own config');
  });

  it('writes the picked scope to the URL and keeps the tab', async () => {
    const router = renderAgentsPage('/agents?tab=agents');
    const list = await openScopeList();
    fireEvent.click(option(list, 'billing-core'));

    await waitFor(() => expect(router.state.location.search).toBe('?scope=prod-billing%2Fbilling-core&tab=agents'));
    expect(scopeButton()).toHaveTextContent('prod-billing / billing-core');
    expect(await screen.findByRole('tab', { name: 'Agents (2)' })).toHaveAttribute('aria-selected', 'true');
  });
});

describe('tabs', () => {
  it('writes the picked tab to the URL and keeps the scope', async () => {
    const router = renderAgentsPage('/agents?scope=prod-billing');
    fireEvent.click(screen.getByRole('tab', { name: 'Raw config' }));
    await waitFor(() => expect(router.state.location.search).toBe('?scope=prod-billing&tab=raw'));

    fireEvent.click(screen.getByRole('tab', { name: 'Settings' }));
    await waitFor(() => expect(router.state.location.search).toBe('?scope=prod-billing'));
  });
});

describe('Settings tab', () => {
  const card = (title: string): Promise<HTMLElement> => screen.findByRole('group', { name: title });

  it('shows the settings of a scope with its own config', async () => {
    renderAgentsPage('/agents?scope=prod-orders%2Forder-processing');
    const packages = await card('Packages');
    expect(within(packages).getByText('com.acme.orders.**')).toBeInTheDocument();
    expect(within(packages).getByText('com.acme.orders.generated.**')).toBeInTheDocument();
    expect(within(await card('Call parameters')).getByText('order.id')).toBeInTheDocument();
    expect(within(await card('Plugins')).getByText(/8 of 22 are on\.$/)).toBeInTheDocument();
    expect(within(await card('Set at startup')).getByText('profiler-collector.profiler:1715')).toBeInTheDocument();
  });

  it('shows an inheriting scope the settings of the scope it inherits from', async () => {
    renderAgentsPage('/agents?scope=staging-orders%2Forder-processing');
    // staging-orders turns recording off, and its services inherit that.
    expect(within(await card('Profiling')).getByRole('switch')).not.toBeChecked();
    expect(within(await card('Packages')).getByText('com.acme.**')).toBeInTheDocument();
  });

  it('starts another scope from its own config, without the edits made before', async () => {
    renderAgentsPage('/agents?scope=prod-billing');
    fireEvent.click(within(await card('Profiling')).getByRole('switch'));
    expect(within(await card('Profiling')).getByRole('switch')).not.toBeChecked();

    fireEvent.click(option(await openScopeList(), 'billing-core'));
    await waitFor(() => expect(scopeButton()).toHaveTextContent('prod-billing / billing-core'));
    expect(within(await card('Profiling')).getByRole('switch')).toBeChecked();

    fireEvent.click(option(await openScopeList(), 'prod-billing'));
    await waitFor(() => expect(scopeButton()).toHaveTextContent(/prod-billing$/));
    expect(within(await card('Profiling')).getByRole('switch')).toBeChecked();
  });
});

describe('Agents tab', () => {
  it('lists the agents of the scope with their revision, status, and detail', async () => {
    renderAgentsPage('/agents?scope=prod-orders%2Forder-processing&tab=agents');
    const failed = (await screen.findByText('order-processing-7c9d4f6b8-m4tqz')).closest('tr');
    if (failed === null) throw new Error('the agent is not in a table row');

    expect(within(failed).getByText('3.2.1')).toBeInTheDocument();
    expect(within(failed).getByText('13 → 14')).toBeInTheDocument();
    expect(within(failed).getByText('Failed')).toBeInTheDocument();
    expect(within(failed).getByText(/Class reload failed for 2 of 148 classes/)).toBeInTheDocument();
    expect(screen.getByText('order-processing-7c9d4f6b8-x2k9p').closest('tr')).toHaveTextContent('Applied');
  });

  it('shows no revision for an agent that cannot be managed', async () => {
    renderAgentsPage('/agents?scope=staging-orders%2Forder-api-gateway&tab=agents');
    const row = (await screen.findByText('order-api-gateway-6c9d8b7f5-n5pvh')).closest('tr');
    expect(row).toHaveTextContent('n/a');
    expect(row).toHaveTextContent('Not managed');
  });

  it('polls while an agent is pending and stops once none is', async () => {
    const requests = countAgentRequests();
    renderAgentsPage('/agents?scope=prod-orders%2Forder-processing&tab=agents');

    expect(await screen.findByText('Pending')).toBeInTheDocument();
    await waitFor(() => expect(requests()).toBeGreaterThan(3));

    // The pending agent reports, and the next poll sees it applied.
    vi.setSystemTime(Date.now() + AGENT_APPLY_DELAY_MS);
    await waitFor(() => expect(screen.queryByText('Pending')).not.toBeInTheDocument());
    expect(screen.queryByText('1 pending')).not.toBeInTheDocument();
    expect(screen.getByText('2 applied')).toBeInTheDocument();

    const settled = requests();
    await new Promise((resolve) => setTimeout(resolve, POLL_MS * 10));
    expect(requests()).toBe(settled);
  });

  it('does not poll a scope without a pending agent', async () => {
    const requests = countAgentRequests();
    renderAgentsPage('/agents?scope=prod-billing&tab=agents');

    expect(await screen.findByText('billing-core-8b6d5c7f9-h4wsm')).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, POLL_MS * 10));
    expect(requests()).toBe(1);
  });
});
