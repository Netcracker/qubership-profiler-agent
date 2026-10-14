import { cleanup, configure, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App as AntdApp, ConfigProvider } from 'antd';
import { HttpResponse, http } from 'msw';
import { RouterProvider, createMemoryRouter } from 'react-router';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { fetchAgentConfig, renderAgentConfig, saveAgentConfig } from '../agents/api';
import type { AgentConfigSettings } from '../agents/types';
import { AGENT_APPLY_DELAY_MS, resetAgentConfigMock } from '../mocks/agent-config';
import { server } from '../mocks/node';
import { AgentsPage } from './agents-page';

// The page against the mock of the draft API (plan 2026-10-13-ui-redesign
// §4.7). The mock derives agent statuses from Date.now(), so the tests fake
// Date only: timers stay real, and the polling tests poll on a short interval.

const POLL_MS = 20;

// The other tests keep the page from polling. Each poll answer is an urgent
// state update, and react-router renders a navigation as a transition, which
// an urgent update interrupts and restarts. A scope with a pending agent polls
// from the start, so at 20 ms a tab switch that takes longer than that to
// render, as it does when the whole suite runs at once, keeps restarting and
// misses the wait for it.
const NO_POLL_MS = 60 * 60 * 1000;

// The default second is too short for a page that loads three resources when
// the whole suite runs at once.
configure({ asyncUtilTimeout: 5000 });

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
function renderAgentsPage(path: string, pollIntervalMs = NO_POLL_MS) {
  const router = createMemoryRouter(
    [
      { path: '/agents', element: <AgentsPage pollIntervalMs={pollIntervalMs} /> },
      { path: '/calls', element: <div>Calls screen</div> },
    ],
    { initialEntries: [path] },
  );
  render(
    // Without motion a closed dialog or popover leaves the DOM at once, not
    // after an animation that jsdom never finishes.
    <ConfigProvider theme={{ token: { motion: false } }}>
      <AntdApp>
        <RouterProvider router={router} />
      </AntdApp>
    </ConfigProvider>,
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

// Found by the title text: antd gives every generated id the same value under
// test, so a dialog's accessible name can resolve to another element.
async function findDialog(title: string): Promise<HTMLElement> {
  const dialog = (await screen.findByText(title)).closest('[role="dialog"]');
  if (!(dialog instanceof HTMLElement)) throw new Error(`${title} is not in a dialog`);
  return dialog;
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
    renderAgentsPage('/agents?scope=prod-orders%2Forder-processing&tab=agents', POLL_MS);

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
    renderAgentsPage('/agents?scope=prod-billing&tab=agents', POLL_MS);

    expect(await screen.findByText('billing-core-8b6d5c7f9-h4wsm')).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, POLL_MS * 10));
    expect(requests()).toBe(1);
  });
});

/** Records the requests that change a config: the method, the path, and the JSON body. */
function recordWrites(): { method: string; path: string; body: unknown }[] {
  const writes: { method: string; path: string; body: unknown }[] = [];
  server.events.on('request:start', async ({ request }) => {
    if (request.method !== 'PUT' && request.method !== 'DELETE') return;
    const text = await request.clone().text();
    writes.push({
      method: request.method,
      path: decodeURIComponent(new URL(request.url).pathname),
      body: text === '' ? null : JSON.parse(text),
    });
  });
  return writes;
}

/** Flips "Record calls", the smallest edit there is. */
async function toggleRecording(): Promise<void> {
  fireEvent.click(within(await screen.findByRole('group', { name: 'Profiling' })).getByRole('switch'));
}

const applyBar = (): HTMLElement | null => screen.queryByRole('region', { name: 'Unsaved changes' });

describe('applying changes', () => {
  it('shows the bar on the first change and hides it on Discard', async () => {
    renderAgentsPage('/agents?scope=prod-billing');
    await screen.findByRole('group', { name: 'Profiling' });
    expect(applyBar()).not.toBeInTheDocument();

    await toggleRecording();
    expect(applyBar()).toHaveTextContent('You have unsaved changes.');

    fireEvent.click(screen.getByRole('button', { name: 'Discard' }));
    expect(applyBar()).not.toBeInTheDocument();
    expect(within(screen.getByRole('group', { name: 'Profiling' })).getByRole('switch')).toBeChecked();
  });

  it('hides the bar when the change is undone by hand', async () => {
    renderAgentsPage('/agents?scope=prod-billing');
    await toggleRecording();
    expect(applyBar()).toBeInTheDocument();
    await toggleRecording();
    expect(applyBar()).not.toBeInTheDocument();
  });

  it('counts the agents that can pull the config, not the unmanaged ones', async () => {
    // staging-orders has two agents, and one of them cannot pull config.
    renderAgentsPage('/agents?scope=staging-orders');
    await screen.findByRole('tab', { name: 'Agents (2)' });
    await toggleRecording();
    expect(screen.getByRole('button', { name: 'Apply to 1 agent' })).toBeInTheDocument();
  });

  it('sends the revision the page loaded and then shows the agents', async () => {
    const writes = recordWrites();
    const router = renderAgentsPage('/agents?scope=prod-orders%2Forder-processing');
    expect(await screen.findByText('revision 14')).toBeInTheDocument();
    await screen.findByRole('tab', { name: 'Agents (3)' });
    await toggleRecording();

    fireEvent.click(screen.getByRole('button', { name: 'Apply to 3 agents' }));

    await waitFor(() => expect(router.state.location.search).toBe('?scope=prod-orders%2Forder-processing&tab=agents'));
    await waitFor(() => expect(writes).toHaveLength(1));
    expect(writes[0]).toMatchObject({
      method: 'PUT',
      path: '/api/v1/agent-config/prod-orders/order-processing',
      body: { expected_revision: 14, settings: { enabled: false } },
    });
    expect(await screen.findByText('revision 15')).toBeInTheDocument();
    await waitFor(() => expect(applyBar()).not.toBeInTheDocument());
    // Every managed agent of the scope was handed the new revision.
    await waitFor(() => expect(screen.getByText('3 pending')).toBeInTheDocument());
    expect((await screen.findAllByText('14 → 15')).length).toBeGreaterThan(0);
  });

  it('gives an inheriting scope its own config on apply', async () => {
    const writes = recordWrites();
    renderAgentsPage('/agents?scope=prod-orders%2Forder-api-gateway');
    expect(await screen.findByText(/This scope has no config of its own/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Use inherited config' })).not.toBeInTheDocument();
    await screen.findByRole('tab', { name: 'Agents (2)' });

    await toggleRecording();
    expect(applyBar()).toHaveTextContent('Applying gives this scope its own config, as a new revision.');
    fireEvent.click(screen.getByRole('button', { name: 'Apply to 2 agents' }));

    await waitFor(() => expect(screen.queryByText(/This scope has no config of its own/)).not.toBeInTheDocument());
    // The save went to the scope itself, against the revision of the inherited config.
    expect(writes[0]).toMatchObject({
      method: 'PUT',
      path: '/api/v1/agent-config/prod-orders/order-api-gateway',
      body: { expected_revision: 14 },
    });
    expect(screen.getByText('revision 15')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Use inherited config' })).toBeInTheDocument();
    expect(option(await openScopeList(), 'order-api-gateway')).toHaveTextContent('own config');
  });

  it('deletes the own config of a scope on "Use inherited config"', async () => {
    const writes = recordWrites();
    const router = renderAgentsPage('/agents?scope=prod-orders%2Forder-processing');
    fireEvent.click(await screen.findByRole('button', { name: 'Use inherited config' }));
    expect(await screen.findByText(/Its agents get the config of Default\./)).toBeInTheDocument();
    expect(writes).toHaveLength(0);

    fireEvent.click(screen.getByRole('button', { name: 'Delete own config' }));

    const banner = await screen.findByText(/This scope has no config of its own/);
    expect(within(banner).getByText('Default')).toBeInTheDocument();
    expect(writes).toEqual([
      { method: 'DELETE', path: '/api/v1/agent-config/prod-orders/order-processing', body: null },
    ]);
    expect(screen.queryByRole('button', { name: 'Use inherited config' })).not.toBeInTheDocument();
    await waitFor(() => expect(router.state.location.search).toContain('tab=agents'));
  });

  it('offers no way back to an inherited config on Default', async () => {
    renderAgentsPage('/agents');
    expect(await screen.findByText('revision 14')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Use inherited config' })).not.toBeInTheDocument();
  });

  it('says that someone else saved first, and reloads or keeps the edits', async () => {
    const scope = 'prod-billing';
    renderAgentsPage(`/agents?scope=${scope}`);
    await screen.findByRole('tab', { name: 'Agents (4)' });
    await toggleRecording();

    // Someone else saves the scope while this page holds revision 14.
    const theirs = await fetchAgentConfig(scope);
    await saveAgentConfig(scope, {
      expected_revision: theirs.revision,
      settings: { ...theirs.settings, min_duration_ms: 77 },
    });

    fireEvent.click(screen.getByRole('button', { name: 'Apply to 4 agents' }));
    const dialog = await findDialog('Someone else saved this scope first');

    fireEvent.click(within(dialog).getByRole('button', { name: 'Keep editing' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(applyBar()).toBeInTheDocument();
    expect(within(screen.getByRole('group', { name: 'Profiling' })).getByRole('switch')).not.toBeChecked();
    expect(screen.getByText('revision 14')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Apply to 4 agents' }));
    fireEvent.click(
      within(await findDialog('Someone else saved this scope first')).getByRole('button', {
        name: 'Reload',
      }),
    );

    expect(await screen.findByText('revision 15')).toBeInTheDocument();
    expect(applyBar()).not.toBeInTheDocument();
    const profiling = screen.getByRole('group', { name: 'Profiling' });
    expect(within(profiling).getByRole('switch')).toBeChecked();
    expect(within(profiling).getByRole('spinbutton')).toHaveValue('77');
  });

  it('reports a save that fails for another reason and keeps the edits', async () => {
    server.use(http.put('/api/v1/agent-config/:scope', () => new HttpResponse(null, { status: 503 })));
    renderAgentsPage('/agents?scope=prod-billing');
    await toggleRecording();
    fireEvent.click(screen.getByRole('button', { name: /^Apply/ }));

    expect(await screen.findByText(/^Could not apply the changes: 503/)).toBeInTheDocument();
    expect(applyBar()).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
});

describe('leaving with unsaved changes', () => {
  const leaveDialog = (): Promise<HTMLElement> => findDialog('Discard unsaved changes?');

  it('asks before opening another scope, and stays on "Keep editing"', async () => {
    const router = renderAgentsPage('/agents?scope=prod-billing');
    await toggleRecording();

    fireEvent.click(option(await openScopeList(), 'billing-core'));
    fireEvent.click(within(await leaveDialog()).getByRole('button', { name: 'Keep editing' }));

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(router.state.location.search).toBe('?scope=prod-billing');
    expect(applyBar()).toBeInTheDocument();
  });

  it('opens the other scope and drops the edits on "Discard changes"', async () => {
    const router = renderAgentsPage('/agents?scope=prod-billing');
    await toggleRecording();

    fireEvent.click(option(await openScopeList(), 'billing-core'));
    fireEvent.click(within(await leaveDialog()).getByRole('button', { name: 'Discard changes' }));

    await waitFor(() => expect(router.state.location.search).toBe('?scope=prod-billing%2Fbilling-core'));
    expect(applyBar()).not.toBeInTheDocument();
    expect(within(await screen.findByRole('group', { name: 'Profiling' })).getByRole('switch')).toBeChecked();

    // The dropped edits do not come back with the scope they were made in.
    fireEvent.click(option(await openScopeList(), 'prod-billing'));
    await waitFor(() => expect(router.state.location.search).toBe('?scope=prod-billing'));
    expect(within(await screen.findByRole('group', { name: 'Profiling' })).getByRole('switch')).toBeChecked();
    expect(applyBar()).not.toBeInTheDocument();
  });

  it('asks before leaving for another screen', async () => {
    const router = renderAgentsPage('/agents?scope=prod-billing');
    await toggleRecording();

    void router.navigate('/calls');
    const dialog = await leaveDialog();
    expect(screen.queryByText('Calls screen')).not.toBeInTheDocument();

    fireEvent.click(within(dialog).getByRole('button', { name: 'Discard changes' }));
    expect(await screen.findByText('Calls screen')).toBeInTheDocument();
  });

  it('switches tabs without asking and keeps the edits', async () => {
    const router = renderAgentsPage('/agents?scope=prod-billing');
    await toggleRecording();

    fireEvent.click(screen.getByRole('tab', { name: /^Agents/ }));
    await waitFor(() => expect(router.state.location.search).toBe('?scope=prod-billing&tab=agents'));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(applyBar()).toBeInTheDocument();

    fireEvent.click(screen.getByRole('tab', { name: 'Settings' }));
    expect(within(await screen.findByRole('group', { name: 'Profiling' })).getByRole('switch')).not.toBeChecked();
  });

  it('leaves without asking when nothing changed', async () => {
    const router = renderAgentsPage('/agents?scope=prod-billing');
    await screen.findByRole('group', { name: 'Profiling' });
    void router.navigate('/calls');
    expect(await screen.findByText('Calls screen')).toBeInTheDocument();
  });

  it('asks the browser to confirm closing the tab only while there are edits', async () => {
    renderAgentsPage('/agents?scope=prod-billing');
    await screen.findByRole('group', { name: 'Profiling' });
    const closeTab = (): boolean => {
      const event = new Event('beforeunload', { cancelable: true });
      window.dispatchEvent(event);
      return event.defaultPrevented;
    };

    expect(closeTab()).toBe(false);
    await toggleRecording();
    expect(closeTab()).toBe(true);
    fireEvent.click(screen.getByRole('button', { name: 'Discard' }));
    expect(closeTab()).toBe(false);
  });
});

describe('Raw config tab', () => {
  const SCOPE = 'prod-billing';
  const HAND_XML = '<profiler-configuration>\n    <!-- written by hand -->\n</profiler-configuration>';

  const xmlArea = (): Promise<HTMLElement> => screen.findByRole('textbox', { name: 'Raw config XML' });

  /** What the server renders from the settings in effect for the scope. */
  async function generatedXml(change: Partial<AgentConfigSettings> = {}): Promise<string> {
    const { settings } = await fetchAgentConfig(SCOPE);
    return renderAgentConfig({ ...settings, ...change });
  }

  async function openTab(name: string | RegExp): Promise<void> {
    fireEvent.click(screen.getByRole('tab', { name }));
    await waitFor(() => expect(screen.getByRole('tab', { name })).toHaveAttribute('aria-selected', 'true'));
  }

  it('shows the XML that the server renders from the settings', async () => {
    const expected = await generatedXml();
    renderAgentsPage(`/agents?scope=${SCOPE}&tab=raw`);

    await waitFor(async () => expect(await xmlArea()).toHaveValue(expected));
    expect(screen.getByText('Generated from Settings')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Discard raw edits' })).not.toBeInTheDocument();
    expect(applyBar()).not.toBeInTheDocument();
  });

  it('renders the unsaved settings, not only the saved ones', async () => {
    const expected = await generatedXml({ enabled: false });
    renderAgentsPage(`/agents?scope=${SCOPE}`);
    await toggleRecording();

    await openTab('Raw config');
    await waitFor(async () => expect(await xmlArea()).toHaveValue(expected));
    expect(expected).toContain('name="profiler.enabled" value="false"');
  });

  it('reports a render that fails and renders again on Retry', async () => {
    const expected = await generatedXml();
    server.use(
      http.post('/api/v1/agent-config/render', () => new HttpResponse(null, { status: 503 }), { once: true }),
    );
    renderAgentsPage(`/agents?scope=${SCOPE}&tab=raw`);

    expect(await screen.findByText('Could not generate the XML')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    await waitFor(async () => expect(await xmlArea()).toHaveValue(expected));
    expect(screen.queryByText('Could not generate the XML')).not.toBeInTheDocument();
  });

  it('makes a hand edit the override, says so on Settings, and saves it as raw_xml', async () => {
    const writes = recordWrites();
    const generated = await generatedXml();
    renderAgentsPage(`/agents?scope=${SCOPE}&tab=raw`);
    await waitFor(async () => expect(await xmlArea()).toHaveValue(generated));
    await screen.findByRole('tab', { name: 'Agents (4)' });

    fireEvent.change(await xmlArea(), { target: { value: HAND_XML } });
    expect(screen.getByText('Edited by hand')).toBeInTheDocument();
    expect(await xmlArea()).toHaveValue(HAND_XML);
    expect(applyBar()).toBeInTheDocument();

    await openTab('Settings');
    expect(
      await screen.findByText('The raw config was edited by hand and overrides these settings.'),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Apply to 4 agents' }));
    await waitFor(() => expect(writes).toHaveLength(1));
    expect(writes[0]).toMatchObject({ method: 'PUT', body: { expected_revision: 14, raw_xml: HAND_XML } });

    // The saved override comes back from the server and still reads as a hand edit.
    await waitFor(() => expect(applyBar()).not.toBeInTheDocument());
    await openTab('Raw config');
    expect(await xmlArea()).toHaveValue(HAND_XML);
    expect(screen.getByText('Edited by hand')).toBeInTheDocument();
    expect((await fetchAgentConfig(SCOPE)).raw_xml).toBe(HAND_XML);
  });

  it('returns to the generated XML when the unsaved hand edit is discarded', async () => {
    const generated = await generatedXml();
    renderAgentsPage(`/agents?scope=${SCOPE}&tab=raw`);
    await waitFor(async () => expect(await xmlArea()).toHaveValue(generated));

    fireEvent.change(await xmlArea(), { target: { value: HAND_XML } });
    fireEvent.click(screen.getByRole('button', { name: 'Discard raw edits' }));

    await waitFor(async () => expect(await xmlArea()).toHaveValue(generated));
    expect(screen.getByText('Generated from Settings')).toBeInTheDocument();
    // Nothing was saved, so nothing is left to apply.
    expect(applyBar()).not.toBeInTheDocument();
  });

  it('discards a saved override from the Settings banner and saves raw_xml as null', async () => {
    const saved = await fetchAgentConfig(SCOPE);
    await saveAgentConfig(SCOPE, { expected_revision: saved.revision, settings: saved.settings, raw_xml: HAND_XML });
    const generated = await generatedXml();
    const writes = recordWrites();

    renderAgentsPage(`/agents?scope=${SCOPE}`);
    const banner = await screen.findByText('The raw config was edited by hand and overrides these settings.');
    expect(applyBar()).not.toBeInTheDocument();

    const alert = banner.closest('[role="alert"]');
    if (!(alert instanceof HTMLElement)) throw new Error('the banner is not an alert');
    fireEvent.click(within(alert).getByRole('button', { name: 'Discard raw edits' }));
    expect(screen.queryByText('The raw config was edited by hand and overrides these settings.')).toBeNull();
    expect(applyBar()).toBeInTheDocument();

    await openTab('Raw config');
    await waitFor(async () => expect(await xmlArea()).toHaveValue(generated));

    fireEvent.click(screen.getByRole('button', { name: /^Apply/ }));
    await waitFor(() => expect(writes).toHaveLength(1));
    expect(writes[0]).toMatchObject({ method: 'PUT', body: { raw_xml: null } });
    await waitFor(async () => expect((await fetchAgentConfig(SCOPE)).raw_xml).toBeNull());
  });

  it('asks for no render while the XML is hand-edited', async () => {
    const saved = await fetchAgentConfig(SCOPE);
    await saveAgentConfig(SCOPE, { expected_revision: saved.revision, settings: saved.settings, raw_xml: HAND_XML });
    let renders = 0;
    server.events.on('request:start', ({ request }) => {
      if (new URL(request.url).pathname.endsWith('/render')) renders += 1;
    });

    renderAgentsPage(`/agents?scope=${SCOPE}&tab=raw`);
    expect(await xmlArea()).toHaveValue(HAND_XML);
    await new Promise((resolve) => setTimeout(resolve, POLL_MS * 5));
    expect(renders).toBe(0);
  });
});
