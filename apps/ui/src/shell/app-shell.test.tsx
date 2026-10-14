import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { HttpResponse, http } from 'msw';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { server } from '../mocks/node';
import { AppShell } from './app-shell';
import { HeaderSlot } from './header-slot';

// The shell on its own, with stand-in screens: the rail, the links between the
// screens, and the header slot. The real pages have their own tests.

const RAIL_KEY = 'profiler.navRail.collapsed';

/** Answers /config the way a backend without the agent-configuration feature does. */
function withoutAgentConfig(): void {
  server.use(http.get('/api/v1/config', () => HttpResponse.json({ dumps_collector_url: '' })));
}

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
// The mock reports the feature. Most cases here are about the two screens every
// deployment has, so they start without it.
beforeEach(() => withoutAgentConfig());
afterEach(() => {
  cleanup();
  server.resetHandlers();
  localStorage.clear();
});
afterAll(() => server.close());

/** A stand-in screen that shows where the router is and fills the header slot. */
function StubScreen({ name }: { name: string }) {
  const location = useLocation();
  return (
    <>
      <HeaderSlot>
        <button type="button">{name} control</button>
      </HeaderSlot>
      <output data-testid="location">{`${location.pathname}${location.search}`}</output>
    </>
  );
}

function renderShell(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route element={<AppShell />}>
          <Route path="/calls" element={<StubScreen name="Calls" />} />
          <Route path="/pods" element={<StubScreen name="Pods" />} />
          <Route path="/agents" element={<StubScreen name="Agents" />} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

describe('AppShell', () => {
  it('names the product and the current screen in the header', () => {
    renderShell('/pods');
    const header = screen.getByRole('banner');
    expect(within(header).getByText('Profiler')).toBeInTheDocument();
    expect(within(header).getByText('Pods Info')).toBeInTheDocument();
    expect(within(header).getByRole('radiogroup', { name: 'Color theme' })).toBeInTheDocument();
  });

  it("renders a screen's controls in the header slot", () => {
    renderShell('/calls');
    expect(within(screen.getByRole('banner')).getByRole('button', { name: 'Calls control' })).toBeInTheDocument();
  });

  it('links to Calls and Pods Info and marks the current screen', () => {
    renderShell('/calls');
    const rail = screen.getByRole('navigation', { name: 'Screens' });
    expect(within(rail).getAllByRole('link').map((link) => link.getAttribute('aria-label'))).toEqual([
      'Calls',
      'Pods Info',
    ]);
    expect(within(rail).getByRole('link', { name: 'Calls' })).toHaveAttribute('aria-current', 'page');
    expect(within(rail).getByRole('link', { name: 'Pods Info' })).not.toHaveAttribute('aria-current');
  });

  it('keeps the search string when switching between Calls and Pods Info', () => {
    const search = '?from=now-3h&to=now&service=payments%2Fbilling';
    renderShell(`/calls${search}`);

    fireEvent.click(screen.getByRole('link', { name: 'Pods Info' }));
    expect(screen.getByTestId('location')).toHaveTextContent(`/pods${search}`);

    fireEvent.click(screen.getByRole('link', { name: 'Calls' }));
    expect(screen.getByTestId('location')).toHaveTextContent(`/calls${search}`);
  });
});

describe('the Agents screen', () => {
  it('is in the rail once /config reports the feature', async () => {
    server.resetHandlers();
    renderShell('/calls');
    const rail = screen.getByRole('navigation', { name: 'Screens' });
    expect(await within(rail).findByRole('link', { name: 'Agents' })).toBeInTheDocument();
    expect(within(rail).getAllByRole('link')).toHaveLength(3);
  });

  it('stays out of the rail when /config fails', async () => {
    let answered = false;
    server.use(
      http.get('/api/v1/config', () => {
        answered = true;
        return new HttpResponse(null, { status: 500 });
      }),
    );
    renderShell('/calls');
    await waitFor(() => expect(answered).toBe(true));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(screen.queryByRole('link', { name: 'Agents' })).not.toBeInTheDocument();
  });

  it('is named "Agent configuration" in the header', async () => {
    server.resetHandlers();
    renderShell('/agents');
    expect(await within(screen.getByRole('banner')).findByText('Agent configuration')).toBeInTheDocument();
  });

  it('has its own search string, and Calls gets its own back', async () => {
    server.resetHandlers();
    const search = '?from=now-3h&to=now';
    renderShell(`/calls${search}`);

    fireEvent.click(await screen.findByRole('link', { name: 'Agents' }));
    expect(screen.getByTestId('location')).toHaveTextContent(/^\/agents$/);

    fireEvent.click(screen.getByRole('link', { name: 'Pods Info' }));
    expect(screen.getByTestId('location')).toHaveTextContent(`/pods${search}`);
  });
});

describe('NavRail', () => {
  it('starts open and shows the screen names', () => {
    renderShell('/calls');
    const rail = screen.getByRole('navigation', { name: 'Screens' });
    expect(within(rail).getByRole('button', { name: 'Collapse navigation' })).toHaveAttribute('aria-expanded', 'true');
    expect(within(rail).getByText('Pods Info')).toBeInTheDocument();
  });

  it('collapses to icons and keeps the links reachable by name', () => {
    renderShell('/calls');
    const rail = screen.getByRole('navigation', { name: 'Screens' });

    fireEvent.click(within(rail).getByRole('button', { name: 'Collapse navigation' }));

    expect(within(rail).getByRole('button', { name: 'Expand navigation' })).toHaveAttribute('aria-expanded', 'false');
    expect(within(rail).queryByText('Pods Info')).not.toBeInTheDocument();
    expect(within(rail).getByRole('link', { name: 'Pods Info' })).toBeInTheDocument();
  });

  it('stores the collapsed state and restores it on the next mount', () => {
    const first = renderShell('/calls');
    fireEvent.click(screen.getByRole('button', { name: 'Collapse navigation' }));
    expect(localStorage.getItem(RAIL_KEY)).toBe('1');
    first.unmount();

    renderShell('/calls');
    expect(screen.getByRole('button', { name: 'Expand navigation' })).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Expand navigation' }));
    expect(localStorage.getItem(RAIL_KEY)).toBe('0');
  });
});
