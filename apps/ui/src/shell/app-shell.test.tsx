import { cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { afterEach, describe, expect, it } from 'vitest';

import { AppShell } from './app-shell';
import { HeaderSlot } from './header-slot';

// The shell on its own, with stand-in screens: the rail, the links between the
// screens, and the header slot. The real pages have their own tests.

const RAIL_KEY = 'profiler.navRail.collapsed';

afterEach(() => {
  cleanup();
  localStorage.clear();
});

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
    expect(within(header).getByRole('button', { name: /^Switch to (dark|light) theme$/ })).toBeInTheDocument();
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
