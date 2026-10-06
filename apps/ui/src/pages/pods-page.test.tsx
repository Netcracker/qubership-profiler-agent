import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { HttpResponse, http } from 'msw';
import { MemoryRouter, Route, Routes } from 'react-router';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { BROWSER_ZONE } from '../controls/time-range';
import { server } from '../mocks/node';
import { setZone } from '../ui/timezone';
import { PodsPage } from './pods-page';

// Opening /ui/pods directly must offer its own period picker + discovery
// rail (09 §2.1-2.2, §4) instead of sending the user to Calls first
// (PR 708 review #16).

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  cleanup();
  setZone(BROWSER_ZONE); // the display zone is a module singleton; keep tests isolated
});
afterAll(() => server.close());

function renderPodsPage(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/pods" element={<PodsPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('PodsPage', () => {
  it('offers a period picker and rail without any URL window', async () => {
    renderPodsPage('/pods');
    expect(screen.getByRole('button', { name: 'Apply' })).toBeInTheDocument();
    expect(screen.getByPlaceholderText('Filter services')).toBeInTheDocument();
    expect(screen.getByText('Pick a period and Apply to see pods.')).toBeInTheDocument();
  });

  it('loads the pods table after picking a period and applying', async () => {
    const to = Date.now();
    renderPodsPage(`/pods?from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0`);
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });
  });

  it('keeps Apply disabled while the selection matches the committed one', async () => {
    const to = Date.now();
    renderPodsPage(`/pods?from=${to - 15 * 60 * 1000}&to=${to}&service=payments/billing`);
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });
    // Nothing to apply: the draft selection equals the committed one. The name
    // may carry a loading spinner, so match the trailing label.
    await waitFor(() => expect(screen.getByRole('button', { name: /Apply$/ })).toBeDisabled());
  });

  it('pins a live relative range to an absolute permalink on "y"', async () => {
    renderPodsPage('/pods?from=now-3h&to=now');
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });
    // The live range reads as its quick-range name.
    expect(screen.getByRole('button', { name: 'Time range' })).toHaveTextContent('Last 3 hours');

    fireEvent.keyDown(document.body, { key: 'y' });

    // Frozen: the trigger now shows an absolute timestamp window, not "Last 3 hours".
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Time range' })).toHaveTextContent(/\d{4}-\d{2}-\d{2}/),
    );
    expect(screen.getByRole('button', { name: 'Time range' })).not.toHaveTextContent('Last 3 hours');
  });

  it('applies a freshly picked period on a bare /pods, populating the table at once', async () => {
    renderPodsPage('/pods');
    // The quick ranges live inside the picker popover; picking one applies
    // immediately, with no separate Apply click.
    fireEvent.click(screen.getByRole('button', { name: 'Time range' }));
    fireEvent.click(screen.getByText('Last 15 minutes'));
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });
  });

  // PR 708 review #18: dump link-out is constructible only for td/top
  // dumps (heap dumps have no listing/handle-discovery route), and only
  // when the deployment configured a dumps-collector base URL.
  it('links each row to its td/top dump download once dumps-collector is configured', async () => {
    const to = Date.now();
    renderPodsPage(`/pods?from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0`);
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });

    const dataRow = screen.getAllByRole('row')[1]!;
    const threadLink = within(dataRow).getByRole('link', { name: 'Thread dumps' });
    const topLink = within(dataRow).getByRole('link', { name: 'Top dumps' });
    const threadHref = threadLink.getAttribute('href') ?? '';
    const topHref = topLink.getAttribute('href') ?? '';
    expect(threadHref).toMatch(/^https:\/\/dumps-collector-petclinic\.example\.com\/cdt\/v2\/download\?/);
    expect(new URL(threadHref).searchParams.get('type')).toBe('td');
    expect(new URL(topHref).searchParams.get('type')).toBe('top');
    for (const key of ['dateFrom', 'dateTo', 'namespace', 'service', 'podName']) {
      expect(new URL(threadHref).searchParams.get(key)).not.toBeNull();
    }
  });

  it('omits the Dumps column when no dumps-collector URL is configured', async () => {
    server.use(http.get('/api/v1/config', () => HttpResponse.json({ dumps_collector_url: '' })));
    const to = Date.now();
    renderPodsPage(`/pods?from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0`);
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });

    expect(screen.queryByText('Dumps')).not.toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'Thread dumps' })).not.toBeInTheDocument();
  });

  // PR 708 review #10: a non-http(s) dumps-collector value must never reach a
  // clickable href, even if it slips past the backend's own guard.
  it('omits the Dumps column when the dumps-collector URL is not http(s)', async () => {
    server.use(http.get('/api/v1/config', () => HttpResponse.json({ dumps_collector_url: 'javascript:alert(1)' })));
    const to = Date.now();
    renderPodsPage(`/pods?from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0`);
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });

    expect(screen.queryByText('Dumps')).not.toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'Thread dumps' })).not.toBeInTheDocument();
  });

  // PR 708 review #6: an individual pod= selection must filter the table, not
  // just service selections.
  it('filters the table to an individually selected pod', async () => {
    const tupleOf = (row: HTMLElement): string =>
      within(row).getByText((c) => /^[^/]+\/[^/]+\/[^/]+$/.test(c)).textContent ?? '';

    const to = Date.now();
    const from = to - 15 * 60 * 1000;

    // First load with no pod filter to read a real pod tuple from the data.
    const { unmount } = renderPodsPage(`/pods?from=${from}&to=${to}&duration_min_ms=0`);
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });
    const unfiltered = screen.getAllByRole('row').slice(1).map(tupleOf);
    const selected = unfiltered[0]!;
    expect(new Set(unfiltered).size).toBeGreaterThan(1); // the view really holds other pods
    unmount();

    renderPodsPage(`/pods?from=${from}&to=${to}&pod=${encodeURIComponent(selected)}&duration_min_ms=0`);
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });
    const filtered = screen.getAllByRole('row').slice(1).map(tupleOf);
    expect(new Set(filtered)).toEqual(new Set([selected]));
  });

  // The pod table renders "Session start"/"Data range" in the app-wide display
  // zone, like the calls table and the call tree — not always the browser's.
  it('renders the pod table timestamps in the selected display zone', async () => {
    // A fixed instant so the assertion is machine-timezone-independent: at UTC
    // it reads back as this exact wall-clock, whatever the test host's zone.
    const restartMs = Date.UTC(2026, 0, 2, 3, 4, 5);
    server.use(
      http.get('/api/v1/pods', () =>
        HttpResponse.json({
          pods: [
            {
              namespace: 'payments',
              service: 'billing',
              pod: 'billing-abcd',
              restart_time_ms: restartMs,
              time_min_ms: restartMs,
              time_max_ms: restartMs + 60_000,
            },
          ],
          partial: false,
          partial_reasons: [],
        }),
      ),
    );
    setZone('UTC');

    const to = Date.now();
    renderPodsPage(`/pods?from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0`);
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });

    const dataRow = screen.getAllByRole('row')[1]!;
    expect(within(dataRow).getByText('2026-01-02 03:04:05.000')).toBeInTheDocument();
    expect(within(dataRow).getByText('2026-01-02 03:04:05.000 — 2026-01-02 03:05:05.000')).toBeInTheDocument();
  });
});
