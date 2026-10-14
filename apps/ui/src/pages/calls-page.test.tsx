import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { HttpResponse, http } from 'msw';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import { podsInRange } from '../mocks/synthetic';
import { server } from '../mocks/node';
import { CallsPage } from './calls-page';

// End-to-end DOM behaviour against the MSW mock: the filter bar drives /calls
// through the translation of redesign plan §4.2.1, and the wide-query
// rejection turns into one-click narrowing that re-runs the query (09 §5).

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  cleanup();
});
afterAll(() => server.close());

function Location() {
  const location = useLocation();
  return <div data-testid="location">{decodeURIComponent(location.search)}</div>;
}

function renderCallsPage(query: string) {
  return render(
    <MemoryRouter initialEntries={[`/calls${query === '' ? '' : `?${query}`}`]}>
      <Routes>
        <Route
          path="/calls"
          element={
            <>
              <CallsPage />
              <Location />
            </>
          }
        />
      </Routes>
    </MemoryRouter>,
  );
}

/** Records the /calls requests while a test runs. */
function recordCalls(): { urls: URL[]; stop: () => void } {
  const urls: URL[] = [];
  const onRequestStart = ({ request }: { request: Request }): void => {
    if (request.url.includes('/api/v1/calls')) urls.push(new URL(request.url));
  };
  server.events.on('request:start', onRequestStart);
  return { urls, stop: () => server.events.removeListener('request:start', onRequestStart) };
}

const rowsLoaded = () =>
  waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });

const window15m = (): string => {
  const to = Date.now();
  return `from=${to - 15 * 60 * 1000}&to=${to}`;
};

/** Reads formatDurationMs text back: `412ms`, `4.6s`, `3m 12s`, `1h 04m`. */
function durationMs(text: string): number {
  const units: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000 };
  return text.split(' ').reduce((sum, part) => {
    const m = /^([\d.]+)(ms|s|m|h)$/.exec(part);
    if (m === null) throw new Error(`not a duration: ${text}`);
    return sum + Number(m[1]) * units[m[2]!]!;
  }, 0);
}

const filterBar = () =>screen.getByRole('combobox', { name: 'Filter calls' });

describe('CallsPage', () => {
  it('renders no discovery rail, only the filter bar', () => {
    renderCallsPage(window15m());
    expect(filterBar()).toBeInTheDocument();
    expect(screen.queryByPlaceholderText('Filter services')).not.toBeInTheDocument();
    expect(screen.queryByRole('tree')).not.toBeInTheDocument();
    expect(document.querySelector('aside')).toBeNull();
  });

  it('gives a visit with no query string the defaults and writes them to the URL', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage('');
      await rowsLoaded();
      const first = calls.urls[0]!.searchParams;
      expect(Number(first.get('to')) - Number(first.get('from'))).toBe(60 * 60 * 1000);
      expect(first.get('duration_min_ms')).toBe('501');
      expect(screen.getByTestId('location')).toHaveTextContent(
        '?from=now-1h&to=now&filter=duration>500ms&order=start_desc&hide_system=1',
      );
      expect(screen.getByRole('switch', { name: 'Hide system/proxy' })).toBeChecked();
    } finally {
      calls.stop();
    }
  });

  it('applies no default filter when the query string has one', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage(`${window15m()}&order=start_desc`);
      await rowsLoaded();
      expect(calls.urls[0]!.searchParams.get('duration_min_ms')).toBeNull();
      expect(screen.getByRole('switch', { name: 'Hide system/proxy' })).not.toBeChecked();
    } finally {
      calls.stop();
    }
  });

  it('converts an old link to tokens and sends them', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage(`${window15m()}&service=payments/billing&error_only=true`);
      await waitFor(() => expect(calls.urls.length).toBeGreaterThan(0), { timeout: 5000 });
      const sp = calls.urls[0]!.searchParams;
      expect(sp.getAll('pod').length).toBeGreaterThan(0);
      expect(sp.getAll('pod').every((p) => p.startsWith('payments/billing/'))).toBe(true);
      expect(sp.get('duration_min_ms')).toBe('500');
      expect(sp.get('error_only')).toBe('true');
      expect(screen.getByRole('button', { name: 'Remove the filter service = billing' })).toBeInTheDocument();
      expect(screen.getByRole('switch', { name: 'Errors only' })).toBeChecked();
    } finally {
      calls.stop();
    }
  });

  it('keeps an untranslatable condition as a disabled token and does not send it', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage(`${window15m()}&filter=${encodeURIComponent('trace_id=abc')}&filter=${encodeURIComponent('duration>=1s')}`);
      await waitFor(() => expect(calls.urls.length).toBeGreaterThan(0), { timeout: 5000 });
      const sp = calls.urls[0]!.searchParams;
      expect([...sp.keys()].sort()).toEqual(['duration_min_ms', 'from', 'limit', 'to']);
      const token = screen.getByRole('button', { name: 'Remove the filter trace_id = abc' }).parentElement!;
      expect(token).toHaveAttribute('data-applied', 'false');
      expect(token).toHaveAttribute('title', 'The backend does not support this filter yet.');
    } finally {
      calls.stop();
    }
  });

  it('edits a draft and sends it only on Run query', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage(window15m());
      await rowsLoaded();
      const before = calls.urls.length;
      await userEvent.click(screen.getByText('>3s'));
      expect(screen.getByText('Filters changed. Run the query to update the results.')).toBeInTheDocument();
      expect(calls.urls.length).toBe(before);

      await userEvent.click(screen.getByRole('button', { name: /Run query$/ }));
      await waitFor(() => expect(calls.urls.at(-1)!.searchParams.get('duration_min_ms')).toBe('3001'));
      expect(screen.queryByText(/Filters changed/)).not.toBeInTheDocument();
      expect(screen.getByTestId('location')).toHaveTextContent('filter=duration>3s');
    } finally {
      calls.stop();
    }
  });

  it('runs the draft on Ctrl+Enter in the filter bar', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage(window15m());
      await rowsLoaded();
      await userEvent.click(filterBar());
      await userEvent.type(filterBar(), 'Tax{Enter}');
      await userEvent.keyboard('{Control>}{Enter}{/Control}');
      await waitFor(() => expect(calls.urls.at(-1)!.searchParams.get('method')).toBe('Tax'));
    } finally {
      calls.stop();
    }
  });

  it('restarts from the first page on Run query', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage(window15m());
      await rowsLoaded();
      await userEvent.click(screen.getByRole('button', { name: 'Load more' }));
      await waitFor(() => expect(calls.urls.some((u) => u.searchParams.has('cursor'))).toBe(true));
      const before = calls.urls.length;

      await userEvent.click(screen.getByRole('button', { name: /Run query$/ }));
      await waitFor(() => expect(calls.urls.length).toBeGreaterThan(before));
      const rerun = calls.urls[before]!.searchParams;
      expect(rerun.has('cursor')).toBe(false);
      expect(rerun.has('from')).toBe(true);
    } finally {
      calls.stop();
    }
  });

  it('replaces the pod token and runs on a row pick', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage(`${window15m()}&filter=${encodeURIComponent('pod=elsewhere')}`);
      // The pod matches nothing, so drop it to see rows first.
      await screen.findByText(/No pod in this window matches/, undefined, { timeout: 5000 });
      await userEvent.click(screen.getByRole('button', { name: 'Remove the filter pod = elsewhere' }));
      await userEvent.click(screen.getByRole('button', { name: /Run query$/ }));
      await rowsLoaded();

      const pick = screen.getAllByTitle(/^Filter by pod /)[0]!;
      const pod = pick.textContent!;
      const before = calls.urls.length;
      await userEvent.click(pick);
      await waitFor(() => expect(calls.urls.length).toBeGreaterThan(before));
      const pods = calls.urls.at(-1)!.searchParams.getAll('pod');
      expect(pods.length).toBeGreaterThan(0);
      expect(pods.every((t) => t.endsWith(`/${pod}`))).toBe(true);
      expect(screen.getByRole('button', { name: `Remove the filter pod = ${pod}` })).toBeInTheDocument();
      expect(screen.getByTestId('location')).toHaveTextContent(`filter=pod=${pod}`);
    } finally {
      calls.stop();
    }
  });

  it('sorts the loaded rows in the browser for an order the backend lacks', async () => {
    const calls = recordCalls();
    try {
      renderCallsPage(window15m());
      await rowsLoaded();
      // antd opens a Select on mousedown and renders the options in its own popup.
      fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Sort by' }));
      fireEvent.click(await screen.findByText('Duration', { selector: '.ant-select-item-option-content' }));
      await userEvent.click(screen.getByRole('button', { name: 'Descending. Click to sort ascending.' }));
      expect(screen.getByText(/Filters changed/)).toBeInTheDocument();
      const before = calls.urls.length;

      await userEvent.click(screen.getByRole('button', { name: /Run query$/ }));
      await waitFor(() => expect(calls.urls.length).toBeGreaterThan(before));
      expect(calls.urls.at(-1)!.searchParams.has('order')).toBe(false);
      expect(screen.getByTestId('location')).toHaveTextContent('order=duration_asc');
      await screen.findByText('Sort by Duration applies to the loaded rows only');
      const durations = screen
        .getAllByTitle('Open the call tree in a new tab')
        .map((a) => durationMs(a.textContent!));
      expect(durations).toEqual([...durations].sort((a, b) => a - b));
    } finally {
      calls.stop();
    }
  });

  it('sends an order the server sorts by and adds no footer note', async () => {
    vi.stubEnv('VITE_ENABLE_MSW', '1');
    const calls = recordCalls();
    try {
      renderCallsPage(`${window15m()}&order=cpu_desc`);
      await rowsLoaded();
      expect(calls.urls[0]!.searchParams.get('order')).toBe('cpu_desc');
      expect(screen.queryByText(/applies to the loaded rows only/)).not.toBeInTheDocument();
    } finally {
      calls.stop();
      vi.unstubAllEnvs();
    }
  });

  it('turns the wide-query 400 into narrowing chips that re-run the query', async () => {
    const to = Date.now();
    renderCallsPage(`from=${to - 7 * 60 * 60 * 1000}&to=${to}`);

    const banner = await screen.findByText('Query too wide', undefined, { timeout: 5000 });
    const alert = banner.closest('[role="alert"]');
    expect(alert).not.toBeNull();

    await userEvent.click(within(alert as HTMLElement).getByRole('button', { name: '>500ms' }));
    // A lower duration bound is a narrowing filter (guard.go), so the retried
    // query passes and rows arrive.
    await rowsLoaded();
    expect(screen.queryByText('Query too wide')).not.toBeInTheDocument();
  });

  it('warns in the results area when a partial /pods narrows the pod filter', async () => {
    server.use(
      http.get('/api/v1/pods', ({ request }) => {
        const sp = new URL(request.url).searchParams;
        return HttpResponse.json({
          pods: podsInRange(Number(sp.get('from')), Number(sp.get('to')), Date.now()),
          partial: true,
          partial_reasons: ['collector-2 timed out'],
        });
      }),
    );

    renderCallsPage(`${window15m()}&filter=${encodeURIComponent('service=billing')}`);

    const banner = await screen.findByText(/These results may be narrowed/, undefined, { timeout: 5000 });
    const alert = banner.closest('[role="alert"]');
    expect(alert).not.toBeNull();
    expect(within(alert as HTMLElement).getByText(/collector-2 timed out/)).toBeInTheDocument();
  });

  it('applies a picked period immediately, refetching calls without Run query', async () => {
    // Key the returned pod on the requested window's span so the committed
    // (15 min) and the picked (1 h) windows resolve to distinguishable pods,
    // making the refetched window observable in the /calls pod filter.
    server.use(
      http.get('/api/v1/pods', ({ request }) => {
        const sp = new URL(request.url).searchParams;
        const fromMs = Number(sp.get('from'));
        const toMs = Number(sp.get('to'));
        const pod = toMs - fromMs <= 20 * 60 * 1000 ? 'narrow-window-pod' : 'wide-window-pod';
        return HttpResponse.json({
          pods: [
            { namespace: 'payments', service: 'billing', pod, restart_time_ms: fromMs, time_min_ms: fromMs, time_max_ms: toMs },
          ],
          partial: false,
          partial_reasons: [],
        });
      }),
    );

    const calls = recordCalls();
    try {
      renderCallsPage(`${window15m()}&filter=${encodeURIComponent('service=billing')}`);
      await waitFor(() => expect(calls.urls.some((u) => u.search.includes('narrow-window-pod'))).toBe(true), {
        timeout: 5000,
      });

      fireEvent.click(screen.getByRole('button', { name: 'Time range' }));
      fireEvent.click(screen.getByText('Last 1 hour'));
      await waitFor(() => expect(calls.urls.some((u) => u.search.includes('wide-window-pod'))).toBe(true), {
        timeout: 5000,
      });
    } finally {
      calls.stop();
    }
  });

  it('warns instead of sending an oversized request when a service filter expands to too many pods', async () => {
    const hugePodList = Array.from({ length: 300 }, (_, i) => ({
      namespace: 'payments',
      service: 'billing',
      pod: `billing-${String(i).padStart(6, '0')}`,
    }));
    server.use(
      http.get('/api/v1/pods', ({ request }) => {
        const sp = new URL(request.url).searchParams;
        const fromMs = Number(sp.get('from'));
        const toMs = Number(sp.get('to'));
        return HttpResponse.json({
          pods: hugePodList.map((p) => ({ ...p, restart_time_ms: fromMs, time_min_ms: fromMs, time_max_ms: toMs })),
          partial: false,
          partial_reasons: [],
        });
      }),
    );

    const calls = recordCalls();
    try {
      renderCallsPage(`${window15m()}&filter=${encodeURIComponent('service=billing')}`);

      const banner = await screen.findByText('Selection too wide', undefined, { timeout: 5000 });
      const alert = banner.closest('[role="alert"]');
      expect(alert).not.toBeNull();
      expect(calls.urls).toEqual([]);

      await userEvent.click(within(alert as HTMLElement).getByRole('button', { name: 'Clear selection' }));
      await waitFor(() => expect(screen.queryByText('Selection too wide')).not.toBeInTheDocument());
      expect(screen.queryByRole('button', { name: 'Remove the filter service = billing' })).not.toBeInTheDocument();
    } finally {
      calls.stop();
    }
  });
});
