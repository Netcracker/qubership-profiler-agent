import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { HttpResponse, http } from 'msw';
import { MemoryRouter, Route, Routes } from 'react-router';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { podsInRange } from '../mocks/synthetic';
import { server } from '../mocks/node';
import { CallsPage } from './calls-page';

// End-to-end DOM behaviour against the MSW mock: rows render from /calls,
// and the wide-query rejection turns into one-click narrowing that re-runs
// the query (09 §5).

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  cleanup();
});
afterAll(() => server.close());

function renderCallsPage(query: string) {
  return render(
    <MemoryRouter initialEntries={[`/calls?${query}`]}>
      <Routes>
        <Route path="/calls" element={<CallsPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('CallsPage', () => {
  it('lands on the default last-hour window with no URL params', async () => {
    // No `from`/`to`: Calls opts into the default window, so the rail and the
    // /calls fan-out both populate without the user first picking a period.
    const callsRequests: string[] = [];
    const onRequestStart = ({ request }: { request: Request }): void => {
      if (request.url.includes('/api/v1/calls')) callsRequests.push(request.url);
    };
    server.events.on('request:start', onRequestStart);
    try {
      renderCallsPage('duration_min_ms=0');
      await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), { timeout: 5000 });
      const spanOk = callsRequests.some((u) => {
        const sp = new URL(u).searchParams;
        return Number(sp.get('to')) - Number(sp.get('from')) === 60 * 60 * 1000;
      });
      expect(spanOk).toBe(true);
    } finally {
      server.events.removeListener('request:start', onRequestStart);
    }
  });

  it('renders calls rows for an applied window', async () => {
    const to = Date.now();
    renderCallsPage(`from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0`);
    // Table rows populate from /calls (the header row plus data rows).
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), {
      timeout: 5000,
    });
    expect(screen.getByText(/loaded/)).toBeInTheDocument();
  });

  it('turns the wide-query 400 into narrowing chips that re-run the query', async () => {
    const to = Date.now();
    renderCallsPage(`from=${to - 7 * 60 * 60 * 1000}&to=${to}&duration_min_ms=0`);

    const banner = await screen.findByText('Query too wide', undefined, { timeout: 5000 });
    const alert = banner.closest('[role="alert"]');
    expect(alert).not.toBeNull();

    await userEvent.click(within(alert as HTMLElement).getByRole('button', { name: '>500ms' }));
    // duration_min_ms=500 is a narrowing filter (guard.go), so the retried
    // query passes and rows arrive.
    await waitFor(() => expect(screen.getAllByRole('row').length).toBeGreaterThan(1), {
      timeout: 5000,
    });
    expect(screen.queryByText('Query too wide')).not.toBeInTheDocument();
  });

  it('warns in the results area when a partial /pods narrows the selection', async () => {
    // A partial /pods drops pods from the rail and silently narrows the /calls
    // pod filter; the results table must say so, not just the rail (09 §5).
    server.use(
      http.get('/api/v1/pods', ({ request }) => {
        const sp = new URL(request.url).searchParams;
        const fromMs = Number(sp.get('from'));
        const toMs = Number(sp.get('to'));
        return HttpResponse.json({
          pods: podsInRange(fromMs, toMs, Date.now()),
          partial: true,
          partial_reasons: ['collector-2 timed out'],
        });
      }),
    );

    const to = Date.now();
    renderCallsPage(`from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0&service=payments/billing`);

    const banner = await screen.findByText(
      /These results may be narrowed/,
      undefined,
      { timeout: 5000 },
    );
    const alert = banner.closest('[role="alert"]');
    expect(alert).not.toBeNull();
    // The originating reason travels with the content-area banner (the rail
    // shows its own copy, so scope the assertion to this alert).
    expect(within(alert as HTMLElement).getByText(/collector-2 timed out/)).toBeInTheDocument();
  });

  it('applies a picked period immediately, refetching calls without a separate Apply', async () => {
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

    const callsRequests: string[] = [];
    const onRequestStart = ({ request }: { request: Request }): void => {
      if (request.url.includes('/api/v1/calls')) callsRequests.push(request.url);
    };
    server.events.on('request:start', onRequestStart);

    try {
      const to = Date.now();
      renderCallsPage(`from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0&service=payments/billing`);
      await waitFor(() => expect(callsRequests.some((u) => u.includes('narrow-window-pod'))).toBe(true), {
        timeout: 5000,
      });

      // Picking "Last 1 hour" applies at once (Grafana-style): the /calls
      // fan-out refetches against the wider window with no Apply click.
      fireEvent.click(screen.getByRole('button', { name: 'Time range' }));
      fireEvent.click(screen.getByText('Last 1 hour'));
      await waitFor(() => expect(callsRequests.some((u) => u.includes('wide-window-pod'))).toBe(true), {
        timeout: 5000,
      });
    } finally {
      server.events.removeListener('request:start', onRequestStart);
    }
  });

  it('warns instead of sending an oversized request when a service selection expands to too many pods', async () => {
    // /calls has no `service` param (02 §2.3) — a service selection expands
    // client-side into repeatable `pod` params. On a large cluster that can
    // build a request line a proxy or browser rejects outright; catch it
    // before sending, not as an opaque network failure (PR 708 review #8).
    const hugePodList = Array.from({ length: 300 }, (_, i) => ({
      namespace: 'payments',
      service: 'billing',
      pod: `billing-${String(i).padStart(6, '0')}`,
      restart_time_ms: 0,
      time_min_ms: 0,
      time_max_ms: 0,
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

    const callsRequests: string[] = [];
    const onRequestStart = ({ request }: { request: Request }): void => {
      if (request.url.includes('/api/v1/calls')) callsRequests.push(request.url);
    };
    server.events.on('request:start', onRequestStart);

    try {
      const to = Date.now();
      renderCallsPage(`from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0&service=payments/billing`);

      const banner = await screen.findByText('Selection too wide', undefined, { timeout: 5000 });
      const alert = banner.closest('[role="alert"]');
      expect(alert).not.toBeNull();
      expect(callsRequests).toEqual([]);

      await userEvent.click(within(alert as HTMLElement).getByRole('button', { name: 'Clear selection' }));
      await waitFor(() => expect(screen.queryByText('Selection too wide')).not.toBeInTheDocument());
    } finally {
      server.events.removeListener('request:start', onRequestStart);
    }
  });
});
