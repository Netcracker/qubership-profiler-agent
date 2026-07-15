import { render, screen, waitFor, within } from '@testing-library/react';
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
afterEach(() => server.resetHandlers());
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
  it('renders calls rows for an applied window', async () => {
    const to = Date.now();
    renderCallsPage(`from=${to - 15 * 60 * 1000}&to=${to}&duration_min_ms=0`);
    // Business methods from the synthetic topology appear as row titles.
    await waitFor(() => expect(screen.getAllByText(/com\.acme\./).length).toBeGreaterThan(0), {
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
    await waitFor(() => expect(screen.getAllByText(/com\.acme\./).length).toBeGreaterThan(0), {
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
});
