import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { HttpResponse, http } from 'msw';
import { MemoryRouter, Route, Routes } from 'react-router';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { server } from '../mocks/node';
import { PodsPage } from './pods-page';

// Opening /ui/pods directly must offer its own period picker + discovery
// rail (09 §2.1-2.2, §4) instead of sending the user to Calls first
// (PR 708 review #16).

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  cleanup();
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

  it('applying a freshly picked period on a bare /pods populates the table', async () => {
    renderPodsPage('/pods');
    fireEvent.click(screen.getByText('15 min'));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Apply' })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));
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
});
