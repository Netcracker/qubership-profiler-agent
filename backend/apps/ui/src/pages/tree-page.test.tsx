import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { pkToPath } from '../api/pk';
import type { CallJSON } from '../api/types';
import { server } from '../mocks/node';
import { callsPage } from '../mocks/synthetic';
import { TreePage } from './tree-page';

// The tree route against the MSW mock: a hot/hinted call decodes and renders
// merged rows; a cold bare PK and a truncated blob surface their 09 §5 states.

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  cleanup();
});
afterAll(() => server.close());

function firstCall(predicate: (c: CallJSON) => boolean, toMs: number): CallJSON {
  const q = {
    fromMs: toMs - 30 * 60 * 1000,
    toMs,
    pods: [],
    method: '',
    durationMinMs: 0,
    durationMaxMs: 0,
    errorOnly: false,
    retentionClasses: [],
  };
  let after: { tsMs: number; pkPath: string } | null = null;
  for (let page = 0; page < 20; page++) {
    const { calls, nextPos } = callsPage(q, after, 500, toMs);
    const hit = calls.find(predicate);
    if (hit !== undefined) return hit;
    if (nextPos === null) break;
    after = nextPos;
  }
  throw new Error('synthetic dataset produced no matching call');
}

function renderTreePage(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/tree/:pk" element={<TreePage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('TreePage', () => {
  it('renders the merged tree for a call with hints', async () => {
    const call = firstCall((c) => c.truncated_reason === null, Date.now());
    renderTreePage(
      `/tree/${encodeURIComponent(pkToPath(call.pk))}?ts_ms=${call.ts_ms}&retention_class=${call.retention_class}`,
    );
    // The synthetic tree always roots at CoyoteAdapter.service.
    expect(await screen.findByText(/CoyoteAdapter\.service/, undefined, { timeout: 5000 })).toBeInTheDocument();
    // The degenerate entry chain is skipped: reveal badges are present.
    expect((await screen.findAllByText(/⤵/)).length).toBeGreaterThan(0);
    // A loaded tree makes the header actions usable again. Raw trace has an
    // href once enabled, so AntD renders it as a link, not a button.
    expect(screen.getByRole('button', { name: 'Adjust duration' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Setup categories' })).toBeEnabled();
    const rawTrace = screen.getByRole('link', { name: /Raw trace/ });
    expect(rawTrace).toHaveAttribute('href');
    expect(rawTrace).not.toHaveAttribute('aria-disabled', 'true');
  });

  it('keeps derived views as closeable tabs', async () => {
    const call = firstCall((c) => c.truncated_reason === null, Date.now());
    renderTreePage(
      `/tree/${encodeURIComponent(pkToPath(call.pk))}?ts_ms=${call.ts_ms}&retention_class=${call.retention_class}`,
    );
    await screen.findByText(/CoyoteAdapter\.service/, undefined, { timeout: 5000 });

    // Open Outgoing calls from the root row's operations menu.
    fireEvent.click(screen.getAllByTitle('Operations')[0]!);
    fireEvent.click(await screen.findByText('Outgoing calls'));
    const outgoingTab = await screen.findByRole('tab', { name: /Outgoing · CoyoteAdapter\.service/ });
    expect(outgoingTab).toBeInTheDocument();

    // A second operation adds a tab instead of replacing the first.
    fireEvent.click(screen.getAllByTitle('Operations')[0]!);
    fireEvent.click((await screen.findAllByText('Incoming calls'))[0]!);
    const incomingTab = await screen.findByRole('tab', { name: /Incoming · CoyoteAdapter\.service/ });
    expect(incomingTab).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /Outgoing · CoyoteAdapter\.service/ })).toBeInTheDocument();

    // Closing the incoming tab keeps the outgoing one open.
    fireEvent.click(incomingTab.closest('.ant-tabs-tab')!.querySelector('.ant-tabs-tab-remove')!);
    expect(screen.queryByRole('tab', { name: /Incoming · CoyoteAdapter\.service/ })).toBeNull();
    expect(screen.getByRole('tab', { name: /Outgoing · CoyoteAdapter\.service/ })).toBeInTheDocument();
  });

  it('shows the cold-call state for a bare PK outside the hot window', async () => {
    const to = Date.now() - 2 * 60 * 60 * 1000;
    const call = firstCall((c) => c.truncated_reason === null, to);
    renderTreePage(`/tree/${encodeURIComponent(pkToPath(call.pk))}`);
    expect(
      await screen.findByText('This call is outside the hot window', undefined, { timeout: 5000 }),
    ).toBeInTheDocument();
    expect(screen.getByText(/Reopen the call from its row/)).toBeInTheDocument();
    // No tree model exists to adjust/categorize, and no trace can be located
    // without hints — the header actions must not look usable (PR 708
    // review #20).
    expect(screen.getByRole('button', { name: 'Adjust duration' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Setup categories' })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Raw trace/ })).toBeDisabled();
  });

  it('shows the truncated-blob state', async () => {
    const call = firstCall((c) => c.truncated_reason !== null, Date.now());
    renderTreePage(
      `/tree/${encodeURIComponent(pkToPath(call.pk))}?ts_ms=${call.ts_ms}&retention_class=${call.retention_class}`,
    );
    expect(
      await screen.findByText(/dropped under load/, undefined, { timeout: 5000 }),
    ).toBeInTheDocument();
  });
});
