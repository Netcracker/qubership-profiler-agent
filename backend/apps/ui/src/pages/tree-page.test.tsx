import { render, screen } from '@testing-library/react';
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
afterEach(() => server.resetHandlers());
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
    // The degenerate entry chain is skipped: the reveal badge is present.
    expect(await screen.findByText(/⤵/)).toBeInTheDocument();
  });

  it('shows the cold-call state for a bare PK outside the hot window', async () => {
    const to = Date.now() - 2 * 60 * 60 * 1000;
    const call = firstCall((c) => c.truncated_reason === null, to);
    renderTreePage(`/tree/${encodeURIComponent(pkToPath(call.pk))}`);
    expect(
      await screen.findByText('This call is outside the hot window', undefined, { timeout: 5000 }),
    ).toBeInTheDocument();
    expect(screen.getByText(/Reopen the call from its row/)).toBeInTheDocument();
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
