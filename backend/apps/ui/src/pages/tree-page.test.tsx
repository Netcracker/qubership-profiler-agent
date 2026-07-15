import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter, Route, Routes } from 'react-router';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import { pkToPath } from '../api/pk';
import type { CallJSON } from '../api/types';
import { server } from '../mocks/node';
import { callsPage, treeForCall } from '../mocks/synthetic';
import { buildTreeModel } from '../tree/model';
import { summariseParams } from '../tree/params-summary';
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

// Finds a call whose tree has a sql shape carrying nested binds — the mock's
// db nodes (mocks/synthetic.ts sqlGroups) usually do, but not guaranteed for
// every seed, so this scans like firstCall rather than assuming call #1.
function callWithSqlBinds(toMs: number): CallJSON {
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
  for (let page = 0; page < 5; page++) {
    const { calls, nextPos } = callsPage(q, after, 500, toMs);
    for (const c of calls) {
      if (c.truncated_reason !== null) continue;
      const summary = summariseParams(buildTreeModel(treeForCall(c)));
      if (summary.sql.some((s) => s.binds.length > 0)) return c;
    }
    if (nextPos === null) break;
    after = nextPos;
  }
  throw new Error('synthetic dataset produced no call with an sql shape carrying binds');
}

// Wrapped in AntdApp, matching production (app.tsx) — the copy-to-clipboard
// toast (message.success) needs the App context to mount safely in jsdom.
function renderTreePage(path: string) {
  return render(
    <AntdApp>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route path="/tree/:pk" element={<TreePage />} />
        </Routes>
      </MemoryRouter>
    </AntdApp>,
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
    // A loaded tree makes the header actions usable again, including the
    // self-contained HTML export.
    expect(screen.getByRole('button', { name: 'Adjust duration' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Setup categories' })).toBeEnabled();
    expect(screen.getByRole('button', { name: /Download HTML/ })).toBeEnabled();
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
    // No tree model exists to adjust/categorize or to export — the header
    // actions must not look usable (PR 708 review #20).
    expect(screen.getByRole('button', { name: 'Adjust duration' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Setup categories' })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Download HTML/ })).toBeDisabled();
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

  it('opens the full-value viewer from the Parameters tab', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: vi.fn().mockResolvedValue(undefined) },
      configurable: true,
    });
    const call = firstCall((c) => c.truncated_reason === null, Date.now());
    renderTreePage(
      `/tree/${encodeURIComponent(pkToPath(call.pk))}?ts_ms=${call.ts_ms}&retention_class=${call.retention_class}`,
    );
    await screen.findByText(/CoyoteAdapter\.service/, undefined, { timeout: 5000 });

    // request.id and java.thread are unconditional root params (mocks/synthetic.ts).
    fireEvent.click(screen.getByRole('tab', { name: 'Parameters' }));
    // getByTitle, not getByRole(..., {name}): dom-testing-library's accessible-name
    // computation hangs against this icon-only AntD button (same reason the existing
    // Operations-menu tests use getAllByTitle rather than a role query).
    const viewButtons = screen.getAllByTitle('View full value');
    expect(viewButtons.length).toBeGreaterThanOrEqual(2);

    fireEvent.click(viewButtons[0]!);
    expect(await screen.findByText(/^Full value — /)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Copy' }));
    expect(navigator.clipboard.writeText).toHaveBeenCalled();
  });

  it('renders the Parameters tab as a mini-tree: metadata flat, sql grouped, binds nested', async () => {
    const call = callWithSqlBinds(Date.now());
    renderTreePage(
      `/tree/${encodeURIComponent(pkToPath(call.pk))}?ts_ms=${call.ts_ms}&retention_class=${call.retention_class}`,
    );
    await screen.findByText(/CoyoteAdapter\.service/, undefined, { timeout: 5000 });
    fireEvent.click(screen.getByRole('tab', { name: 'Parameters' }));

    // request.id is an unconditional root metadata param (mocks/synthetic.ts) — a
    // flat row. findAllByText because the Call Tree tab's own params render the
    // same tag while its pane is merely hidden, not unmounted.
    expect((await screen.findAllByText('request.id')).length).toBeGreaterThan(0);
    // At least one sql shape row is present, and its binds start collapsed.
    expect((await screen.findAllByText('sql')).length).toBeGreaterThan(0);
    expect(screen.queryByText('binds')).toBeNull();

    // Every expand toggle in this table belongs to an sql row that carries
    // binds (childless rows never get one — paramSummaryRows only sets
    // `children` when a shape has binds), so the first one suffices.
    const expandButtons = await screen.findAllByRole('button', { name: 'Expand row' });
    expect(expandButtons.length).toBeGreaterThan(0);
    fireEvent.click(expandButtons[0]!);
    expect(await screen.findByText('binds')).toBeInTheDocument();
  });
});
