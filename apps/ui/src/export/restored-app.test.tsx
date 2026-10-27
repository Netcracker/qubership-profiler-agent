import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { encodeTree } from '../msgpack/encode';
import type { TreeNodeWire, TreeWire } from '../msgpack/tree-wire';
import { getThemeMode, syncThemeMode } from '../ui/theme-mode';
import { bytesToBase64, RESTORE_VERSION } from './restore';
import type { RestorePayload } from './restore';
import { RestoredApp } from './restored-app';

const node = (methodIdx: number, durationMs: number, selfDurationMs: number, extra: Partial<TreeNodeWire> = {}): TreeNodeWire => ({
  methodIdx,
  durationMs,
  selfDurationMs,
  suspensionMs: 0,
  selfSuspensionMs: 0,
  executions: 1,
  selfExecutions: 1,
  ...extra,
});

const WIRE: TreeWire = {
  v: 1,
  methods: [
    'void com.acme.billing.InvoiceService.handle(Request) (InvoiceService.java:10) [app.jar]',
    'ResultSet com.acme.billing.InvoiceDao.select(long) (InvoiceDao.java:20) [app.jar]',
  ],
  params: ['request.id'],
  root: node(0, 10, 4, {
    executions: 2,
    params: [{ paramIdx: 0, groups: [{ value: 'req-42', durationMs: 10, executions: 1 }] }],
    children: [node(1, 6, 6)],
  }),
};

function payload(overrides: Partial<RestorePayload> = {}): RestorePayload {
  return {
    v: RESTORE_VERSION,
    pkPath: 'ns:svc:pod-1:1:2:3:0',
    tsMs: 1,
    retentionClass: 'long_clean',
    treeB64: bytesToBase64(encodeTree(WIRE)),
    adjustText: '',
    categoryText: '',
    tabs: [],
    activeTab: 'tree',
    ...overrides,
  };
}

afterEach(() => {
  cleanup();
  localStorage.clear();
  syncThemeMode(); // the theme store is a module singleton; return it to the default
});

describe('RestoredApp', () => {
  it('renders the embedded tree offline, with no download button', () => {
    render(<RestoredApp payload={payload()} />);
    // The header marks it as an exported copy and drops the export action.
    expect(screen.getByText('offline copy')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Download HTML/ })).toBeNull();
    expect(screen.queryByRole('link', { name: 'Back to Calls' })).toBeNull();
    // Adjust/Setup still work offline (pure transforms on the embedded wire).
    expect(screen.getByRole('button', { name: /Adjust duration/ })).toBeEnabled();
    // The embedded tree renders without any fetch.
    expect(screen.getByText(/InvoiceService\.handle/)).toBeInTheDocument();
  });

  it.each(['light', 'dark'] as const)('renders all four tabs in the exported %s theme', (theme) => {
    // The reader's browser stored the other theme; the copy still opens as it was saved.
    const other = theme === 'dark' ? 'light' : 'dark';
    localStorage.setItem('profiler.theme', other);
    syncThemeMode();

    render(<RestoredApp payload={payload({ theme })} />);
    expect(document.documentElement.dataset['theme']).toBe(theme);
    expect(getThemeMode()).toBe(theme);
    // Opening a file must not rewrite what the reader chose elsewhere.
    expect(localStorage.getItem('profiler.theme')).toBe(other);

    expect(screen.getAllByRole('tab').map((tab) => tab.textContent)).toEqual([
      'Call Tree',
      'Flame graph',
      'Hotspots',
      'Parameters',
    ]);
    // Call Tree.
    expect(screen.getByTitle(WIRE.methods[1]!)).toBeInTheDocument();
    // Flame graph.
    fireEvent.click(screen.getByRole('tab', { name: 'Flame graph' }));
    expect(screen.getByRole('button', { name: /^InvoiceDao\.select · 6ms · 60% of the call/ })).toBeInTheDocument();
    // Hotspots: both methods have self time, so both are hotspots.
    fireEvent.click(screen.getByRole('tab', { name: 'Hotspots' }));
    expect(screen.getAllByTitle(WIRE.methods[1]!).length).toBeGreaterThan(1);
    // Parameters.
    fireEvent.click(screen.getByRole('tab', { name: 'Parameters' }));
    expect(screen.getAllByText('req-42').length).toBeGreaterThan(0);
  });

  it('reopens on the tab that was active at export time', () => {
    render(<RestoredApp payload={payload({ activeTab: 'flame' })} />);
    expect(screen.getByRole('tab', { name: 'Flame graph' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('button', { name: /^InvoiceService\.handle · / })).toBeInTheDocument();
  });

  it('follows the browser when an older export names no theme', () => {
    localStorage.setItem('profiler.theme', 'dark');
    syncThemeMode();
    render(<RestoredApp payload={payload()} />);
    expect(document.documentElement.dataset['theme']).toBe('dark');
  });
});
