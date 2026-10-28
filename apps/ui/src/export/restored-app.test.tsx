import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { encodeTree } from '../msgpack/encode';
import type { TreeWire } from '../msgpack/tree-wire';
import { bytesToBase64, RESTORE_VERSION } from './restore';
import type { RestorePayload } from './restore';
import { getThemeMode, setThemePreference, syncThemeMode } from '../ui/theme-mode';
import { RestoredApp } from './restored-app';

const WIRE: TreeWire = {
  v: 1,
  methods: ['com.acme.billing.InvoiceService.handle(Request)'],
  params: [],
  root: {
    methodIdx: 0,
    durationMs: 10,
    selfDurationMs: 10,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 1,
    selfExecutions: 1,
  },
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

describe('RestoredApp', () => {
  afterEach(() => {
    cleanup();
    localStorage.clear();
    syncThemeMode(); // the store is a module singleton; return it to the default
  });

  it('renders the embedded tree offline, with no download button', () => {
    render(<RestoredApp payload={payload()} />);
    // The header marks it as an exported copy and drops the export action.
    expect(screen.getByText('offline copy')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Download HTML/ })).toBeNull();
    // Adjust/Setup still work offline (pure transforms on the embedded wire).
    expect(screen.getByRole('button', { name: 'Adjust duration' })).toBeEnabled();
    // The embedded tree renders without any fetch.
    expect(screen.getByText(/InvoiceService\.handle/)).toBeInTheDocument();
  });

  it('opens in the saved theme, whatever the reader stored', () => {
    setThemePreference('light');
    render(<RestoredApp payload={payload({ theme: 'dark' })} />);
    expect(getThemeMode()).toBe('dark');
    expect(document.documentElement.dataset['theme']).toBe('dark');
    // Pinned for this page load only: the reader's stored choice is untouched.
    expect(localStorage.getItem('profiler.theme')).toBe('light');
  });

  it("opens an export without a saved theme in the reader's theme", () => {
    setThemePreference('dark');
    render(<RestoredApp payload={payload()} />);
    expect(screen.getByText('offline copy')).toBeInTheDocument();
    expect(getThemeMode()).toBe('dark');
  });
});
