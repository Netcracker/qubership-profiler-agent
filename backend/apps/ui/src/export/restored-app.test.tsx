import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { encodeTree } from '../msgpack/encode';
import type { TreeWire } from '../msgpack/tree-wire';
import { bytesToBase64, RESTORE_VERSION } from './restore';
import type { RestorePayload } from './restore';
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

function payload(): RestorePayload {
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
  };
}

describe('RestoredApp', () => {
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
});
