import { cleanup, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { CallJSON } from '../api/types';
import { CallsTable } from './calls-table';
import type { CallsTableProps } from './calls-table';
import { defaultColumnPrefs } from './column-prefs';

afterEach(cleanup);

const TRACE_ID = '4bf92f3577b34da6a3ce929d0e0e4736';

function call(recordIndex: number, params: Record<string, string[]>, durationMs = 250): CallJSON {
  return {
    pk: {
      pod_namespace: 'synthetic-ns',
      pod_service: 'orders',
      pod_name: `orders-7f9c-${recordIndex}`,
      restart_time_ms: 1_700_000_000_000,
      trace_file_index: 0,
      buffer_offset: recordIndex * 100,
      record_index: recordIndex,
    },
    ts_ms: 1_700_000_100_000 + recordIndex,
    duration_ms: durationMs,
    method: 'void com.example.OrderService.place()',
    thread_name: 'http-nio-8080-exec-1',
    cpu_time_ms: 40,
    wait_time_ms: 0,
    memory_used: 2048,
    queue_wait_ms: 3,
    suspend_ms: 0,
    child_calls: 12,
    transactions: 0,
    logs_generated: 0,
    logs_written: 0,
    file_read: 0,
    file_written: 0,
    net_read: 1024,
    net_written: 512,
    error_flag: false,
    retention_class: 'normal_clean',
    params,
    trace_blob_size: null,
    truncated_reason: null,
  };
}

const TRACED = call(1, { 'trace.id': [TRACE_ID] });
const UNTRACED = call(2, {});

function renderTable(props: Partial<CallsTableProps> = {}) {
  return render(
    <CallsTable
      rows={[TRACED, UNTRACED]}
      loading={false}
      prefs={defaultColumnPrefs()}
      onPrefsChange={vi.fn()}
      virtual={false}
      {...props}
    />,
  );
}

function rowOf(c: CallJSON): HTMLElement {
  return screen.getByText(c.pk.pod_name).closest('tr')!;
}

describe('CallsTable', () => {
  it('shows the default columns with the column chooser at the end of the header', () => {
    renderTable();
    const headers = screen.getAllByRole('columnheader').map((th) => th.textContent);
    expect(headers.slice(0, 9)).toEqual(['Start', 'Duration', 'Pod', 'Title', 'trace_id', 'CPU', 'Queue', 'Calls', 'Net IO']);
    expect(screen.getByRole('button', { name: 'Choose columns' })).toBeInTheDocument();
  });

  it('calls onPickFilter with the pod name and the full trace ID', async () => {
    const user = userEvent.setup();
    const onPickFilter = vi.fn();
    renderTable({ onPickFilter });

    await user.click(screen.getByRole('button', { name: TRACED.pk.pod_name }));
    expect(onPickFilter).toHaveBeenLastCalledWith('pod', TRACED.pk.pod_name);

    const trace = within(rowOf(TRACED)).getByTitle(`Filter by trace_id ${TRACE_ID}`);
    expect(trace.textContent).toBe(`${TRACE_ID.slice(0, 12)}…`);
    await user.click(trace);
    expect(onPickFilter).toHaveBeenLastCalledWith('trace_id', TRACE_ID);
    expect(onPickFilter).toHaveBeenCalledTimes(2);
  });

  it('renders the pod and trace ID as plain text without onPickFilter', () => {
    renderTable();
    const row = rowOf(TRACED);
    expect(within(row).queryByRole('button', { name: TRACED.pk.pod_name })).toBeNull();
    expect(within(row).queryByTitle(/^Filter by/)).toBeNull();
    expect(within(row).getByTitle(TRACE_ID).textContent).toBe(`${TRACE_ID.slice(0, 12)}…`);
  });

  it('marks a call without a trace ID as not traced, with no filter shortcut', () => {
    renderTable({ onPickFilter: vi.fn() });
    const cell = within(rowOf(UNTRACED)).getByText('not traced');
    expect(cell).toHaveAttribute('title', expect.stringMatching(/no trace ID/));
    expect(within(rowOf(UNTRACED)).queryByTitle(/^Filter by trace_id/)).toBeNull();
  });

  it('reads the trace ID from the Brave param as well', () => {
    const brave = call(3, { 'brave.trace_id': ['a1b2c3d4e5f60718'] });
    renderTable({ rows: [brave] });
    expect(within(rowOf(brave)).getByTitle('a1b2c3d4e5f60718')).toBeInTheDocument();
  });

  it('links the duration to the call tree next to the heat dot', () => {
    const slow = call(4, {}, 3000);
    renderTable({ rows: [slow] });
    const link = within(rowOf(slow)).getByRole('link', { name: '3.0s' });
    expect(link).toHaveAttribute('href', expect.stringContaining('/tree/'));
    expect(link.previousElementSibling).toHaveStyle({ background: 'var(--pf-color-crit)' });
  });

  it('notes in the footer that a column sort covers the loaded rows only', async () => {
    const user = userEvent.setup();
    renderTable({ footer: <span>2 loaded</span> });
    expect(screen.queryByText('Column sort applies to the loaded rows only')).toBeNull();
    await user.click(screen.getAllByRole('columnheader').find((th) => th.textContent === 'CPU')!);
    expect(screen.getByText('2 loaded')).toBeInTheDocument();
    expect(screen.getByText('Column sort applies to the loaded rows only')).toBeInTheDocument();
  });
});
