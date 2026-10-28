import { renderHook, waitFor } from '@testing-library/react';
import { HttpResponse, http } from 'msw';
import { act } from 'react';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import * as endpoints from '../api/endpoints';
import { server } from '../mocks/node';
import { useStats } from './use-stats';

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const NOW = Date.now();
const QUERY = { fromMs: NOW - 10 * 60 * 1000, toMs: NOW, groupBy: 'time', buckets: 10 };

const problem = (status: number, code: string) =>
  HttpResponse.json({ type: 'about:blank', title: code, status, code }, { status, headers: { 'Content-Type': 'application/problem+json' } });

describe('useStats', () => {
  it('stays idle without a query', () => {
    const { result } = renderHook(() => useStats(null));
    expect(result.current.state.kind).toBe('idle');
  });

  it('loads the grouping', async () => {
    const { result } = renderHook(() => useStats(QUERY));
    await waitFor(() => expect(result.current.state.kind).toBe('ready'));
    const state = result.current.state;
    if (state.kind !== 'ready') throw new Error('unreachable');
    expect(state.data.rows).toHaveLength(10);
  });

  it('hides the chart on a backend without /stats and on a too-wide query', async () => {
    server.use(http.get('/api/v1/stats', () => problem(404, 'not_found')));
    const { result } = renderHook(() => useStats(QUERY));
    await waitFor(() => expect(result.current.state.kind).toBe('hidden'));

    server.use(http.get('/api/v1/stats', () => problem(400, 'query_too_wide')));
    act(() => result.current.refetch());
    await waitFor(() => expect(result.current.state.kind).toBe('hidden'));
  });

  it('reports another failure, and a refetch recovers', async () => {
    server.use(http.get('/api/v1/stats', () => problem(503, 'read_budget_exhausted')));
    const { result } = renderHook(() => useStats(QUERY));
    await waitFor(() => expect(result.current.state.kind).toBe('error'));
    server.resetHandlers();
    act(() => result.current.refetch());
    await waitFor(() => expect(result.current.state.kind).toBe('ready'));
  });

  it('reports a failure that is not an Error by its text', async () => {
    vi.spyOn(endpoints, 'fetchStats').mockRejectedValueOnce('offline');
    try {
      const { result } = renderHook(() => useStats(QUERY));
      await waitFor(() => expect(result.current.state).toEqual({ kind: 'error', message: 'offline' }));
    } finally {
      vi.restoreAllMocks();
    }
  });
});
