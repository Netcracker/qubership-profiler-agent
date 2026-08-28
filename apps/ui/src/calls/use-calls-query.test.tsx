import { renderHook, waitFor } from '@testing-library/react';
import { HttpResponse, http } from 'msw';
import { act } from 'react';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { pkToPath } from '../api/pk';
import { server } from '../mocks/node';
import { MAX_EMPTY_PAGES, PAGE_LIMIT, useCallsQuery } from './use-calls-query';
import type { CallsReady } from './use-calls-query';

// MSW-driven behaviour of the calls pagination hook: keyset pages, the
// wide-query prompt, partial markers, cursor expiry, and bounded empty-page
// auto-follow (09 §5).

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const NOW = Date.now();
const FILTER = { fromMs: NOW - 15 * 60 * 1000, toMs: NOW, durationMinMs: 0 };

function ready(state: ReturnType<typeof useCallsQuery>['state']): CallsReady {
  if (state.kind !== 'ready') throw new Error(`expected ready, got ${state.kind}`);
  return state;
}

describe('useCallsQuery', () => {
  it('loads page one and appends the next page without duplicates', async () => {
    const { result } = renderHook(() => useCallsQuery(FILTER));
    await waitFor(() => expect(result.current.state.kind).toBe('ready'), { timeout: 5000 });

    const first = ready(result.current.state);
    expect(first.rows).toHaveLength(PAGE_LIMIT);
    expect(first.nextCursor).not.toBeNull();
    // The shared total order (ts_ms DESC, pk ASC) holds within the page.
    for (let i = 1; i < first.rows.length; i++) {
      expect(first.rows[i - 1]!.ts_ms).toBeGreaterThanOrEqual(first.rows[i]!.ts_ms);
    }

    act(() => result.current.loadMore());
    await waitFor(() => expect(ready(result.current.state).rows.length).toBeGreaterThan(PAGE_LIMIT), {
      timeout: 5000,
    });
    const second = ready(result.current.state);
    expect(second.rows).toHaveLength(2 * PAGE_LIMIT);
    expect(new Set(second.rows.map((c) => pkToPath(c.pk))).size).toBe(second.rows.length);
  });

  it('surfaces the wide-query rejection with its narrowing chips', async () => {
    const { result } = renderHook(() =>
      useCallsQuery({ fromMs: NOW - 7 * 60 * 60 * 1000, toMs: NOW, durationMinMs: 0 }),
    );
    await waitFor(() => expect(result.current.state.kind).toBe('too-wide'));
    const state = result.current.state;
    if (state.kind !== 'too-wide') throw new Error('unreachable');
    expect(state.problem.title).toBe('query too wide');
    expect(state.problem.suggested_filters).toEqual(['pod', 'retention_class', 'duration_min_ms', 'error_only']);
  });

  it('passes partial markers through to the ready state', async () => {
    server.use(
      http.get('/api/v1/calls', () =>
        HttpResponse.json({
          calls: [],
          next_cursor: null,
          partial: true,
          partial_reasons: ['replica 10.0.0.3: timeout after 2s'],
        }),
      ),
    );
    const { result } = renderHook(() => useCallsQuery(FILTER));
    await waitFor(() => expect(result.current.state.kind).toBe('ready'));
    const state = ready(result.current.state);
    expect(state.partial).toBe(true);
    expect(state.partialReasons).toEqual(['replica 10.0.0.3: timeout after 2s']);
  });

  it('keeps loaded rows and flags an expired cursor on load-more', async () => {
    const { result } = renderHook(() => useCallsQuery(FILTER));
    await waitFor(() => expect(result.current.state.kind).toBe('ready'), { timeout: 5000 });
    const loaded = ready(result.current.state).rows.length;

    server.use(
      http.get('/api/v1/calls', ({ request }) => {
        if (new URL(request.url).searchParams.get('cursor') === null) return undefined;
        return HttpResponse.json(
          {
            type: 'about:blank',
            title: 'invalid request',
            status: 400,
            detail: 'cursor expired: issued 1200s ago, TTL 15m0s',
          },
          { status: 400, headers: { 'Content-Type': 'application/problem+json' } },
        );
      }),
    );
    act(() => result.current.loadMore());
    await waitFor(() => expect(ready(result.current.state).cursorExpired).toBe(true));
    expect(ready(result.current.state).rows).toHaveLength(loaded);
  });

  it('follows empty pages a bounded number of rounds, then pauses', async () => {
    let requests = 0;
    server.use(
      http.get('/api/v1/calls', () => {
        requests += 1;
        return HttpResponse.json({
          calls: [],
          next_cursor: `cursor-${requests}`,
          partial: false,
          partial_reasons: [],
        });
      }),
    );
    const { result } = renderHook(() => useCallsQuery(FILTER));
    await waitFor(() => expect(result.current.state.kind).toBe('ready'));
    const state = ready(result.current.state);
    expect(state.emptyPaused).toBe(true);
    expect(state.rows).toHaveLength(0);
    expect(state.nextCursor).not.toBeNull();
    expect(requests).toBe(1 + MAX_EMPTY_PAGES);

    // Manual continue resumes for another bounded round.
    act(() => result.current.loadMore());
    await waitFor(() => expect(ready(result.current.state).loadingMore).toBe(false));
    expect(requests).toBe(1 + 2 * MAX_EMPTY_PAGES);
  });
});
