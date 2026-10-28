import { renderHook, waitFor } from '@testing-library/react';
import { HttpResponse, delay, http } from 'msw';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { capabilitiesOf } from '../../filter/capabilities';
import type { Condition } from '../../filter/condition';
import { MOCK_FILTER_CAPABILITIES } from '../../mocks/handlers';
import { server } from '../../mocks/node';
import { SUGGESTION_LIMIT, suggestionNote, useParamNames, useValueSuggestions } from './use-value-suggestions';
import type { ValueSuggestionsInput } from './use-value-suggestions';

beforeAll(() => server.listen({ onUnhandledFrame: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const NOW = Date.now();
const CAPS = capabilitiesOf(MOCK_FILTER_CAPABILITIES);
const BASE: ValueSuggestionsInput = { field: 'service', fromMs: NOW - 10 * 60 * 1000, toMs: NOW, conditions: [], caps: CAPS };

/** Records the /stats requests while a test runs. */
function recordStats(): URL[] {
  const urls: URL[] = [];
  server.events.on('request:start', ({ request }) => {
    if (request.url.includes('/api/v1/stats')) urls.push(new URL(request.url));
  });
  return urls;
}

describe('useValueSuggestions', () => {
  afterEach(() => server.events.removeAllListeners());

  it('returns null when /stats cannot group by the field', () => {
    const noService = capabilitiesOf({ ...MOCK_FILTER_CAPABILITIES, group_by: ['time'] });
    expect(renderHook(() => useValueSuggestions({ ...BASE, caps: noService })).result.current).toBeNull();
    expect(renderHook(() => useValueSuggestions({ ...BASE, caps: null })).result.current).toBeNull();
    expect(renderHook(() => useValueSuggestions({ ...BASE, field: null })).result.current).toBeNull();
  });

  it('loads the most frequent values with counts, under the other conditions', async () => {
    const urls = recordStats();
    const conditions: Condition[] = [
      { field: 'duration', op: '>', values: ['100ms'] },
      { field: 'service', op: '!=', values: ['billing'] },
    ];
    const { result } = renderHook(() => useValueSuggestions({ ...BASE, conditions }));
    expect(result.current?.kind).toBe('loading');
    await waitFor(() => expect(result.current?.kind).toBe('ready'));
    const state = result.current!;
    if (state.kind !== 'ready') throw new Error('unreachable');
    expect(state).toMatchObject({ approximate: false, partial: false });
    expect(state.values.length).toBeGreaterThan(0);
    expect(state.values.every((v) => typeof v.count === 'number')).toBe(true);
    const sp = urls[0]!.searchParams;
    expect(sp.get('group_by')).toBe('service');
    expect(sp.get('limit')).toBe(String(SUGGESTION_LIMIT));
    // A condition on the field itself would hide its other values.
    expect(sp.getAll('filter')).toEqual(['duration>100ms']);
  });

  it('sends the pod tuples of a selection, and waits while they wait for /pods', async () => {
    const urls = recordStats();
    const tuples = ['payments/billing/billing-1', 'orders/checkout/checkout-1'];
    const { result, rerender } = renderHook((props: ValueSuggestionsInput) => useValueSuggestions(props), {
      initialProps: { ...BASE, pods: null } as ValueSuggestionsInput,
    });
    expect(result.current).toBeNull();
    rerender({ ...BASE, pods: tuples });
    await waitFor(() => expect(result.current?.kind).toBe('ready'));
    expect(urls).toHaveLength(1);
    expect(urls[0]!.searchParams.getAll('pod')).toEqual(tuples);
  });

  it('sends one request after a burst of changes, for the latest field', async () => {
    const urls = recordStats();
    const { result, rerender } = renderHook((props: ValueSuggestionsInput) => useValueSuggestions(props), {
      initialProps: BASE,
    });
    rerender({ ...BASE, field: 'namespace' });
    rerender({ ...BASE, field: 'pod' });
    await waitFor(() => expect(result.current?.kind).toBe('ready'));
    expect(result.current?.field).toBe('pod');
    expect(urls.map((u) => u.searchParams.get('group_by'))).toEqual(['pod']);
  });

  it('drops the answer of a request a newer one replaced', async () => {
    let calls = 0;
    server.use(
      http.get('/api/v1/stats', async ({ request }) => {
        calls += 1;
        const field = new URL(request.url).searchParams.get('group_by')!;
        if (field === 'service') await delay(400);
        return HttpResponse.json({
          group_by: field,
          rows: [{ key: `${field}-value`, count: 1 }],
          other: null,
          distinct: 1,
          totals: { count: 1, errors: 0 },
          approximate: false,
          partial: false,
          partial_reasons: [],
        });
      }),
    );
    const { result, rerender } = renderHook((props: ValueSuggestionsInput) => useValueSuggestions(props), {
      initialProps: BASE,
    });
    await waitFor(() => expect(calls).toBe(1));
    rerender({ ...BASE, field: 'pod' });
    await waitFor(() => expect(result.current?.kind).toBe('ready'));
    await delay(500);
    const state = result.current!;
    if (state.kind !== 'ready') throw new Error('unreachable');
    expect(state.values).toEqual([{ value: 'pod-value', count: 1 }]);
  });

  it('fails softly, so the bar still takes a typed value', async () => {
    server.use(http.get('/api/v1/stats', () => HttpResponse.json({ status: 500 }, { status: 500 })));
    const { result } = renderHook(() => useValueSuggestions(BASE));
    await waitFor(() => expect(result.current?.kind).toBe('failed'));
  });
});

describe('suggestionNote', () => {
  it('says when values load, fail, or are only the top of more', () => {
    expect(suggestionNote(null, 'pod')).toBeNull();
    expect(suggestionNote({ kind: 'loading', field: 'pod' }, 'service')).toBeNull();
    expect(suggestionNote({ kind: 'loading', field: 'pod' }, 'pod')).toBe('Loading values…');
    expect(suggestionNote({ kind: 'failed', field: 'pod' }, 'pod')).toBe('Could not load values; enter your own.');
    const values = [{ value: 'a', count: 2 }];
    const exact = { kind: 'ready', field: 'pod', values, approximate: false, partial: false } as const;
    expect(suggestionNote({ ...exact, distinct: 1 }, 'pod')).toBeNull();
    expect(suggestionNote({ ...exact, distinct: 1200 }, 'pod')).toBe('The 1 most frequent of 1,200 values.');
  });

  it('qualifies estimated and incomplete counts, even with every value listed', () => {
    const values = [{ value: 'a', count: 2 }];
    const ready = { kind: 'ready', field: 'pod', values, distinct: 1, approximate: false, partial: false } as const;
    expect(suggestionNote({ ...ready, approximate: true }, 'pod')).toBe('The counts are estimates.');
    expect(suggestionNote({ ...ready, partial: true }, 'pod')).toBe('Some sources did not answer, so the counts may be low.');
    expect(suggestionNote({ ...ready, distinct: 10_000, approximate: true, partial: true }, 'pod')).toBe(
      'The 1 most frequent of at least 10,000 values. The counts are estimates. ' +
        'Some sources did not answer, so the counts may be low.',
    );
  });
});

describe('useParamNames', () => {
  it('lists the param names of the window', async () => {
    const { result } = renderHook(() => useParamNames(BASE.fromMs, BASE.toMs, CAPS));
    await waitFor(() => expect(result.current.length).toBeGreaterThan(0));
    expect(result.current).toContain('web.url');
  });

  it('lists none when the backend cannot group by param name', () => {
    const caps = capabilitiesOf({ ...MOCK_FILTER_CAPABILITIES, group_by: ['time'] });
    expect(renderHook(() => useParamNames(BASE.fromMs, BASE.toMs, caps)).result.current).toEqual([]);
  });

  it('lists none after a failure', async () => {
    server.use(http.get('/api/v1/stats', () => HttpResponse.json({ status: 500 }, { status: 500 })));
    const { result } = renderHook(() => useParamNames(BASE.fromMs, BASE.toMs, CAPS));
    await delay(100);
    expect(result.current).toEqual([]);
  });
});
