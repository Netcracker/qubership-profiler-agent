import { describe, expect, it } from 'vitest';

import {
  DEFAULT_DURATION_MIN_MS,
  EMPTY_CALLS_SEARCH,
  callsSearchToParams,
  parseCallsSearch,
  parseTreeSearch,
  treeHref,
} from './search-params';
import type { CallsSearchState } from './search-params';

describe('calls search state', () => {
  it('round-trips a fully populated state', () => {
    const state: CallsSearchState = {
      fromMs: 1714060800000,
      toMs: 1714064400000,
      services: ['payments/billing', 'orders/checkout'],
      pods: ['payments/billing/billing-7f8c'],
      durationMinMs: 3000,
      errorOnly: true,
      retentionClasses: ['long_clean', 'any_error'],
      query: 'Service.handle',
      hideSystem: false,
    };
    expect(parseCallsSearch(callsSearchToParams(state))).toEqual(state);
  });

  it('defaults: empty params parse to the default state', () => {
    expect(parseCallsSearch(new URLSearchParams())).toEqual(EMPTY_CALLS_SEARCH);
  });

  it('keeps default URLs clean', () => {
    expect(callsSearchToParams(EMPTY_CALLS_SEARCH).toString()).toBe('');
  });

  it('applies the >500ms default only when duration_min_ms is absent', () => {
    expect(parseCallsSearch(new URLSearchParams()).durationMinMs).toBe(DEFAULT_DURATION_MIN_MS);
    expect(parseCallsSearch(new URLSearchParams('duration_min_ms=0')).durationMinMs).toBe(0);
  });

  it('drops malformed values instead of propagating them', () => {
    const sp = new URLSearchParams(
      'from=abc&to=-5&service=no-slash&pod=only/two&retention_class=bogus&duration_min_ms=x',
    );
    expect(parseCallsSearch(sp)).toEqual(EMPTY_CALLS_SEARCH);
  });
});

describe('tree search state', () => {
  it('parses hints and builds the row href', () => {
    const href = treeHref('ns:svc:pod-1:1714060800000:5:12340:0', 1714060812345, 'long_clean');
    const [path, query] = href.split('?');
    expect(path).toBe(`/ui/tree/${encodeURIComponent('ns:svc:pod-1:1714060800000:5:12340:0')}`);
    const hints = parseTreeSearch(new URLSearchParams(query));
    expect(hints).toEqual({ tsMs: 1714060812345, retentionClass: 'long_clean' });
  });

  it('treats missing or unknown hints as absent', () => {
    expect(parseTreeSearch(new URLSearchParams('retention_class=nope'))).toEqual({
      tsMs: null,
      retentionClass: null,
    });
  });
});
