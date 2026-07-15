import { describe, expect, it } from 'vitest';

import {
  DEFAULT_DURATION_MIN_MS,
  EMPTY_CALLS_SEARCH,
  callsSearchToParams,
  freezeWindow,
  hasRelativeWindow,
  parseCallsSearch,
  parseTreeSearch,
  treeHref,
} from './search-params';
import type { CallsSearchState } from './search-params';

describe('calls search state', () => {
  it('round-trips a fully populated state (absolute ms window)', () => {
    const state: CallsSearchState = {
      from: '1714060800000',
      to: '1714064400000',
      fromMs: 1714060800000,
      toMs: 1714064400000,
      services: ['payments/billing', 'orders/checkout'],
      pods: ['payments/billing/billing-7f8c'],
      durationMinMs: 3000,
      durationMaxMs: 8000,
      errorOnly: true,
      retentionClasses: ['long_clean', 'any_error'],
      query: 'Service.handle',
      hideSystem: false,
    };
    expect(parseCallsSearch(callsSearchToParams(state))).toEqual(state);
  });

  it('keeps a relative window relative and resolves it against the given now', () => {
    const now = 1714064400000;
    const parsed = parseCallsSearch(new URLSearchParams('from=now-3h&to=now'), now);
    expect(parsed.from).toBe('now-3h');
    expect(parsed.to).toBe('now');
    expect(parsed.toMs).toBe(now);
    expect(parsed.fromMs).toBe(now - 3 * 60 * 60 * 1000);
    // The URL keeps the tokens verbatim, so the range stays live on the next load.
    expect(callsSearchToParams(parsed).toString()).toBe('from=now-3h&to=now');
  });

  it('defaults: empty params parse to the default state', () => {
    expect(parseCallsSearch(new URLSearchParams())).toEqual(EMPTY_CALLS_SEARCH);
  });

  it('keeps default URLs clean', () => {
    expect(callsSearchToParams(EMPTY_CALLS_SEARCH).toString()).toBe('');
  });

  it('fills an absent window with the last hour only when defaultWindow is set', () => {
    const now = 1714064400000;
    const off = parseCallsSearch(new URLSearchParams(), now);
    expect([off.from, off.to, off.fromMs, off.toMs]).toEqual([null, null, null, null]);

    const on = parseCallsSearch(new URLSearchParams(), now, { defaultWindow: true });
    expect(on.from).toBe('now-1h');
    expect(on.to).toBe('now');
    expect(on.fromMs).toBe(now - 60 * 60 * 1000);
    expect(on.toMs).toBe(now);
  });

  it('prefers a URL window over the default and drops a malformed one to the default', () => {
    const now = 1714064400000;
    const explicit = parseCallsSearch(new URLSearchParams('from=now-3h&to=now'), now, { defaultWindow: true });
    expect([explicit.from, explicit.to]).toEqual(['now-3h', 'now']);

    const malformed = parseCallsSearch(new URLSearchParams('from=abc&to='), now, { defaultWindow: true });
    expect([malformed.from, malformed.to]).toEqual(['now-1h', 'now']);
  });

  it('applies the >500ms default only when duration_min_ms is absent', () => {
    expect(parseCallsSearch(new URLSearchParams()).durationMinMs).toBe(DEFAULT_DURATION_MIN_MS);
    expect(parseCallsSearch(new URLSearchParams('duration_min_ms=0')).durationMinMs).toBe(0);
  });

  it('freezes a live relative window into an absolute permalink', () => {
    const now = 1714064400000;
    const live = parseCallsSearch(new URLSearchParams('from=now-3h&to=now'), now);
    expect(hasRelativeWindow(live)).toBe(true);

    const pinned = freezeWindow(live);
    expect(pinned.from).toBe(String(now - 3 * 60 * 60 * 1000));
    expect(pinned.to).toBe(String(now));
    expect(hasRelativeWindow(pinned)).toBe(false);
  });

  it('leaves an already-absolute window unchanged', () => {
    const abs = parseCallsSearch(new URLSearchParams('from=1000&to=2000'), Date.now());
    expect(hasRelativeWindow(abs)).toBe(false);
    expect(freezeWindow(abs)).toEqual(abs);
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
    // The href hangs off the build-time base (default '/'); assert against it.
    expect(path).toBe(`${import.meta.env.BASE_URL}tree/${encodeURIComponent('ns:svc:pod-1:1714060800000:5:12340:0')}`);
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
