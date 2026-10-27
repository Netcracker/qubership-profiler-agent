import { describe, expect, it } from 'vitest';

import { CALLS_ORDERS } from '../calls/sort-order';
import { formatCondition } from '../filter/grammar';
import {
  DEFAULT_CONDITIONS,
  TREE_TABS,
  EMPTY_CALLS_SEARCH,
  callsSearchToParams,
  freezeWindow,
  hasRelativeWindow,
  parseCallsSearch,
  parseTreeSearch,
  treeHref,
  withTreeTab,
} from './search-params';
import type { CallsSearchState } from './search-params';

describe('calls search state', () => {
  it('round-trips a fully populated state (absolute ms window)', () => {
    const state: CallsSearchState = {
      from: '1714060800000',
      to: '1714064400000',
      fromMs: 1714060800000,
      toMs: 1714064400000,
      conditions: [
        { field: 'namespace', op: '=', values: ['payments'] },
        { field: 'service', op: 'in', values: ['billing', 'ledger'] },
        { field: 'duration', op: 'between', values: ['3s..8s'] },
        { field: 'error', op: '=', values: ['true'] },
        { field: 'method', op: '=~', values: ['Service\\.handle'] },
      ],
      hideSystem: true,
      order: 'start_desc',
    };
    expect(parseCallsSearch(callsSearchToParams(state))).toEqual(state);
  });

  it('writes the state in full with repeatable filter parameters', () => {
    const state: CallsSearchState = {
      ...EMPTY_CALLS_SEARCH,
      from: 'now-1h',
      to: 'now',
      conditions: [
        { field: 'duration', op: '>', values: ['100ms'] },
        { field: 'namespace', op: '=', values: ['prod-orders'] },
      ],
      hideSystem: true,
    };
    expect(decodeURIComponent(callsSearchToParams(state).toString())).toBe(
      'from=now-1h&to=now&filter=duration>100ms&filter=namespace=prod-orders&order=start_desc&hide_system=1',
    );
  });

  it('keeps a relative window relative and resolves it against the given now', () => {
    const now = 1714064400000;
    const parsed = parseCallsSearch(new URLSearchParams('from=now-3h&to=now'), now);
    expect(parsed.from).toBe('now-3h');
    expect(parsed.to).toBe('now');
    expect(parsed.toMs).toBe(now);
    expect(parsed.fromMs).toBe(now - 3 * 60 * 60 * 1000);
    // The URL keeps the tokens verbatim, so the range stays live on the next load.
    expect(callsSearchToParams(parsed).toString()).toBe('from=now-3h&to=now&order=start_desc');
  });

  it.each(CALLS_ORDERS)('round-trips order=%s', (order) => {
    const sp = callsSearchToParams({ ...EMPTY_CALLS_SEARCH, order });
    expect(sp.get('order')).toBe(order);
    expect(parseCallsSearch(sp).order).toBe(order);
  });

  it('reads an absent or unknown order as start time, newest first', () => {
    expect(parseCallsSearch(new URLSearchParams('from=now-1h&to=now')).order).toBe('start_desc');
    expect(parseCallsSearch(new URLSearchParams('from=now-1h&to=now&order=pod_desc')).order).toBe('start_desc');
    expect(parseCallsSearch(new URLSearchParams('from=now-1h&to=now&order=duration')).order).toBe('start_desc');
  });

  it('parses empty params to the empty state without the defaults option', () => {
    expect(parseCallsSearch(new URLSearchParams())).toEqual(EMPTY_CALLS_SEARCH);
  });

  it('applies the defaults only to a visit with no query string', () => {
    const now = 1714064400000;
    const bare = parseCallsSearch(new URLSearchParams(), now, { defaults: true });
    expect(bare.from).toBe('now-1h');
    expect(bare.to).toBe('now');
    expect(bare.fromMs).toBe(now - 60 * 60 * 1000);
    expect(bare.toMs).toBe(now);
    expect(bare.conditions).toEqual(DEFAULT_CONDITIONS);
    expect(bare.hideSystem).toBe(true);

    // A query string, even one without filters, means the user chose no filters and no hiding.
    const chosen = parseCallsSearch(new URLSearchParams('from=now-3h&to=now&order=start_desc'), now, { defaults: true });
    expect(chosen.conditions).toEqual([]);
    expect(chosen.hideSystem).toBe(false);
  });

  it('prefers a URL window over the default and drops a malformed one to the default', () => {
    const now = 1714064400000;
    const explicit = parseCallsSearch(new URLSearchParams('from=now-3h&to=now'), now, { defaults: true });
    expect([explicit.from, explicit.to]).toEqual(['now-3h', 'now']);

    const malformed = parseCallsSearch(new URLSearchParams('from=abc&to='), now, { defaults: true });
    expect([malformed.from, malformed.to]).toEqual(['now-1h', 'now']);
  });

  it('converts an old link and keeps its window, with the old page hiding system calls', () => {
    const parsed = parseCallsSearch(new URLSearchParams('from=1000&to=2000&service=payments/billing&error_only=true&q=Tax'));
    expect(parsed.conditions.map(formatCondition)).toEqual([
      'namespace=payments',
      'service=billing',
      'duration>=500ms',
      'error=true',
      'method=~Tax',
    ]);
    expect(parsed.hideSystem).toBe(true);
    expect(parseCallsSearch(new URLSearchParams('duration_min_ms=0&hide_system=false')).hideSystem).toBe(false);
    // The old parameters are never written back.
    expect(callsSearchToParams(parsed).has('service')).toBe(false);
  });

  it('ignores the old parameters when the link has filter parameters', () => {
    const parsed = parseCallsSearch(new URLSearchParams('filter=pod%3Dbilling-1&service=payments/billing'));
    expect(parsed.conditions).toEqual([{ field: 'pod', op: '=', values: ['billing-1'] }]);
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
    const sp = new URLSearchParams('from=abc&to=-5&filter=bogus&filter=duration%3E1s&hide_system=maybe');
    expect(parseCallsSearch(sp)).toEqual({
      ...EMPTY_CALLS_SEARCH,
      conditions: [{ field: 'duration', op: '>', values: ['1s'] }],
    });
  });
});

describe('tree search state', () => {
  it('parses hints and builds the row href', () => {
    const href = treeHref('ns:svc:pod-1:1714060800000:5:12340:0', 1714060812345, 'long_clean');
    const [path, query] = href.split('?');
    // The href hangs off the build-time base (default '/'); assert against it.
    expect(path).toBe(`${import.meta.env.BASE_URL}tree/${encodeURIComponent('ns:svc:pod-1:1714060800000:5:12340:0')}`);
    const hints = parseTreeSearch(new URLSearchParams(query));
    expect(hints).toEqual({ tsMs: 1714060812345, retentionClass: 'long_clean', tab: 'tree' });
  });

  it('treats missing or unknown hints as absent', () => {
    expect(parseTreeSearch(new URLSearchParams('retention_class=nope'))).toEqual({
      tsMs: null,
      retentionClass: null,
      tab: 'tree',
    });
  });

  it.each(TREE_TABS)('round-trips tab=%s through the href', (tab) => {
    const href = treeHref('ns:svc:pod-1:1714060800000:5:12340:0', 1714060812345, 'long_clean', tab);
    const query = new URLSearchParams(href.split('?')[1]);
    expect(parseTreeSearch(query).tab).toBe(tab);
    // The default tab stays out of the link.
    expect(query.has('tab')).toBe(tab !== 'tree');
  });

  it('falls back to Call Tree on an unknown tab', () => {
    expect(parseTreeSearch(new URLSearchParams('tab=flame')).tab).toBe('flame');
    expect(parseTreeSearch(new URLSearchParams('tab=gantt')).tab).toBe('tree');
    expect(parseTreeSearch(new URLSearchParams('tab=')).tab).toBe('tree');
    // A derived tab key is browser state; one that leaks into a link is ignored.
    expect(parseTreeSearch(new URLSearchParams('tab=z1')).tab).toBe('tree');
  });

  it('keeps the hints when the tab changes', () => {
    const sp = withTreeTab(new URLSearchParams('ts_ms=5&retention_class=long_clean'), 'params');
    expect(sp.toString()).toBe('ts_ms=5&retention_class=long_clean&tab=params');
    expect(withTreeTab(sp, 'tree').toString()).toBe('ts_ms=5&retention_class=long_clean');
  });
});
