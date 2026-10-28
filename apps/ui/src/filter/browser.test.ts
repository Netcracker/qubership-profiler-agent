import { describe, expect, it } from 'vitest';

import type { CallJSON } from '../api/types';
import { BROWSER_TRACE_NOTE, browserNote, browserSupports, filterInBrowser, isBrowserCondition } from './browser';
import type { Condition } from './condition';

function call(id: number, traceId: string | null): CallJSON {
  return { pk: { record_index: id }, params: traceId === null ? {} : { 'trace.id': [traceId] } } as unknown as CallJSON;
}

const trace = (id: string): Condition => ({ field: 'trace_id', op: '=', values: [id] });

describe('browser-side conditions', () => {
  it('applies trace_id = <id> in the browser and nothing else', () => {
    expect(isBrowserCondition(trace('abc'))).toBe(true);
    expect(isBrowserCondition({ field: 'trace_id', op: '!=', values: ['abc'] })).toBe(false);
    expect(isBrowserCondition({ field: 'request_id', op: '=', values: ['abc'] })).toBe(false);
    expect(browserNote(trace('abc'))).toBe(BROWSER_TRACE_NOTE);
    expect(browserNote({ field: 'pod', op: '=', values: ['a'] })).toBeNull();
  });

  it('lets the bar offer trace_id with = only', () => {
    expect(browserSupports('trace_id')).toBe(true);
    expect(browserSupports('trace_id', '=')).toBe(true);
    expect(browserSupports('trace_id', '=~')).toBe(false);
    expect(browserSupports('request_id', '=')).toBe(false);
  });

  it('keeps the rows of the trace and drops untraced ones', () => {
    const rows = [call(1, 'aaa'), call(2, 'bbb'), call(3, null), call(4, 'aaa')];
    expect(filterInBrowser(rows, [trace('aaa')]).map((c) => c.pk.record_index)).toEqual([1, 4]);
    expect(filterInBrowser(rows, [])).toBe(rows);
    expect(filterInBrowser(rows, [trace('aaa'), trace('bbb')])).toEqual([]);
  });

  it('keeps the untraced rows for None and the traced ones for Any', () => {
    const rows = [call(1, 'aaa'), call(2, null), call(3, 'bbb')];
    const ids = (values: string[]) => filterInBrowser(rows, values.map(trace)).map((c) => c.pk.record_index);
    expect(ids(['None'])).toEqual([2]);
    expect(ids(['Any'])).toEqual([1, 3]);
    expect(ids(['Any', 'bbb'])).toEqual([3]);
    expect(ids(['None', 'Any'])).toEqual([]);
  });
});
