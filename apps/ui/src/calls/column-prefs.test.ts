import { afterEach, describe, expect, it } from 'vitest';

import { defaultColumnPrefs, loadColumnPrefs, saveColumnPrefs } from './column-prefs';
import { DEFAULT_COLUMN_ORDER, DEFAULT_HIDDEN_COLUMNS } from './columns';

const V1 = 'profiler-ui.calls.columns.v1';
const V2 = 'profiler-ui.calls.columns.v2';

// Every column of the version 1 table, in its default order.
const V1_ORDER = ['start', 'duration', 'pod', 'title', 'cpu', 'suspend', 'queue', 'calls', 'transactions', 'diskIo', 'netIo', 'memory'];

afterEach(() => localStorage.clear());

describe('defaultColumnPrefs', () => {
  it('shows the nine columns of the mockup and hides the other metrics', () => {
    const prefs = defaultColumnPrefs();
    expect(prefs.order.filter((k) => !prefs.hidden.includes(k))).toEqual([
      'start',
      'duration',
      'pod',
      'title',
      'trace_id',
      'cpu',
      'queue',
      'calls',
      'net_io',
    ]);
    expect(prefs.hidden).toEqual(['suspend', 'transactions', 'disk_io', 'memory']);
    expect(prefs.widths).toEqual({});
  });
});

describe('loadColumnPrefs', () => {
  it('returns the defaults when nothing is stored', () => {
    expect(loadColumnPrefs()).toEqual(defaultColumnPrefs());
  });

  it('migrates a version 1 entry: renamed keys, the user order, and trace_id after Title', () => {
    localStorage.setItem(
      V1,
      JSON.stringify({
        order: ['netIo', ...V1_ORDER.filter((k) => k !== 'netIo')],
        hidden: ['diskIo', 'memory'],
        widths: { netIo: 140, diskIo: 90, title: 500 },
      }),
    );
    const prefs = loadColumnPrefs();
    expect(prefs.order).toEqual([
      'net_io',
      'start',
      'duration',
      'pod',
      'title',
      'trace_id',
      'cpu',
      'suspend',
      'queue',
      'calls',
      'transactions',
      'disk_io',
      'memory',
    ]);
    // The user saw every version 1 column, so only the ones they hid stay hidden.
    expect(prefs.hidden).toEqual(['disk_io', 'memory']);
    expect(prefs.widths).toEqual({ net_io: 140, disk_io: 90, title: 500 });
  });

  it('drops keys the table no longer has, duplicates, and bad widths', () => {
    localStorage.setItem(
      V1,
      JSON.stringify({
        order: ['gone', ...V1_ORDER, 'start', 7],
        hidden: ['gone', 'suspend'],
        widths: { gone: 100, cpu: 10, calls: 'wide', queue: 120 },
      }),
    );
    const prefs = loadColumnPrefs();
    expect(prefs.order).not.toContain('gone');
    expect(new Set(prefs.order).size).toBe(prefs.order.length);
    expect([...prefs.order].sort()).toEqual([...DEFAULT_COLUMN_ORDER].sort());
    expect(prefs.hidden).toEqual(['suspend']);
    expect(prefs.widths).toEqual({ queue: 120 });
  });

  it('prefers a version 2 entry and does not rename its keys', () => {
    localStorage.setItem(V1, JSON.stringify({ order: V1_ORDER, hidden: ['netIo'], widths: {} }));
    localStorage.setItem(V2, JSON.stringify({ order: [...DEFAULT_COLUMN_ORDER], hidden: ['netIo', 'cpu'], widths: {} }));
    const prefs = loadColumnPrefs();
    expect(prefs.order).toEqual([...DEFAULT_COLUMN_ORDER]);
    expect(prefs.hidden).toEqual(['cpu']);
  });

  it('adds a column the stored entry lacks with its default visibility', () => {
    const stored = DEFAULT_COLUMN_ORDER.filter((k) => k !== 'trace_id' && k !== 'memory');
    localStorage.setItem(V2, JSON.stringify({ order: stored, hidden: [], widths: {} }));
    const prefs = loadColumnPrefs();
    expect(prefs.order).toEqual([...DEFAULT_COLUMN_ORDER]);
    expect(prefs.hidden).toEqual(DEFAULT_HIDDEN_COLUMNS.filter((k) => k === 'memory'));
  });

  it('reads an entry with missing or malformed lists and widths as the defaults plus what it has', () => {
    localStorage.setItem(V2, JSON.stringify({ hidden: ['cpu'], widths: null }));
    expect(loadColumnPrefs()).toEqual({ order: [...DEFAULT_COLUMN_ORDER], hidden: ['cpu', ...DEFAULT_HIDDEN_COLUMNS], widths: {} });

    localStorage.setItem(V2, JSON.stringify({ order: 'start', hidden: {}, widths: [] }));
    expect(loadColumnPrefs()).toEqual(defaultColumnPrefs());
  });

  it('falls back to the defaults on unparseable storage', () => {
    localStorage.setItem(V2, '{not json');
    expect(loadColumnPrefs()).toEqual(defaultColumnPrefs());
  });
});

describe('saveColumnPrefs', () => {
  it('writes version 2 and removes the version 1 entry', () => {
    localStorage.setItem(V1, JSON.stringify({ order: V1_ORDER, hidden: [], widths: {} }));
    const prefs = { ...defaultColumnPrefs(), hidden: ['cpu'] };
    saveColumnPrefs(prefs);
    expect(localStorage.getItem(V1)).toBeNull();
    expect(JSON.parse(localStorage.getItem(V2)!)).toEqual(prefs);
    expect(loadColumnPrefs()).toEqual(prefs);
  });
});
