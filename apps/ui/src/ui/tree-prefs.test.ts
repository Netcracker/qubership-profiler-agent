import { afterEach, describe, expect, it } from 'vitest';

import { DEFAULT_TREE_PREFS, getTreePrefs, setTreePref, syncTreePrefs } from './tree-prefs';

afterEach(() => {
  localStorage.clear();
  syncTreePrefs(); // the store is a module singleton; return it to the defaults
});

describe('tree display switches', () => {
  it('draws both kinds of lines by default', () => {
    expect(getTreePrefs()).toEqual({ treeLines: true, rowLines: true });
  });

  it('keeps a switch across a reload', () => {
    setTreePref('treeLines', false);
    expect(getTreePrefs()).toEqual({ treeLines: false, rowLines: true });
    // A reload re-reads the storage; the in-memory value plays no part.
    syncTreePrefs();
    expect(getTreePrefs()).toEqual({ treeLines: false, rowLines: true });

    setTreePref('rowLines', false);
    setTreePref('treeLines', true);
    syncTreePrefs();
    expect(getTreePrefs()).toEqual({ treeLines: true, rowLines: false });
  });

  it('falls back to the defaults on a stored value it cannot read', () => {
    localStorage.setItem('profiler.tree.prefs', '{not json');
    syncTreePrefs();
    expect(getTreePrefs()).toEqual(DEFAULT_TREE_PREFS);

    localStorage.setItem('profiler.tree.prefs', JSON.stringify({ treeLines: 'no', rowLines: false }));
    syncTreePrefs();
    expect(getTreePrefs()).toEqual({ treeLines: true, rowLines: false });

    localStorage.setItem('profiler.tree.prefs', JSON.stringify({ treeLines: false, rowLines: 1 }));
    syncTreePrefs();
    expect(getTreePrefs()).toEqual({ treeLines: false, rowLines: true });
  });

  it('writes nothing when a switch already holds the value', () => {
    const before = getTreePrefs();
    setTreePref('treeLines', true);
    expect(localStorage.getItem('profiler.tree.prefs')).toBeNull();
    // The same object, so a subscribed component does not re-render.
    expect(getTreePrefs()).toBe(before);
  });
});
