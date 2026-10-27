import { describe, expect, it } from 'vitest';

import type { TreeWire } from '../msgpack/tree-wire';
import { buildTreeModel } from './model';
import { searchTree } from './search';
import { buildRows, expandAll, expandLarge, guideCells, initialExpansion } from './visible-rows';
import type { TreeRow } from './visible-rows';

// Fixture (durations add up: 800 + 100 + 50 + self 120 = 1070):
//   root ── big(800) ── mid(800) ── leaf(800 self)   ← pass-through chain
//        ├─ small(100, self 20) ── smallChild(80)     ← ≤10% → starts collapsed
//        └─ tiny(50, self 10) ── tinyChild(40)        ← ≤10% → starts collapsed
// root carries one param with two groups (sql with a bind, ::other).
const WIRE: TreeWire = {
  v: 1,
  methods: [
    'void a.Root.run() (Root.java:1) [a.jar]',
    'void a.Big.pass() (Big.java:2) [a.jar]',
    'void a.Mid.pass() (Mid.java:3) [a.jar]',
    'void a.Leaf.work() (Leaf.java:4) [a.jar]',
    'void a.Small.other() (Small.java:5) [a.jar]',
    'void a.Tiny.rare() (Tiny.java:6) [a.jar]',
    'void a.Child.bit() (Child.java:7) [a.jar]',
  ],
  params: ['sql', 'binds'],
  root: {
    methodIdx: 0,
    durationMs: 1070,
    selfDurationMs: 120,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 8,
    selfExecutions: 1,
    params: [
      {
        paramIdx: 0,
        groups: [
          {
            value: 'select 1',
            durationMs: 500,
            executions: 2,
            params: [{ paramIdx: 1, groups: [{ value: '[42]', durationMs: 500, executions: 2 }] }],
          },
          { value: '::other', durationMs: 100, executions: 1 },
        ],
      },
    ],
    children: [
      {
        methodIdx: 1,
        durationMs: 800,
        selfDurationMs: 0,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 3,
        selfExecutions: 1,
        children: [
          {
            methodIdx: 2,
            durationMs: 800,
            selfDurationMs: 0,
            suspensionMs: 0,
            selfSuspensionMs: 0,
            executions: 2,
            selfExecutions: 1,
            children: [
              {
                methodIdx: 3,
                durationMs: 800,
                selfDurationMs: 800,
                suspensionMs: 0,
                selfSuspensionMs: 0,
                executions: 1,
                selfExecutions: 1,
              },
            ],
          },
        ],
      },
      {
        methodIdx: 4,
        durationMs: 100,
        selfDurationMs: 20,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 2,
        selfExecutions: 1,
        children: [
          { methodIdx: 6, durationMs: 80, selfDurationMs: 80, suspensionMs: 0, selfSuspensionMs: 0, executions: 1, selfExecutions: 1 },
        ],
      },
      {
        methodIdx: 5,
        durationMs: 50,
        selfDurationMs: 10,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 2,
        selfExecutions: 1,
        children: [
          { methodIdx: 6, durationMs: 40, selfDurationMs: 40, suspensionMs: 0, selfSuspensionMs: 0, executions: 1, selfExecutions: 1 },
        ],
      },
    ],
  },
};

function names(model: ReturnType<typeof buildTreeModel>, rows: TreeRow[]): string[] {
  return rows.map((r) =>
    r.kind === 'node' ? `${model.methods[r.node.methodIdx]!.split(' ')[1]!.split('(')[0]!}@${r.depth}` : `param:${r.group.value}@${r.depth}`,
  );
}

describe('buildTreeModel', () => {
  it('maps wire executions to self/child pairs and flags unresolved params', () => {
    const model = buildTreeModel(WIRE);
    expect(model.nodeCount).toBe(8);
    expect(model.root.selfExecutions).toBe(1);
    expect(model.root.childExecutions).toBe(7);
    expect(model.hasUnresolvedParams).toBe(false);
    const big = model.root.children[0]!;
    expect(big.collapseLevels).toBe(1); // pass-through into mid
  });
});

describe('initialExpansion + buildRows', () => {
  const model = buildTreeModel(WIRE);

  it('auto-expands large branches, collapses ≤10% subtrees, skips chains', () => {
    const { expanded, capped } = initialExpansion(model, 10_000);
    expect(capped).toBe(false);
    const rows = buildRows(model, { expanded, revealedChains: new Set(), expandedParams: new Set() });
    expect(names(model, rows)).toEqual([
      'a.Root.run@0',
      'param:select 1@1',
      'param:::other@1',
      'a.Big.pass@1', // skips mid
      'a.Leaf.work@2',
      'a.Small.other@1', // collapsed
      'a.Tiny.rare@1', // collapsed
    ]);
    const bigRow = rows.find((r) => r.kind === 'node' && r.node.methodIdx === 1);
    expect(bigRow?.kind === 'node' && bigRow.skippedLevels).toBe(1);
  });

  it('expanding a collapsed subtree applies that node’s own cutoffs', () => {
    const { expanded } = initialExpansion(model, 10_000);
    const small = model.root.children.find((c) => c.methodIdx === 4)!;
    expandLarge(small, 10_000, expanded);
    const rows = buildRows(model, { expanded, revealedChains: new Set(), expandedParams: new Set() });
    expect(names(model, rows)).toContain('a.Child.bit@2');
  });

  it('revealing a chain shows the skipped pass-through nodes', () => {
    const { expanded } = initialExpansion(model, 10_000);
    const big = model.root.children[0]!;
    const mid = big.children[0]!;
    expanded.add(mid.id);
    const rows = buildRows(model, {
      expanded,
      revealedChains: new Set([big.id]),
      expandedParams: new Set(),
    });
    expect(names(model, rows)).toContain('a.Mid.pass@2');
    expect(names(model, rows)).toContain('a.Leaf.work@3');
  });

  it('expands nested binds under their SQL group on demand', () => {
    const { expanded } = initialExpansion(model, 10_000);
    const base = { expanded, revealedChains: new Set<number>(), expandedParams: new Set<string>() };
    const collapsedRows = buildRows(model, base);
    const sqlRow = collapsedRows.find((r) => r.kind === 'param' && r.group.value === 'select 1');
    if (sqlRow?.kind !== 'param') throw new Error('sql param row missing');
    expect(sqlRow.hasChildren).toBe(true);

    const rows = buildRows(model, { ...base, expandedParams: new Set([sqlRow.pathKey]) });
    expect(names(model, rows)).toContain('param:[42]@2');
  });

  it('caps the row budget instead of expanding a pathological tree', () => {
    const { capped } = initialExpansion(model, 3);
    expect(capped).toBe(true);
  });
});

describe('expandAll', () => {
  const model = buildTreeModel(WIRE);
  const noState = { revealedChains: new Set<number>(), expandedParams: new Set<string>() };

  it('opens every node, also the subtrees that start collapsed, and keeps chains skipped', () => {
    const { expanded, capped, rows } = expandAll(model.root, 10_000);
    expect(capped).toBe(false);
    const built = buildRows(model, { ...noState, expanded });
    expect(names(model, built)).toEqual([
      'a.Root.run@0',
      'param:select 1@1',
      'param:::other@1',
      'a.Big.pass@1', // still skips mid
      'a.Leaf.work@2',
      'a.Small.other@1',
      'a.Child.bit@2',
      'a.Tiny.rare@1',
      'a.Child.bit@2',
    ]);
    // The count it reports is the count the tree draws.
    expect(rows).toBe(built.length);
  });

  it('opens a revealed chain level by level', () => {
    const big = model.root.children[0]!;
    const { expanded } = expandAll(model.root, 10_000, new Set([big.id]));
    const built = buildRows(model, { ...noState, revealedChains: new Set([big.id]), expanded });
    expect(names(model, built)).toContain('a.Mid.pass@2');
    expect(names(model, built)).toContain('a.Leaf.work@3');
  });

  it('stops breadth first at the row budget and never draws past it', () => {
    // Root (1) + its 2 params and 3 children = 6 rows; Big adds 1 more = 7;
    // Small would make 8.
    const { expanded, capped, rows } = expandAll(model.root, 7);
    expect(capped).toBe(true);
    expect(rows).toBe(7);
    const built = buildRows(model, { ...noState, expanded });
    expect(built.length).toBe(7);
    expect(names(model, built)).toContain('a.Leaf.work@2');
    expect(names(model, built)).not.toContain('a.Child.bit@2');
  });

  it('keeps the root open when its own rows exceed the budget', () => {
    const { expanded, capped } = expandAll(model.root, 3);
    expect(capped).toBe(true);
    expect([...expanded]).toEqual([model.root.id]);
  });

  it('builds the children of a lazy node before it walks them', () => {
    // A hotspot tree grafts a node's callers on first expansion; Expand all must not skip them.
    const lazy = buildTreeModel(WIRE);
    const small = lazy.root.children.find((c) => lazy.methods[c.methodIdx]!.includes('Small'))!;
    const grafted = small.children;
    small.children = [];
    small.notComputed = true;
    const computed: number[] = [];
    const { expanded } = expandAll(lazy.root, 10_000, new Set(), (n) => {
      computed.push(n.id);
      n.children = grafted;
      n.notComputed = false;
    });
    expect(computed).toEqual([small.id]);
    expect(expanded.has(small.id)).toBe(true);
    expect(names(lazy, buildRows(lazy, { ...noState, expanded }))).toContain('a.Child.bit@2');
  });
});

describe('guideCells', () => {
  const model = buildTreeModel(WIRE);

  /** Every node open and the SQL group's binds shown, so each kind of row is on screen. */
  function allRows(): TreeRow[] {
    const expanded = new Set<number>();
    const open = (n: (typeof model)['root']): void => {
      expanded.add(n.id);
      n.children.forEach(open);
    };
    open(model.root);
    const base = { expanded, revealedChains: new Set<number>(), expandedParams: new Set<string>() };
    const sql = buildRows(model, base).find((r) => r.kind === 'param' && r.group.value === 'select 1');
    if (sql?.kind !== 'param') throw new Error('sql param row missing');
    return buildRows(model, { ...base, expandedParams: new Set([sql.pathKey]) });
  }

  const cellsByName = (): Record<string, string> => {
    const rows = allRows();
    return Object.fromEntries(names(model, rows).map((name, i) => [name, guideCells(rows[i]!).join(' ')]));
  };

  it('gives the root no connector', () => {
    expect(cellsByName()['a.Root.run@0']).toBe('');
  });

  it('branches to a middle child and lets the line run on', () => {
    const cells = cellsByName();
    expect(cells['a.Big.pass@1']).toBe('tee');
    expect(cells['a.Small.other@1']).toBe('tee');
  });

  it('ends the line at the last child', () => {
    expect(cellsByName()['a.Tiny.rare@1']).toBe('end');
  });

  it('passes the line of an open ancestor and leaves a closed level blank', () => {
    const rows = allRows();
    const child = (parentMethodIdx: number): string => {
      const row = rows.find((r) => r.kind === 'node' && r.node.parent?.methodIdx === parentMethodIdx && r.depth === 2);
      return guideCells(row!).join(' ');
    };
    // Small has a sibling below, so its line passes its child; Tiny is the last one.
    expect(child(4)).toBe('pass end');
    expect(child(5)).toBe('none end');
  });

  it('gives a parameter row no elbow, only the line to the child calls', () => {
    const cells = cellsByName();
    expect(cells['param:select 1@1']).toBe('pass');
    expect(cells['param:::other@1']).toBe('pass');
    // A bind sits one level deeper, and that level has no line.
    expect(cells['param:[42]@2']).toBe('pass none');
  });

  it('draws nothing beside the parameters of a method that calls nothing', () => {
    const leafWithParam: TreeWire = {
      v: 1,
      methods: ['void a.Root.run() (Root.java:1) [a.jar]', 'void a.Dao.query() (Dao.java:2) [a.jar]'],
      params: ['sql'],
      root: {
        methodIdx: 0,
        durationMs: 100,
        selfDurationMs: 10,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 2,
        selfExecutions: 1,
        children: [
          {
            methodIdx: 1,
            durationMs: 90,
            selfDurationMs: 90,
            suspensionMs: 0,
            selfSuspensionMs: 0,
            executions: 1,
            selfExecutions: 1,
            params: [{ paramIdx: 0, groups: [{ value: 'select 2', durationMs: 90, executions: 1 }] }],
          },
        ],
      },
    };
    const m = buildTreeModel(leafWithParam);
    const rows = buildRows(m, {
      expanded: new Set([m.root.id, m.root.children[0]!.id]),
      revealedChains: new Set(),
      expandedParams: new Set(),
    });
    expect(rows.map((r) => guideCells(r).join(' '))).toEqual(['', 'end', 'none none']);
  });

  it('has one cell per level of depth', () => {
    for (const row of allRows()) expect(guideCells(row)).toHaveLength(row.depth);
  });
});

describe('searchTree', () => {
  const model = buildTreeModel(WIRE);

  it('matches methods and expands every ancestor of a match', () => {
    const result = searchTree(model, 'leaf.work');
    expect(result).not.toBeNull();
    expect(result!.matchCount).toBe(1);
    const big = model.root.children[0]!;
    const mid = big.children[0]!;
    expect([...result!.expand].sort()).toEqual([model.root.id, big.id, mid.id].sort());

    // With search active the chain is revealed, so the match is reachable.
    const rows = buildRows(
      model,
      { expanded: new Set([...result!.expand, model.root.id]), revealedChains: new Set(), expandedParams: new Set() },
      true,
    );
    expect(names(model, rows)).toContain('a.Mid.pass@2');
    expect(names(model, rows)).toContain('a.Leaf.work@3');
  });

  it('returns null for a blank query', () => {
    expect(searchTree(model, '  ')).toBeNull();
  });

  it('matches a visible param value and expands the node that owns it', () => {
    // root's own param row (sql: 'select 1'), not a descendant node — the
    // match needs root itself expanded, not just root's ancestors (PR 708
    // review #10).
    const result = searchTree(model, 'select 1');
    expect(result).not.toBeNull();
    expect(result!.matched.has(model.root.id)).toBe(true);
    expect(result!.expand.has(model.root.id)).toBe(true);
  });

  it('matches a bind value nested under its SQL group', () => {
    const result = searchTree(model, '42');
    expect(result).not.toBeNull();
    expect(result!.matched.has(model.root.id)).toBe(true);
  });

  it('matches a param key name, not just its value', () => {
    const result = searchTree(model, 'binds');
    expect(result).not.toBeNull();
    expect(result!.matched.has(model.root.id)).toBe(true);
  });
});
