import { describe, expect, it } from 'vitest';

import type { TreeWire } from '../msgpack/tree-wire';
import { buildTreeModel } from './model';
import { searchTree } from './search';
import { buildRows, expandLarge, initialExpansion } from './visible-rows';
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
