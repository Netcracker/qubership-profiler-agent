import { describe, expect, it } from 'vitest';

import type { TreeWire } from '../msgpack/tree-wire';
import { buildTreeModel } from './model';
import { DEFAULT_TOP_SQL_GROUPS, summariseParams } from './params-summary';

// root(1000, self 400) with a sql param (2 groups) and one metadata param
//  └─ child(600, self 600) repeats one of the same sql values, so the shape
//     summary must aggregate across nodes rather than key on the node.
const WIRE: TreeWire = {
  v: 1,
  methods: ['void a.Root.run() (Root.java:1) [a.jar]', 'void a.Child.run() (Child.java:2) [a.jar]'],
  params: ['sql', 'binds', 'node.name'],
  root: {
    methodIdx: 0,
    durationMs: 1000,
    selfDurationMs: 400,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 2,
    selfExecutions: 1,
    params: [
      { paramIdx: 2, groups: [{ value: 'pod-a', durationMs: 1000, executions: 1 }] },
      {
        paramIdx: 0,
        groups: [
          {
            value: 'SELECT * FROM orders WHERE id = 1',
            durationMs: 100,
            executions: 1,
            params: [{ paramIdx: 1, groups: [{ value: '1', durationMs: 100, executions: 1 }] }],
          },
          { value: 'DELETE FROM widgets', durationMs: 50, executions: 1 },
        ],
      },
    ],
    children: [
      {
        methodIdx: 1,
        durationMs: 600,
        selfDurationMs: 600,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 1,
        selfExecutions: 1,
        params: [
          {
            paramIdx: 0,
            groups: [
              {
                // Differs from the root's SQL only by its literal — same shape.
                value: 'SELECT * FROM orders WHERE id = 42',
                durationMs: 200,
                executions: 3,
                params: [{ paramIdx: 1, groups: [{ value: '42', durationMs: 200, executions: 3 }] }],
              },
            ],
          },
        ],
      },
    ],
  },
};

describe('summariseParams', () => {
  it('keeps non-sql params as flat metadata rows', () => {
    const summary = summariseParams(buildTreeModel(WIRE));
    expect(summary.metadata).toHaveLength(1);
    expect(summary.metadata[0]).toMatchObject({ value: 'pod-a', durationMs: 1000, executions: 1 });
  });

  it('groups sql values sharing a signature across nodes into one shape', () => {
    const summary = summariseParams(buildTreeModel(WIRE));
    const select = summary.sql.find((s) => s.value === 'SELECT * FROM orders WHERE id = 1')!;
    expect(select).toBeDefined();
    // 100 (root) + 200 (child), despite differing literals.
    expect(select.durationMs).toBe(300);
    expect(select.executions).toBe(4); // 1 (root) + 3 (child)
  });

  it('keeps a differently-shaped sql text as a separate group', () => {
    const summary = summariseParams(buildTreeModel(WIRE));
    const del = summary.sql.find((s) => s.value === 'DELETE FROM widgets')!;
    expect(del).toBeDefined();
    expect(del.durationMs).toBe(50);
  });

  it('sorts sql shapes by cumulative duration, descending', () => {
    const summary = summariseParams(buildTreeModel(WIRE));
    expect(summary.sql[0]!.value).toBe('SELECT * FROM orders WHERE id = 1'); // 300 > 50
  });

  it('nests binds under their owning sql shape, aggregated by their own signature', () => {
    const summary = summariseParams(buildTreeModel(WIRE));
    const select = summary.sql.find((s) => s.value === 'SELECT * FROM orders WHERE id = 1')!;
    // The bind values '1' and '42' are numeric literals, so they share a signature too.
    expect(select.binds).toHaveLength(1);
    expect(select.binds[0]).toMatchObject({ durationMs: 300, executions: 4 });
  });

  it('does not nest binds under an unrelated sql shape', () => {
    const summary = summariseParams(buildTreeModel(WIRE));
    const del = summary.sql.find((s) => s.value === 'DELETE FROM widgets')!;
    expect(del.binds).toHaveLength(0);
  });

  it('folds shapes beyond the top-N cap into ::other, keeping the hot ones on top', () => {
    const groups = Array.from({ length: DEFAULT_TOP_SQL_GROUPS + 5 }, (_, i) => ({
      // A unique word count per group defeats the signature — each is its own shape.
      value: `SELECT${' a'.repeat(i + 1)}`,
      durationMs: DEFAULT_TOP_SQL_GROUPS + 5 - i, // strictly descending, so rank is deterministic
      executions: 1,
    }));
    const wire: TreeWire = {
      v: 1,
      methods: ['void a.Root.run() (Root.java:1) [a.jar]'],
      params: ['sql'],
      root: {
        methodIdx: 0,
        durationMs: groups.reduce((sum, g) => sum + g.durationMs, 0),
        selfDurationMs: 0,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 1,
        selfExecutions: 1,
        params: [{ paramIdx: 0, groups }],
      },
    };
    const summary = summariseParams(buildTreeModel(wire));
    expect(summary.sql).toHaveLength(DEFAULT_TOP_SQL_GROUPS + 1); // the cap plus ::other
    expect(summary.sql[0]!.value).toBe('SELECT a'); // the largest group ranks first
    const other = summary.sql.at(-1)!;
    expect(other.value).toBe('::other');
    // The 5 groups past the cap: durations 5,4,3,2,1 = 15.
    expect(other.durationMs).toBe(15);
    expect(other.executions).toBe(5);
  });

  it('merges the node-level ::other bucket into the tab-level overflow bucket, not a second row', () => {
    const wire: TreeWire = {
      v: 1,
      methods: ['void a.Root.run() (Root.java:1) [a.jar]'],
      params: ['sql'],
      root: {
        methodIdx: 0,
        durationMs: 130,
        selfDurationMs: 130,
        suspensionMs: 0,
        selfSuspensionMs: 0,
        executions: 1,
        selfExecutions: 1,
        params: [
          {
            paramIdx: 0,
            groups: [
              { value: 'SELECT a', durationMs: 100, executions: 1 },
              { value: '::other', durationMs: 30, executions: 7 },
            ],
          },
        ],
      },
    };
    const summary = summariseParams(buildTreeModel(wire), 1);
    expect(summary.sql).toHaveLength(2); // 1 shape (the cap) + one merged ::other
    const other = summary.sql.at(-1)!;
    expect(other.value).toBe('::other');
    expect(other.durationMs).toBe(30);
    expect(other.executions).toBe(7);
  });
});
