import { describe, expect, it } from 'vitest';

import type { TreeWire } from '../msgpack/tree-wire';
import { buildTreeModel } from './model';
import { summariseParams } from './params-summary';

// root(1000, self 400) with a sql param (2 groups)
//  └─ child(600, self 600) repeats one of the same (key, value) pairs, so the
//     summary must aggregate across nodes rather than key on the node.
const WIRE: TreeWire = {
  v: 1,
  methods: ['void a.Root.run() (Root.java:1) [a.jar]', 'void a.Child.run() (Child.java:2) [a.jar]'],
  params: ['sql', 'binds'],
  root: {
    methodIdx: 0,
    durationMs: 1000,
    selfDurationMs: 400,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 2,
    selfExecutions: 1,
    params: [
      {
        paramIdx: 0,
        groups: [
          { value: 'select 1', durationMs: 100, executions: 1 },
          { value: 'select 2', durationMs: 50, executions: 1 },
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
        params: [{ paramIdx: 0, groups: [{ value: 'select 1', durationMs: 200, executions: 3 }] }],
      },
    ],
  },
};

describe('summariseParams', () => {
  it('aggregates the same (key, value) pair across different nodes', () => {
    const stats = summariseParams(buildTreeModel(WIRE));
    const select1 = stats.find((s) => s.value === 'select 1')!;
    expect(select1.keyIdx).toBe(0);
    expect(select1.durationMs).toBe(300); // 100 (root) + 200 (child)
    expect(select1.executions).toBe(4); // 1 (root) + 3 (child)
    expect(select1.nodes).toBe(2);
  });

  it('keeps different values under the same key as separate entries', () => {
    const stats = summariseParams(buildTreeModel(WIRE));
    expect(stats.map((s) => s.value).sort()).toEqual(['select 1', 'select 2']);
    const select2 = stats.find((s) => s.value === 'select 2')!;
    expect(select2.nodes).toBe(1);
  });

  it('sorts by total duration, descending', () => {
    const stats = summariseParams(buildTreeModel(WIRE));
    expect(stats[0]!.value).toBe('select 1'); // 300 > 50
  });
});
