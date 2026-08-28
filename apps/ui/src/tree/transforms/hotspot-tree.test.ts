import { describe, expect, it } from 'vitest';

import type { TreeWire } from '../../msgpack/tree-wire';
import { buildTreeModel } from '../model';
import type { TreeModel, TreeNode } from '../model';
import { applyCategories, parseCategoryConfig } from './categories';
import { computeFlatProfile } from './flat-profile';
import { buildHotspotTree, graftIncoming } from './hotspot-tree';
import { incomingCalls } from './merge';

// The hotspot grouping: dotted category paths become a group tree, and a
// method row grafts the same callers incomingCalls computes.

const METHODS = [
  'void com.acme.Main.run() (Main.java:1) [app.jar]',
  'void com.acme.db.Dao.select() (Dao.java:10) [app.jar]',
  'void com.acme.db.Dao.update() (Dao.java:20) [app.jar]',
  'void com.acme.http.Client.get() (Client.java:5) [app.jar]',
];

const CONFIG = `db.jdbc.select *Dao.select*
db.jdbc.update *Dao.update*
http *Client.get*`;

function fixtureModel(): TreeModel {
  const leaf = (methodIdx: number, durationMs: number) => ({
    methodIdx,
    durationMs,
    selfDurationMs: durationMs,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 1,
    selfExecutions: 1,
  });
  const wire: TreeWire = {
    v: 1,
    methods: [...METHODS],
    params: [],
    root: {
      methodIdx: 0,
      durationMs: 1150,
      selfDurationMs: 100,
      suspensionMs: 0,
      selfSuspensionMs: 0,
      executions: 5,
      selfExecutions: 1,
      children: [
        leaf(1, 500),
        leaf(2, 300),
        {
          methodIdx: 3,
          durationMs: 250,
          selfDurationMs: 200,
          suspensionMs: 0,
          selfSuspensionMs: 0,
          executions: 2,
          selfExecutions: 1,
          children: [leaf(1, 50)],
        },
      ],
    },
  };
  const model = buildTreeModel(wire);
  applyCategories(model, parseCategoryConfig(CONFIG));
  return model;
}

const word = (model: TreeModel, node: TreeNode): string => model.methods[node.methodIdx] ?? '?';

describe('buildHotspotTree', () => {
  it('splits dotted category names into a group tree', () => {
    const model = fixtureModel();
    const hotspot = buildHotspotTree(model, computeFlatProfile(model));
    const m = hotspot.model;

    const root = m.root;
    expect(word(m, root)).toBe('all');
    expect(root.children.map((c) => word(m, c))).toEqual(['db', 'http', 'unsorted']);

    const db = root.children[0]!;
    expect(db.children.map((c) => word(m, c))).toEqual(['jdbc']);
    const jdbc = db.children[0]!;
    expect(jdbc.children.map((c) => word(m, c))).toEqual(['select', 'update']);

    // Leaves hold the method rows, flagged for the lazy incoming graft.
    const select = jdbc.children[0]!;
    expect(select.children).toHaveLength(1);
    const method = select.children[0]!;
    expect(word(m, method)).toBe(METHODS[1]);
    expect(method.notComputed).toBe(true);
    expect(method.selfDurationMs).toBe(550);
    expect(method.durationMs).toBe(550);

    // Group totals roll up: db = select 550 + update 300.
    expect(db.selfDurationMs).toBe(850);
    expect(root.selfDurationMs).toBe(550 + 300 + 200 + 100);

    // The grouping starts unfolded; the method rows start collapsed.
    for (const g of [root, db, jdbc, select]) expect(hotspot.initialExpanded.has(g.id)).toBe(true);
    expect(hotspot.initialExpanded.has(method.id)).toBe(false);
    // Skeleton ids stay negative so graft ids can never collide.
    expect(method.id).toBeLessThan(0);
  });

  it('grafts the same callers incomingCalls produces', () => {
    const model = fixtureModel();
    const hotspot = buildHotspotTree(model, computeFlatProfile(model));
    const method = hotspot.model.root.children[0]!.children[0]!.children[0]!.children[0]!;
    expect(word(hotspot.model, method)).toBe(METHODS[1]);

    graftIncoming(model, method);
    expect(method.notComputed).toBe(false);

    const shape = (n: TreeNode): unknown => ({
      methodIdx: n.methodIdx,
      durationMs: n.durationMs,
      selfDurationMs: n.selfDurationMs,
      children: n.children.map(shape),
    });
    const reference = incomingCalls(model, method.methodIdx, 'db.jdbc.select');
    expect(method.children.map(shape)).toEqual(reference.root.children.map(shape));
    // Both occurrences reach the callers: Main.run directly and via Client.get.
    expect(method.children.map((c) => word(model, c)).sort()).toEqual([METHODS[0], METHODS[3]].sort());
  });
});
