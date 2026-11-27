import fc from 'fast-check';
import { describe, expect, it } from 'vitest';

import type { TreeWire } from '../../msgpack/tree-wire';
import { buildTreeModel, createNode } from '../model';
import type { TreeModel, TreeNode } from '../model';
import { applyCategories, parseCategoryConfig } from './categories';
import { toFlameData } from './flame-data';
import type { FlameDatum } from './flame-data';

// Synthetic model (the model sorts children by duration, descending):
//   Entry(1000, self 100)
//   ├─ ServiceA(500, self 100)
//   │   ├─ Query.run(300, self 300)
//   │   └─ Util.format(100, self 100)
//   └─ ServiceB(400, self 399)
//       └─ Util.format(1, self 1)
const METHODS = [
  'void e.Entry.handle() (Entry.java:1) [app.jar]',
  'void s.ServiceA.doWork() (ServiceA.java:2) [app.jar]',
  'ResultSet db.Query.run() (Query.java:3) [db.jar]',
  'void s.ServiceB.other() (ServiceB.java:4) [app.jar]',
  'String u.Util.format() (Util.java:5) [app.jar]',
];

type WireNode = TreeWire['root'];

const node = (methodIdx: number, durationMs: number, selfDurationMs: number, children?: WireNode[]): WireNode => ({
  methodIdx,
  durationMs,
  selfDurationMs,
  suspensionMs: 0,
  selfSuspensionMs: 0,
  executions: 1,
  selfExecutions: 1,
  ...(children === undefined ? {} : { children }),
});

function fixture(): TreeModel {
  return buildTreeModel({
    v: 1,
    methods: [...METHODS],
    params: [],
    root: node(0, 1000, 100, [
      node(3, 400, 399, [node(4, 1, 1)]),
      node(1, 500, 100, [node(4, 100, 100), node(2, 300, 300)]),
    ]),
  });
}

const nameOf = (n: TreeNode): string => `m${n.methodIdx}`;

/** `[name, value, children]`, for compact assertions. */
type Shape = [string, number, Shape[]];
function shape(d: FlameDatum): Shape {
  return [d.name, d.value, d.children.map(shape)];
}

function walk(d: FlameDatum, visit: (d: FlameDatum) => void): void {
  visit(d);
  d.children.forEach((c) => walk(c, visit));
}

describe('toFlameData', () => {
  it('builds the {name, value, children} tree in the model order, with total times as values', () => {
    expect(shape(toFlameData(fixture().root, nameOf))).toEqual([
      'm0',
      1000,
      [
        ['m1', 500, [['m2', 300, []], ['m4', 100, []]]],
        ['m3', 400, [['m4', 1, []]]],
      ],
    ]);
  });

  it('keeps a reference to each model node, for the selection', () => {
    const model = fixture();
    const data = toFlameData(model.root, nameOf);
    expect(data.node).toBe(model.root);
    expect(data.children[0]!.node).toBe(model.root.children[0]);
    expect(data.children[0]!.children[1]!.node).toBe(model.root.children[0]!.children[1]);
  });

  it('carries the effective category: the own one, else the nearest ancestor’s', () => {
    const model = fixture();
    applyCategories(model, parseCategoryConfig('svc s.ServiceA.*\ndb db.Query.run'));
    const data = toFlameData(model.root, nameOf);
    const [serviceA, serviceB] = data.children;
    expect(data.category).toBeNull();
    expect(serviceA!.category?.name).toBe('svc');
    expect(serviceA!.children[0]!.category?.name).toBe('db');
    expect(serviceA!.children[1]!.category?.name).toBe('svc');
    expect(serviceB!.category).toBeNull();
  });

  it('starts a subtree with the category it has in the whole call', () => {
    const model = fixture();
    applyCategories(model, parseCategoryConfig('svc s.ServiceA.*'));
    const util = model.root.children[0]!.children[1]!;
    expect(toFlameData(util, nameOf).category?.name).toBe('svc');
  });

  it('starts a subtree with no category when none of its ancestors has one', () => {
    const model = fixture();
    applyCategories(model, parseCategoryConfig('db db.Query.run'));
    const util = model.root.children[0]!.children[1]!;
    expect(toFlameData(util, nameOf).category).toBeNull();
  });

  it('clips children to the time their parent has left', () => {
    // A merged or adjusted tree can claim more time below a node than the node has.
    const root = createNode(0, 0);
    root.durationMs = 100;
    [70, 50, 20].forEach((ms, i) => {
      const child = createNode(i + 1, 1);
      child.durationMs = ms;
      child.parent = root;
      root.children.push(child);
    });
    expect(shape(toFlameData(root, nameOf))).toEqual(['m0', 100, [['m1', 70, []], ['m1', 30, []], ['m1', 0, []]]]);
  });

  it('gives a root without duration the full width and its children none', () => {
    const root = createNode(0, 0);
    const child = createNode(1, 1);
    child.durationMs = 5;
    child.parent = root;
    root.children.push(child);
    expect(shape(toFlameData(root, nameOf))).toEqual(['m0', 1, [['m1', 0, []]]]);
  });
});

describe('toFlameData invariants', () => {
  /** Random trees whose child totals may exceed the parent's, as adjusted trees can. */
  const arbTree = fc.letrec<{ tree: WireNode }>((tie) => ({
    tree: fc
      .record({
        durationMs: fc.integer({ min: 0, max: 1000 }),
        children: fc.oneof(
          { depthSize: 'small', withCrossShrink: true },
          fc.constant<WireNode[]>([]),
          fc.array(tie('tree'), { maxLength: 3 }),
        ),
      })
      .map(({ durationMs, children }) => node(0, durationMs, 0, children)),
  })).tree;

  it('never gives the children of a frame more than the frame', () => {
    fc.assert(
      fc.property(arbTree, (wire) => {
        const data = toFlameData(buildTreeModel({ v: 1, methods: ['m'], params: [], root: wire }).root, nameOf);
        walk(data, (d) => {
          const below = d.children.reduce((sum, c) => sum + c.value, 0);
          expect(below).toBeLessThanOrEqual(d.value);
          expect(d.value).toBeGreaterThanOrEqual(0);
        });
      }),
      { numRuns: 200 },
    );
  });
});
