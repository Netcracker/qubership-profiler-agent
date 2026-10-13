import fc from 'fast-check';
import { describe, expect, it } from 'vitest';

import type { TreeWire } from '../../msgpack/tree-wire';
import { buildTreeModel, createNode } from '../model';
import type { TreeModel, TreeNode } from '../model';
import { applyCategories, parseCategoryConfig } from './categories';
import { flameLayout } from './flame';
import type { FlameBlock, FlameLayout } from './flame';

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

function fixture(): TreeModel {
  const node = (methodIdx: number, durationMs: number, selfDurationMs: number, children?: TreeWire['root'][]): TreeWire['root'] => ({
    methodIdx,
    durationMs,
    selfDurationMs,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    executions: 1,
    selfExecutions: 1,
    ...(children === undefined ? {} : { children }),
  });
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

/** `[method index, depth, offset, width]` per block, for compact assertions. */
function shape(layout: FlameLayout): [number, number, number, number][] {
  return layout.blocks.map((b) => [b.node.methodIdx, b.depth, b.offset, b.width]);
}

function blockOf(layout: FlameLayout, node: TreeNode): FlameBlock {
  const block = layout.blocks.find((b) => b.node === node);
  if (block === undefined) throw new Error(`no block for node ${node.id}`);
  return block;
}

describe('flameLayout', () => {
  it('places each node under its parent, in child order, as shares of the root', () => {
    const layout = flameLayout(fixture().root);
    const got = shape(layout);
    const want: [number, number, number, number][] = [
      [0, 0, 0, 1],
      [1, 1, 0, 0.5],
      [2, 2, 0, 0.3],
      [4, 2, 0.3, 0.1],
      [3, 1, 0.5, 0.4],
      [4, 2, 0.5, 0.001],
    ];
    expect(got).toHaveLength(want.length);
    got.forEach((row, i) => {
      expect(row.slice(0, 2)).toEqual(want[i]!.slice(0, 2));
      expect(row[2]).toBeCloseTo(want[i]![2], 12);
      expect(row[3]).toBeCloseTo(want[i]![3], 12);
    });
    expect(layout.rows).toBe(3);
  });

  it('leaves out blocks narrower than minWidth, with their subtrees, and keeps the siblings in place', () => {
    const model = fixture();
    const full = flameLayout(model.root);
    // 0.2 culls Util.format (0.1 and 0.001) but keeps Query.run (0.3).
    const culled = flameLayout(model.root, { minWidth: 0.2 });
    expect(culled.blocks.map((b) => b.node.methodIdx)).toEqual([0, 1, 2, 3]);
    for (const block of culled.blocks) {
      const before = blockOf(full, block.node);
      expect(block.offset).toBe(before.offset);
      expect(block.width).toBe(before.width);
    }

    // 0.45 culls ServiceB (0.4), and its child goes with it; the third row empties.
    const coarse = flameLayout(model.root, { minWidth: 0.45 });
    expect(coarse.blocks.map((b) => b.node.methodIdx)).toEqual([0, 1]);
    expect(coarse.rows).toBe(2);
  });

  it('keeps a block exactly minWidth wide', () => {
    const layout = flameLayout(fixture().root, { minWidth: 0.5 });
    expect(layout.blocks.map((b) => b.node.methodIdx)).toEqual([0, 1]);
  });

  it('spans the full width with the subtree of a re-rooted node', () => {
    const model = fixture();
    const serviceA = model.root.children[0]!;
    expect(serviceA.methodIdx).toBe(1);
    const layout = flameLayout(serviceA);
    const got = shape(layout);
    expect(got[0]).toEqual([1, 0, 0, 1]);
    expect(got[1]![0]).toBe(2);
    expect(got[1]![2]).toBe(0);
    expect(got[1]![3]).toBeCloseTo(0.6, 12);
    expect(got[2]![0]).toBe(4);
    expect(got[2]![2]).toBeCloseTo(0.6, 12);
    expect(got[2]![3]).toBeCloseTo(0.2, 12);
    expect(layout.rows).toBe(2);
  });

  it('colors a block by its own category, else by the nearest ancestor that has one', () => {
    const model = fixture();
    applyCategories(model, parseCategoryConfig('orders s.ServiceA.doWork*\nsql db.Query.run*'));
    const layout = flameLayout(model.root);
    const names = layout.blocks.map((b) => [b.node.methodIdx, b.category?.name ?? null]);
    expect(names).toEqual([
      [0, null],
      [1, 'orders'],
      [2, 'sql'],
      [4, 'orders'],
      [3, null],
      [4, null],
    ]);
  });

  it('inherits the category from above the new root after re-rooting', () => {
    // Only the grandparent carries a category: no applyCategories pass ran
    // to copy it down, as in a derived tree.
    const top = createNode(0, 0);
    const middle = createNode(1, 1);
    const leaf = createNode(2, 2);
    top.durationMs = 10;
    middle.durationMs = 10;
    leaf.durationMs = 10;
    top.category = { name: 'orders', color: 'hsl(0,100%,95%)' };
    top.children = [middle];
    middle.parent = top;
    middle.children = [leaf];
    leaf.parent = middle;

    const layout = flameLayout(leaf);
    expect(layout.blocks).toHaveLength(1);
    expect(layout.blocks[0]!.category?.name).toBe('orders');
  });

  it('gives a zero-duration root the full width and its descendants none', () => {
    const root = createNode(0, 0);
    const child = createNode(1, 1);
    child.durationMs = 5;
    child.parent = root;
    root.children = [child];

    const layout = flameLayout(root);
    expect(shape(layout)).toEqual([
      [0, 0, 0, 1],
      [1, 1, 0, 0],
    ]);
    // One pixel of a 1000 px graph: the zero-width child is not drawn.
    expect(shape(flameLayout(root, { minWidth: 1 / 1000 }))).toEqual([[0, 0, 0, 1]]);
  });

  it('lays out a root without children as one full-width block', () => {
    const root = createNode(0, 0);
    expect(shape(flameLayout(root))).toEqual([[0, 0, 0, 1]]);
    root.durationMs = 42;
    const layout = flameLayout(root);
    expect(shape(layout)).toEqual([[0, 0, 0, 1]]);
    expect(layout.rows).toBe(1);
  });
});

// Durations are drawn independently per node, so a child may claim more than
// its parent has: the invariants must hold for totals that disagree, too.
interface Shape {
  durationMs: number;
  children: Shape[];
}

const shapeArb: fc.Arbitrary<Shape> = fc.letrec<{ node: Shape }>((tie) => ({
  node: fc.record({
    durationMs: fc.oneof(fc.constant(0), fc.integer({ min: 0, max: 10_000 }), fc.double({ min: 0, max: 1e6, noNaN: true })),
    children: fc.oneof({ depthSize: 'small', withCrossShrink: true }, fc.constant<Shape[]>([]), fc.array(tie('node'), { maxLength: 4 })),
  }),
})).node;

function build(shapeOf: Shape): TreeNode {
  let nextId = 0;
  const make = (s: Shape, parent: TreeNode | null): TreeNode => {
    const node = createNode(nextId++, 0);
    node.durationMs = s.durationMs;
    node.parent = parent;
    node.children = s.children.map((c) => make(c, node));
    return node;
  };
  return make(shapeOf, null);
}

const EPSILON = 1e-9;

describe('flameLayout invariants', () => {
  it('lays children out in child order from the parent offset, within the parent width', () => {
    fc.assert(
      fc.property(shapeArb, (s) => {
        const root = build(s);
        const layout = flameLayout(root);
        const byNode = new Map(layout.blocks.map((b) => [b.node, b]));
        expect(byNode.get(root)).toMatchObject({ offset: 0, width: 1, depth: 0 });

        for (const parent of layout.blocks) {
          let cursor = parent.offset;
          let sum = 0;
          for (const childNode of parent.node.children) {
            // Without culling every node has a block.
            const child = byNode.get(childNode)!;
            expect(child.depth).toBe(parent.depth + 1);
            expect(child.width).toBeGreaterThanOrEqual(0);
            expect(child.offset).toBeCloseTo(cursor, 9);
            cursor = child.offset + child.width;
            sum += child.width;
          }
          expect(sum).toBeLessThanOrEqual(parent.width + EPSILON);
          expect(cursor).toBeLessThanOrEqual(parent.offset + parent.width + EPSILON);
        }
      }),
    );
  });

  it('culling removes exactly the blocks under minWidth and those below them', () => {
    fc.assert(
      fc.property(shapeArb, fc.double({ min: 0, max: 1, noNaN: true }), (s, minWidth) => {
        const root = build(s);
        const full = flameLayout(root);
        const culled = flameLayout(root, { minWidth });
        const kept = new Set(culled.blocks.map((b) => b.node));

        for (const block of full.blocks) {
          const parentKept = block.node === root || kept.has(block.node.parent!);
          expect(kept.has(block.node)).toBe(parentKept && block.width >= minWidth);
        }
        for (const block of culled.blocks) {
          expect(block).toEqual(full.blocks.find((b) => b.node === block.node));
        }
      }),
    );
  });

  it('re-rooting on any node spans the full width at depth 0', () => {
    fc.assert(
      fc.property(shapeArb, fc.nat(), (s, pick) => {
        const whole = flameLayout(build(s));
        const target = whole.blocks[pick % whole.blocks.length]!.node;
        const first = flameLayout(target).blocks[0]!;
        expect(first).toMatchObject({ node: target, offset: 0, width: 1, depth: 0 });
      }),
    );
  });
});
