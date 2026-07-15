import fc from 'fast-check';
import { describe, expect, it } from 'vitest';

import { MsgpackDecodeError, decodeTree } from './decode';
import { encodeTree } from './encode';
import type { ParamGroupWire, ParamWire, TreeNodeWire, TreeWire } from './tree-wire';

// The decoder faces payloads from the network; corrupted input must produce
// MsgpackDecodeError — never a RangeError, stack overflow, or hang (02 §2.5.1,
// same rule the Go FuzzDecode pins for the encoder side).

function decodeIsTotal(bytes: Uint8Array): void {
  try {
    decodeTree(bytes);
  } catch (e) {
    expect(e).toBeInstanceOf(MsgpackDecodeError);
  }
}

const METHODS = ['a.A.m() (A.java:1) [a.jar]', 'b.B.n() (B.java:2) [b.jar]', 'c.C.o() (C.java:3) [c.jar]'];
const PARAM_KEYS = ['sql', 'binds', 'request.id'];

const intArb = fc.integer({ min: 0, max: 2 ** 31 - 1 });

function groupArb(depth: number): fc.Arbitrary<ParamGroupWire> {
  return fc
    .tuple(
      fc.string({ maxLength: 20 }),
      intArb,
      intArb,
      depth > 0 ? fc.option(fc.array(paramArb(depth - 1), { minLength: 1, maxLength: 2 }), { nil: undefined }) : fc.constant(undefined),
      fc.option(fc.constant(true), { nil: undefined }),
    )
    .map(([value, durationMs, executions, params, unresolved]) => {
      const g: ParamGroupWire = { value, durationMs, executions };
      if (params !== undefined) g.params = params;
      if (unresolved !== undefined) g.unresolved = unresolved;
      return g;
    });
}

function paramArb(depth: number): fc.Arbitrary<ParamWire> {
  return fc
    .tuple(fc.integer({ min: 0, max: PARAM_KEYS.length - 1 }), fc.array(groupArb(depth), { minLength: 1, maxLength: 3 }))
    .map(([paramIdx, groups]) => ({ paramIdx, groups }));
}

function nodeArb(depth: number): fc.Arbitrary<TreeNodeWire> {
  return fc
    .tuple(
      fc.integer({ min: 0, max: METHODS.length - 1 }),
      fc.tuple(intArb, intArb, intArb, intArb, intArb, intArb),
      fc.option(fc.array(paramArb(1), { minLength: 1, maxLength: 2 }), { nil: undefined }),
      depth > 0 ? fc.option(fc.array(nodeArb(depth - 1), { minLength: 1, maxLength: 3 }), { nil: undefined }) : fc.constant(undefined),
    )
    .map(([methodIdx, [durationMs, selfDurationMs, suspensionMs, selfSuspensionMs, executions, selfExecutions], params, children]) => {
      const n: TreeNodeWire = { methodIdx, durationMs, selfDurationMs, suspensionMs, selfSuspensionMs, executions, selfExecutions };
      if (params !== undefined) n.params = params;
      if (children !== undefined) n.children = children;
      return n;
    });
}

const treeArb: fc.Arbitrary<TreeWire> = nodeArb(3).map((root) => ({
  v: 1,
  methods: [...METHODS],
  params: [...PARAM_KEYS],
  root,
}));

describe('decoder totality under fuzz', () => {
  it('never throws anything but MsgpackDecodeError on arbitrary bytes', () => {
    fc.assert(
      fc.property(fc.uint8Array({ maxLength: 512 }), (bytes) => decodeIsTotal(bytes)),
      { numRuns: 500 },
    );
  });

  it('never throws anything but MsgpackDecodeError on a corrupted valid payload', () => {
    const base = encodeTree({
      v: 1,
      methods: [...METHODS],
      params: [...PARAM_KEYS],
      root: {
        methodIdx: 0,
        durationMs: 100,
        selfDurationMs: 40,
        suspensionMs: 3,
        selfSuspensionMs: 1,
        executions: 5,
        selfExecutions: 2,
        params: [{ paramIdx: 0, groups: [{ value: 'select 1', durationMs: 60, executions: 3 }] }],
        children: [
          { methodIdx: 1, durationMs: 60, selfDurationMs: 60, suspensionMs: 2, selfSuspensionMs: 2, executions: 3, selfExecutions: 3 },
        ],
      },
    });
    fc.assert(
      fc.property(
        fc.array(fc.tuple(fc.nat(base.length - 1), fc.integer({ min: 0, max: 255 })), { minLength: 1, maxLength: 8 }),
        (mutations) => {
          const corrupted = base.slice();
          for (const [pos, value] of mutations) corrupted[pos] = value;
          decodeIsTotal(corrupted);
        },
      ),
      { numRuns: 500 },
    );
  });

  it('decode ∘ encode is identity on synthetic trees', () => {
    fc.assert(
      fc.property(treeArb, (tree) => {
        expect(decodeTree(encodeTree(tree))).toEqual(tree);
      }),
      { numRuns: 100 },
    );
  });
});
