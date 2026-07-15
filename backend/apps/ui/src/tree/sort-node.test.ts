import { describe, expect, it } from 'vitest';

import type { TreeNode } from './model';
import { sortNode } from './sort-node';

// Behavioural pins for the sortNode port (profiler.mjs:6186). The fixtures
// speak the old model's language: selfExecutions ↔ M_EXECUTIONS,
// childExecutions ↔ M_CHILD_EXECUTIONS, params presence ↔ M_TAGS.

interface Spec {
  dur: number;
  self: number;
  execs?: number;
  susp?: number;
  selfSusp?: number;
  params?: boolean;
  prev?: number;
  children?: TreeNode[];
}

let nextId = 0;

function node(spec: Spec): TreeNode {
  const children = spec.children ?? [];
  const n: TreeNode = {
    id: nextId++,
    methodIdx: 0,
    durationMs: spec.dur,
    selfDurationMs: spec.self,
    suspensionMs: spec.susp ?? 0,
    selfSuspensionMs: spec.selfSusp ?? 0,
    selfExecutions: spec.execs ?? 1,
    childExecutions: children.reduce((sum, c) => sum + c.selfExecutions + c.childExecutions, 0),
    params: spec.params === true ? [{ paramIdx: 0, groups: [{ value: 'x', durationMs: 1, executions: 1 }] }] : [],
    children,
    parent: null,
    collapseLevels: 0,
  };
  if (spec.prev !== undefined) n.prevSelfDurationMs = spec.prev;
  for (const c of children) c.parent = n;
  return n;
}

describe('sortNode — top-down (duration) mode', () => {
  it('accumulates collapse levels down a pass-through chain', () => {
    // A → B → C → D(leaf, all self): C does not collapse (its child's time
    // is self time — the interesting node), B and A do.
    const d = node({ dur: 100, self: 100 });
    const c = node({ dur: 100, self: 0, children: [d] });
    const b = node({ dur: 100, self: 0, children: [c] });
    const a = node({ dur: 100, self: 0, children: [b] });
    sortNode(a);
    expect([a.collapseLevels, b.collapseLevels, c.collapseLevels, d.collapseLevels]).toEqual([2, 1, 0, 0]);
  });

  it('tolerates up to 10% unexplained time inside the chain', () => {
    const d = node({ dur: 90, self: 90 });
    const c = node({ dur: 91, self: 1, children: [d] }); // 90 unexplained? no: 91-1-90+90 = 90 > 9.1 → breaks
    const b = node({ dur: 100, self: 9, children: [c] }); // 100-9-91+1 = 1 ≤ 10 → collapses
    const a = node({ dur: 100, self: 0, children: [b] }); // 100-0-100+9 = 9 ≤ 10 → collapses
    sortNode(a);
    expect(b.collapseLevels).toBe(1);
    expect(c.collapseLevels).toBe(0);
    expect(a.collapseLevels).toBe(2);
  });

  it('params pin a node and break the chain below (-2 protocol)', () => {
    const d = node({ dur: 100, self: 100 });
    const c = node({ dur: 100, self: 0, children: [d] });
    const b = node({ dur: 100, self: 0, params: true, children: [c] });
    const a = node({ dur: 100, self: 0, children: [b] });
    sortNode(a);
    // B is pinned (-2); A's chain over it breaks: -2 → -3 → 0 levels.
    expect(b.collapseLevels).toBe(0);
    expect(a.collapseLevels).toBe(0);
  });

  it('levels resume accumulating above the pinned node (-3.. protocol)', () => {
    // grand → parent → pinned(params) → leaf. parent gets -3 (0 levels),
    // grand gets -4 → 1 level: expanding grand lands on the pinned node.
    const leaf = node({ dur: 100, self: 100 });
    const pinned = node({ dur: 100, self: 0, params: true, children: [leaf] });
    const parent = node({ dur: 100, self: 0, children: [pinned] });
    const grand = node({ dur: 100, self: 0, children: [parent] });
    sortNode(grand);
    expect(pinned.collapseLevels).toBe(0);
    expect(parent.collapseLevels).toBe(0);
    expect(grand.collapseLevels).toBe(1);
  });

  it('adjust-duration marker −2 pins exactly like params', () => {
    const d = node({ dur: 100, self: 100 });
    const b = node({ dur: 100, self: 0, prev: -2, children: [d] });
    const a = node({ dur: 100, self: 0, children: [b] });
    sortNode(a);
    expect(b.collapseLevels).toBe(0);
    expect(a.collapseLevels).toBe(0);
  });

  it('execution fan-out breaks the chain (-1)', () => {
    // The dominant child runs 10× per parent call: not a pass-through.
    const c = node({ dur: 95, self: 95, execs: 10 });
    const b = node({ dur: 100, self: 5, execs: 1, children: [c] });
    const a = node({ dur: 100, self: 0, execs: 1, children: [b] });
    sortNode(a);
    expect(b.collapseLevels).toBe(0);
    // B returned -1 → A decrements to -2 → 0 levels.
    expect(a.collapseLevels).toBe(0);
  });

  it('a negative sibling verdict cancels a positive chain (-3)', () => {
    // Without the pinned sibling the parent would collapse two levels.
    const mkStraight = () =>
      node({ dur: 95, self: 0, children: [node({ dur: 95, self: 0, children: [node({ dur: 95, self: 95 })] })] });

    const alone = node({ dur: 100, self: 0, children: [mkStraight()] });
    sortNode(alone);
    expect(alone.collapseLevels).toBe(2);

    const pinnedSibling = node({ dur: 5, self: 0, params: true, children: [node({ dur: 5, self: 5 })] });
    const parent = node({ dur: 100, self: 0, children: [mkStraight(), pinnedSibling] });
    sortNode(parent);
    expect(parent.collapseLevels).toBe(0);
  });

  it('sorts children by duration, then self, suspension, executions', () => {
    const slow = node({ dur: 50, self: 10 });
    const fast = node({ dur: 10, self: 10 });
    const suspended = node({ dur: 50, self: 10, susp: 5 });
    const parent = node({ dur: 200, self: 90, children: [fast, slow, suspended] });
    sortNode(parent);
    expect(parent.children).toEqual([suspended, slow, fast]);
  });

  it('floats adjust-marked (negative prev) children to the front', () => {
    const big = node({ dur: 100, self: 0 });
    const marked = node({ dur: 1, self: 1, prev: -1 });
    const parent = node({ dur: 101, self: 0, children: [big, marked] });
    sortNode(parent);
    expect(parent.children[0]).toBe(marked);
  });
});

describe('sortNode — bottom-up (selfDuration) mode', () => {
  it('collapses on the self-duration axis', () => {
    // Child holds ~all of the parent's self time → chain.
    const c = node({ dur: 100, self: 95, execs: 10 });
    const b = node({ dur: 100, self: 96, execs: 10, children: [c] });
    const a = node({ dur: 100, self: 100, execs: 10, children: [b] });
    sortNode(a, 'selfDuration');
    expect(a.collapseLevels).toBeGreaterThan(0);
  });

  it('breaks when the parent context fans out executions', () => {
    // parentNode.childExecutions > node.childExecutions * 5 → -1.
    const leaf = node({ dur: 10, self: 10, execs: 1 });
    const mid = node({ dur: 10, self: 10, execs: 1, children: [leaf] });
    const wide = node({ dur: 100, self: 100, execs: 100, children: [mid] });
    // Inflate the parent's descendant count well past mid's.
    wide.childExecutions = 50;
    mid.childExecutions = 1;
    sortNode(wide, 'selfDuration');
    expect(mid.collapseLevels).toBe(0);
  });

  it('sorts children by self duration first', () => {
    const bigTotal = node({ dur: 100, self: 5 });
    const bigSelf = node({ dur: 20, self: 20 });
    const parent = node({ dur: 120, self: 0, children: [bigTotal, bigSelf] });
    sortNode(parent, 'selfDuration');
    expect(parent.children[0]).toBe(bigSelf);
  });
});
