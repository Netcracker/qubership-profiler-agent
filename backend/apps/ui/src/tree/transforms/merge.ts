import { createNode } from '../model';
import type { TreeModel, TreeNode } from '../model';
import { sortNode } from '../sort-node';
import { mergeParamsInto } from './params-merge';

// The old UI's per-node merge operations, ported over the merged model
// (profiler.mjs mergeTopDown:6167, mergeBottomUp:6227, findUsages:6465).
// Node identity is methodIdx alone — the signature axis was dropped with
// the server-side merge (stage5-progress.md, merge keying decision).

let nextId = 0;
const freshNode = (methodIdx: number): TreeNode => createNode(nextId++, methodIdx);

function getOrCreateChild(parent: TreeNode, methodIdx: number): TreeNode {
  let child = parent.children.find((c) => c.methodIdx === methodIdx);
  if (child === undefined) {
    child = freshNode(methodIdx);
    child.parent = parent;
    parent.children.push(child);
  }
  return child;
}

function countNodes(root: TreeNode): number {
  let n = 1;
  root.children.forEach((c) => (n += countNodes(c)));
  return n;
}

function derived(model: TreeModel, root: TreeNode, extraMethods: string[] = []): TreeModel {
  return {
    methods: extraMethods.length === 0 ? model.methods : [...model.methods, ...extraMethods],
    paramKeys: model.paramKeys,
    root,
    nodeCount: countNodes(root),
    hasUnresolvedParams: model.hasUnresolvedParams,
  };
}

/**
 * Outgoing calls (old mergeTopDown): every occurrence of the method merges
 * its subtree into one node; a nested occurrence folds into the root, so
 * self-recursion cannot inflate totals.
 */
export function outgoingCalls(model: TreeModel, methodIdx: number): TreeModel {
  const root = freshNode(methodIdx);

  const copy = (src: TreeNode, dst: TreeNode): void => {
    dst.selfDurationMs += src.selfDurationMs;
    dst.selfSuspensionMs += src.selfSuspensionMs;
    dst.selfExecutions += src.selfExecutions;
    dst.params = mergeParamsInto(dst.params, src.params);
    for (const child of src.children) {
      if (child.methodIdx === methodIdx) copy(child, root);
      else copy(child, getOrCreateChild(dst, child.methodIdx));
    }
  };

  const find = (node: TreeNode): void => {
    if (node.methodIdx === methodIdx) {
      copy(node, root);
      return; // nested occurrences fold during the copy
    }
    node.children.forEach(find);
  };
  find(model.root);

  // Old computeTotals: totals recompute bottom-up from the merged selves.
  const computeTotals = (node: TreeNode): void => {
    node.durationMs = node.selfDurationMs;
    node.suspensionMs = node.selfSuspensionMs;
    node.childExecutions = 0;
    for (const child of node.children) {
      computeTotals(child);
      node.durationMs += child.durationMs;
      node.suspensionMs += child.suspensionMs;
      node.childExecutions += child.selfExecutions + child.childExecutions;
    }
  };
  computeTotals(root);
  sortNode(root, 'duration');
  return derived(model, root);
}

interface BottomUpAccumulator {
  root: TreeNode;
  nodes: TreeNode[];
  time: number[];
}

/**
 * Incoming calls (old mergeBottomUp): the result roots at the method and
 * grows towards the callers. A target nested under another target
 * contributes only the duration its ancestor did not already carry
 * (the time[] subtraction), so recursion is counted once.
 */
export function incomingCalls(model: TreeModel, methodIdx: number, requiredCategory?: string): TreeModel {
  const acc: BottomUpAccumulator = { root: freshNode(-1 as number), nodes: [], time: [] };
  const root = acc.root;

  const append = (level: number, duration: number, selfDuration: number, suspension: number, selfSuspension: number, executions: number): void => {
    root.durationMs += duration;
    root.selfDurationMs += selfDuration;
    root.suspensionMs += suspension;
    root.selfSuspensionMs += selfSuspension;
    root.childExecutions += executions;
    let node = root;
    for (let i = level; i >= 0; i--) {
      const x = acc.nodes[i]!;
      node = getOrCreateChild(node, x.methodIdx);
      node.selfExecutions += executions;
      node.childExecutions += x.selfExecutions;
      node.durationMs += duration;
      node.selfDurationMs += selfDuration;
      node.suspensionMs += suspension;
      node.selfSuspensionMs += selfSuspension;
      // The tree root's own params stay out, as in the old merge (i == 0).
      if (i !== 0) node.params = mergeParamsInto(node.params, x.params);
    }
  };

  const visit = (node: TreeNode, level: number): void => {
    acc.time[level] = 0;
    acc.nodes[level] = node;
    for (let i = node.children.length - 1; i >= 0; i--) visit(node.children[i]!, level + 1);

    const categoryOk = requiredCategory === undefined || node.category?.name === requiredCategory;
    if (node.methodIdx !== methodIdx || !categoryOk) {
      if (level > 0) acc.time[level - 1]! += acc.time[level]!;
      return;
    }
    const duration = node.durationMs - acc.time[level]!;
    append(level, duration, node.selfDurationMs, node.suspensionMs, node.selfSuspensionMs, node.selfExecutions);
    if (level > 0) acc.time[level - 1]! += duration;
  };
  visit(model.root, 0);

  // The pseudo-root always holds exactly one child — the method itself.
  const unwrapped = root.children.length === 1 ? root.children[0]! : root;
  unwrapped.parent = null;
  sortNode(unwrapped, 'selfDuration');
  return derived(model, unwrapped);
}

/**
 * Find usages (old findUsages): every call path that reaches the method,
 * as a tree rooted at the callers (top-down), the method at the leaves.
 * minLevel keeps shared path prefixes from double-counting child
 * executions and params across consecutive occurrences.
 */
export function findUsages(model: TreeModel, methodIdx: number): TreeModel {
  const acc: BottomUpAccumulator = { root: freshNode(-1 as number), nodes: [], time: [] };
  const root = acc.root;
  let minLevel = 0;

  const append = (level: number, duration: number, selfDuration: number, suspension: number, selfSuspension: number, executions: number): void => {
    let node = root;
    node.selfExecutions += executions;
    node.durationMs += duration;
    node.selfDurationMs += selfDuration;
    node.suspensionMs += suspension;
    node.selfSuspensionMs += selfSuspension;
    let i = 0;
    for (; i <= level; i++) {
      const x = acc.nodes[i]!;
      node = getOrCreateChild(node, x.methodIdx);
      node.selfExecutions += executions;
      node.durationMs += duration;
      node.selfDurationMs += selfDuration;
      node.suspensionMs += suspension;
      node.selfSuspensionMs += selfSuspension;
      if (i < minLevel) continue;
      node.childExecutions += x.selfExecutions;
      node.params = mergeParamsInto(node.params, x.params);
    }
    minLevel = i;
  };

  const visit = (node: TreeNode, level: number): void => {
    acc.time[level] = 0;
    acc.nodes[level] = node;
    for (let i = node.children.length - 1; i >= 0; i--) visit(node.children[i]!, level + 1);

    if (node.methodIdx !== methodIdx) {
      if (level > 0) acc.time[level - 1]! += acc.time[level]!;
      if (level < minLevel) minLevel = level;
      return;
    }
    const duration = node.durationMs - acc.time[level]!;
    append(level, duration, node.selfDurationMs, node.suspensionMs, node.selfSuspensionMs, node.selfExecutions);
    if (level > 0) acc.time[level - 1]! += duration;
    if (level < minLevel) minLevel = level;
  };
  visit(model.root, 0);

  let out = root;
  let extraMethods: string[] = [];
  if (root.children.length === 1) {
    out = root.children[0]!;
    out.parent = null;
  } else {
    // Several distinct entry paths: keep the pseudo-root with its own label.
    root.methodIdx = model.methods.length;
    extraMethods = ['all call paths'];
  }
  sortNode(out, 'selfDuration');
  return derived(model, out, extraMethods);
}
