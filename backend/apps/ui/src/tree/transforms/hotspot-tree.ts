import { createNode } from '../model';
import type { TreeModel, TreeNode } from '../model';
import { sortNode } from '../sort-node';
import { incomingCalls } from './merge';
import type { CategoryProfile } from './flat-profile';

// Hotspots as a bottom-up tree, the old UI's shape: a dotted category name
// forms a group hierarchy (`db.jdbc.select` nests under `db.jdbc` under
// `db`, profiler.mjs allocateGroupNode:3724), each group holds its method
// rows, and a method row expands in place into its incoming callers
// (Tree__computeIncoming:6583). Grouping only — matching stays in
// categories.ts, and the tree colouring never splits on dots.

export interface HotspotTree {
  model: TreeModel;
  /** The pseudo-root and every group node: the grouping starts unfolded. */
  initialExpanded: Set<number>;
}

export function buildHotspotTree(source: TreeModel, profiles: CategoryProfile[]): HotspotTree {
  const extraMethods: string[] = [];
  // Skeleton ids are negative, like the old group nodes — the positive ids
  // the incoming-graft mints can then never collide with them.
  let nextId = -1;
  let nodeCount = 0;

  const pseudo = (word: string): TreeNode => {
    const node = createNode(nextId--, source.methods.length + extraMethods.length);
    extraMethods.push(word);
    nodeCount++;
    return node;
  };

  const root = pseudo('all');
  const initialExpanded = new Set([root.id]);
  const groupByPath = new Map<string, TreeNode>();

  const groupFor = (path: string): TreeNode => {
    if (path === '') return root;
    let node = groupByPath.get(path);
    if (node !== undefined) return node;
    const lastDot = path.lastIndexOf('.');
    node = pseudo(lastDot >= 0 ? path.slice(lastDot + 1) : path);
    const parent = groupFor(lastDot >= 0 ? path.slice(0, lastDot) : '');
    node.parent = parent;
    parent.children.push(node);
    groupByPath.set(path, node);
    initialExpanded.add(node.id);
    return node;
  };

  // Profiles arrive heaviest-first (uncategorised last), so insertion order
  // already reads by weight; no re-sort, or 'unsorted' would float up.
  for (const profile of profiles) {
    const path = profile.category?.name ?? (profiles.length > 1 ? 'unsorted' : '');
    const group = groupFor(path);
    if (profile.category !== null) group.category = profile.category;

    let suspension = 0;
    let executions = 0;
    for (const m of profile.methods) {
      const node = createNode(nextId--, m.methodIdx);
      nodeCount++;
      // Self time is the ranking axis: duration mirrors it, as the old
      // mergeCategoryResults did, so the bar length reads as share.
      node.durationMs = m.selfDurationMs;
      node.selfDurationMs = m.selfDurationMs;
      node.suspensionMs = m.selfSuspensionMs;
      node.selfSuspensionMs = m.selfSuspensionMs;
      node.selfExecutions = m.selfExecutions;
      node.params = m.params;
      node.notComputed = true;
      if (profile.category !== null) node.category = profile.category;
      node.parent = group;
      group.children.push(node);
      suspension += m.selfSuspensionMs;
      executions += m.selfExecutions;
    }

    for (let g: TreeNode | null = group; g !== null; g = g.parent) {
      g.durationMs += profile.totalSelfMs;
      g.selfDurationMs += profile.totalSelfMs;
      g.suspensionMs += suspension;
      g.selfSuspensionMs += suspension;
      g.selfExecutions += executions;
    }
  }

  return {
    model: {
      methods: [...source.methods, ...extraMethods],
      paramKeys: source.paramKeys,
      root,
      nodeCount,
      hasUnresolvedParams: source.hasUnresolvedParams,
    },
    initialExpanded,
  };
}

/**
 * Expands a hotspot row in place: the node's children become its incoming
 * callers, computed against the source tree and scoped to the node's
 * category (old Tree__computeIncoming over mergeBottomUp).
 */
export function graftIncoming(source: TreeModel, node: TreeNode): void {
  const result = incomingCalls(source, node.methodIdx, node.category?.name);
  node.children = result.root.children;
  for (const child of node.children) child.parent = node;
  sortNode(node, 'selfDuration');
  node.collapseLevels = 0;
  node.notComputed = false;
}
