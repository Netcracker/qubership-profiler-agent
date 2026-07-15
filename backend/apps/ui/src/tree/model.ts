import type { ParamWire, TreeNodeWire, TreeWire } from '../msgpack/tree-wire';
import { sortNode } from './sort-node';

// Client-side tree model over the merged wire (02 §2.5.3). Field mapping to
// the old UI's node array (profiler.mjs M_*): durationMs ↔ M_DURATION,
// selfDurationMs ↔ M_SELF_DURATION, suspension pair ↔ M_(SELF_)SUSPENSION,
// selfExecutions ↔ M_EXECUTIONS, childExecutions ↔ M_CHILD_EXECUTIONS
// (wire `executions` = self + descendants), params ↔ M_TAGS.

export interface TreeNode {
  /** Stable identity: pre-order index in the wire tree. */
  id: number;
  methodIdx: number;
  durationMs: number;
  selfDurationMs: number;
  suspensionMs: number;
  selfSuspensionMs: number;
  /** Invocations of this method directly under its parent (old M_EXECUTIONS). */
  selfExecutions: number;
  /** Invocations aggregated below this node (old M_CHILD_EXECUTIONS). */
  childExecutions: number;
  params: ParamWire[];
  children: TreeNode[];
  parent: TreeNode | null;
  /** Pass-through chain depth from sortNode (old M_COLLAPSE_LEVELS). */
  collapseLevels: number;
  /**
   * Adjust-duration bookkeeping (old M_PREV_SELF_DURATION): undefined on a
   * fresh tree; the 5.3 transforms set it. A negative value floats the node
   * to the top of the order, and −2 pins it as never-collapsible.
   */
  prevSelfDurationMs?: number;
  prevSelfSuspensionMs?: number;
  prevSelfExecutions?: number;
  /** Effective business category (old M_CATEGORY), assigned client-side. */
  category?: CategoryDef;
}

/** A business category from Setup categories (old BC_COLOR / BC_NAME pair). */
export interface CategoryDef {
  color: string;
  name: string;
}

export interface TreeModel {
  methods: string[];
  paramKeys: string[];
  root: TreeNode;
  nodeCount: number;
  /** Any param group carries an unresolved big-param reference (02 §2.5). */
  hasUnresolvedParams: boolean;
}

function hasUnresolved(params: readonly ParamWire[]): boolean {
  return params.some((p) =>
    p.groups.some((g) => g.unresolved === true || (g.params !== undefined && hasUnresolved(g.params))),
  );
}

export function buildTreeModel(wire: TreeWire): TreeModel {
  let nextId = 0;
  let unresolved = false;

  const build = (w: TreeNodeWire, parent: TreeNode | null): TreeNode => {
    const params = w.params ?? [];
    if (!unresolved && params.length > 0 && hasUnresolved(params)) unresolved = true;
    const node: TreeNode = {
      id: nextId++,
      methodIdx: w.methodIdx,
      durationMs: w.durationMs,
      selfDurationMs: w.selfDurationMs,
      suspensionMs: w.suspensionMs,
      selfSuspensionMs: w.selfSuspensionMs,
      selfExecutions: w.selfExecutions,
      childExecutions: w.executions - w.selfExecutions,
      params,
      children: [],
      parent,
      collapseLevels: 0,
    };
    node.children = (w.children ?? []).map((c) => build(c, node));
    return node;
  };

  const root = build(wire.root, null);
  sortNode(root, 'duration');
  return {
    methods: wire.methods,
    paramKeys: wire.params,
    root,
    nodeCount: nextId,
    hasUnresolvedParams: unresolved,
  };
}

/** A blank node for the 5.3 merge transforms (old Tree__createNode). */
export function createNode(id: number, methodIdx: number): TreeNode {
  return {
    id,
    methodIdx,
    durationMs: 0,
    selfDurationMs: 0,
    suspensionMs: 0,
    selfSuspensionMs: 0,
    selfExecutions: 0,
    childExecutions: 0,
    params: [],
    children: [],
    parent: null,
    collapseLevels: 0,
  };
}

/** Total invocations of the node: old `M_EXECUTIONS + M_CHILD_EXECUTIONS`. */
export function totalExecutions(node: TreeNode): number {
  return node.selfExecutions + node.childExecutions;
}

/** Follows the dominant-first-child chain `levels` deep (old collapse skip). */
export function chainEnd(node: TreeNode, levels: number): TreeNode {
  let out = node;
  for (let i = 0; i < levels && out.children.length > 0; i++) out = out.children[0]!;
  return out;
}
