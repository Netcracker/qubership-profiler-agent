import type { ParamGroupWire, ParamWire } from '../msgpack/tree-wire';
import { chainEnd, totalExecutions } from './model';
import type { TreeModel, TreeNode } from './model';

// Visible-row derivation, replicating the old renderer's behaviour
// (profiler.mjs treeNode2html / renderNodeChilds):
// - a node's subtree starts collapsed when it holds ≤10% of its render
//   context's duration (or, for zero-duration nodes, ≤10% of its calls) —
//   the context is the root on first render and the expanded node afterwards;
// - an expanded node with collapseLevels > 0 displays the children of the
//   chain end, skipping the pass-through levels, unless the chain is
//   explicitly revealed;
// - params render as rows before children, one row per aggregated group
//   (::other last, as the server ordered them), binds nested one level down.
//
// Each row also carries what the connector lines need (09 §3.3): a line runs
// from a method down to each method it calls, and a parameter row has no line
// of its own, because a parameter is not a call.

export interface NodeRow {
  kind: 'node';
  node: TreeNode;
  depth: number;
  /** Pass-through levels this row skips when expanded (0 = none). */
  skippedLevels: number;
  hasChildren: boolean;
  expanded: boolean;
  /**
   * One flag per level between the root and this row's parent: whether the
   * ancestor at that level has more siblings below, so its line keeps running
   * past this row. Shared by all rows under one parent; do not mutate.
   */
  trail: readonly boolean[];
  /** The last child of its parent: its connector ends here instead of running on. */
  last: boolean;
}

export interface ParamRow {
  kind: 'param';
  /** Stable key: node id + param/group path. */
  pathKey: string;
  /** Index into TreeModel.paramKeys. */
  keyIdx: number;
  group: ParamGroupWire;
  depth: number;
  hasChildren: boolean;
  expanded: boolean;
  /** Same as NodeRow.trail, for the levels above the method that owns the parameter. */
  trail: readonly boolean[];
  /** The owning method shows child calls below its parameters, so their line passes this row. */
  passBy: boolean;
}

export type TreeRow = NodeRow | ParamRow;

/**
 * One 18 px cell of the connector column: nothing, a vertical line passing by,
 * a branch to this row with the line running on, or the last branch.
 */
export type GuideCell = 'none' | 'pass' | 'tee' | 'end';

/**
 * The connector cells of a row, left to right; their count is the row's depth.
 * Computed per rendered row, so the 20,000-row list holds one short array per
 * parent, not one per row.
 */
export function guideCells(row: TreeRow): GuideCell[] {
  const cells: GuideCell[] = row.trail.map((runsOn) => (runsOn ? 'pass' : 'none'));
  if (row.kind === 'node') {
    if (row.depth > 0) cells.push(row.last ? 'end' : 'tee');
    return cells;
  }
  cells.push(row.passBy ? 'pass' : 'none');
  // A bind sits one level under its SQL; that level has no line.
  while (cells.length < row.depth) cells.push('none');
  return cells;
}

export interface ExpansionState {
  /** Node ids whose children (and params) are visible. */
  expanded: ReadonlySet<number>;
  /** Node ids whose pass-through chain is shown instead of skipped. */
  revealedChains: ReadonlySet<number>;
  /** Param row path keys whose nested params (binds) are visible. */
  expandedParams: ReadonlySet<string>;
}

/** Old initiallyHidden: subtree collapsed when below the context cutoffs. */
function subtreeCollapsed(x: TreeNode, cutoffDurationMs: number, cutoffCalls: number): boolean {
  return x.durationMs <= cutoffDurationMs && (x.durationMs !== 0 || totalExecutions(x) <= cutoffCalls);
}

/**
 * Expands `context` and every large descendant, the way the old renderer
 * unfolded a subtree in one go. Stops adding once the projected row count
 * exceeds `rowBudget` (the size guard of 07 §5.4 — degrade, do not freeze).
 */
export function expandLarge(
  context: TreeNode,
  rowBudget: number,
  into?: Set<number>,
): { expanded: Set<number>; capped: boolean } {
  const expanded = into ?? new Set<number>();
  const cutoffDurationMs = context.durationMs * 0.1;
  const cutoffCalls = totalExecutions(context) * 0.1;
  let rows = 0;
  let capped = false;

  const visit = (x: TreeNode): void => {
    rows += 1 + x.params.reduce((sum, p) => sum + p.groups.length, 0);
    const display = chainEnd(x, x.collapseLevels);
    if (display.children.length === 0) return;
    if (rows > rowBudget) {
      capped = true;
      return;
    }
    expanded.add(x.id);
    for (const child of display.children) {
      if (!subtreeCollapsed(child, cutoffDurationMs, cutoffCalls)) visit(child);
      else rows += 1;
    }
  };

  visit(context);
  expanded.add(context.id);
  return { expanded, capped };
}

export function initialExpansion(model: TreeModel, rowBudget: number): { expanded: Set<number>; capped: boolean } {
  return expandLarge(model.root, rowBudget);
}

function paramRows(
  params: readonly ParamWire[],
  depth: number,
  prefix: string,
  state: ExpansionState,
  out: TreeRow[],
  trail: readonly boolean[],
  passBy: boolean,
): void {
  for (const param of params) {
    for (let i = 0; i < param.groups.length; i++) {
      const group = param.groups[i]!;
      const pathKey = `${prefix}.p${param.paramIdx}g${i}`;
      const hasChildren = group.params !== undefined && group.params.length > 0;
      const expanded = hasChildren && state.expandedParams.has(pathKey);
      out.push({ kind: 'param', pathKey, keyIdx: param.paramIdx, group, depth, hasChildren, expanded, trail, passBy });
      if (expanded && group.params !== undefined) {
        paramRows(group.params, depth + 1, pathKey, state, out, trail, passBy);
      }
    }
  }
}

/**
 * Flattens the visible tree into rows for the virtualiser. `searchActive`
 * reveals every pass-through chain so a match inside one cannot hide.
 */
export function buildRows(model: TreeModel, state: ExpansionState, searchActive = false): TreeRow[] {
  const out: TreeRow[] = [];
  const visit = (x: TreeNode, depth: number, trail: readonly boolean[], last: boolean): void => {
    const skippedLevels = searchActive || state.revealedChains.has(x.id) ? 0 : x.collapseLevels;
    const display = chainEnd(x, skippedLevels);
    const hasChildren = display.children.length > 0 || x.params.length > 0 || x.notComputed === true;
    const expanded = state.expanded.has(x.id);
    out.push({ kind: 'node', node: x, depth, skippedLevels, hasChildren, expanded, trail, last });
    if (!expanded) return;
    // The root has no connector of its own, so its children start the trail.
    const childTrail = depth === 0 ? trail : [...trail, !last];
    const children = display.children;
    paramRows(x.params, depth + 1, `n${x.id}`, state, out, childTrail, children.length > 0);
    children.forEach((child, i) => visit(child, depth + 1, childTrail, i === children.length - 1));
  };
  visit(model.root, 0, [], true);
  return out;
}
