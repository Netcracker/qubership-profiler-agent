import type { TreeNode } from './model';

// Exact port of the old UI's child ordering and pass-through-chain collapse:
// profiler.mjs sortNode (line 6186) with orderCallsByDuration /
// orderCallsBySelfDuration. The collapse value protocol is preserved
// verbatim, including the negative states:
//   >= 0  — the node's dominant-child chain may collapse this many levels;
//   -1    — broken by execution fan-out (the dominant child multiplies calls);
//   -2    — pinned: the node carries params/tags, or adjust-duration marked it;
//   -3..  — broken below, but levels keep accumulating above the break:
//           collapseLevels = -3 - canCollapse for canCollapse < -2.
// 'duration' mode is the top-down tree; 'selfDuration' is bottom-up views.

export type SortMode = 'duration' | 'selfDuration';

function prevNeg(n: TreeNode): boolean {
  return n.prevSelfDurationMs !== undefined && n.prevSelfDurationMs < 0;
}

/** orderCallsByDuration: adjusted (negative-prev) nodes first, then totals. */
export function compareByDuration(a: TreeNode, b: TreeNode): number {
  if (prevNeg(a)) return prevNeg(b) ? a.prevSelfDurationMs! - b.prevSelfDurationMs! : -1;
  if (prevNeg(b)) return 1;
  return (
    b.durationMs - a.durationMs ||
    b.selfDurationMs - a.selfDurationMs ||
    b.suspensionMs - a.suspensionMs ||
    b.selfSuspensionMs - a.selfSuspensionMs ||
    b.selfExecutions + b.childExecutions - a.childExecutions - a.selfExecutions ||
    b.selfExecutions - a.selfExecutions
  );
}

/** orderCallsBySelfDuration: self-time first — the bottom-up ordering. */
export function compareBySelfDuration(a: TreeNode, b: TreeNode): number {
  if (prevNeg(a)) return prevNeg(b) ? a.prevSelfDurationMs! - b.prevSelfDurationMs! : -1;
  if (prevNeg(b)) return 1;
  return (
    b.selfDurationMs - a.selfDurationMs ||
    b.selfSuspensionMs - a.selfSuspensionMs ||
    b.durationMs - a.durationMs ||
    b.suspensionMs - a.suspensionMs ||
    b.selfExecutions - a.selfExecutions ||
    b.selfExecutions + b.childExecutions - a.childExecutions - a.selfExecutions
  );
}

/**
 * Sorts children (dominant first) and computes collapseLevels on every node.
 * Returns the node's canCollapse value (the raw protocol above); callers
 * normally only read `collapseLevels` afterwards.
 */
export function sortNode(node: TreeNode, mode: SortMode = 'duration', parentNode?: TreeNode): number {
  const byDuration = mode === 'duration';
  const cmp = byDuration ? compareByDuration : compareBySelfDuration;
  const t = node.children;
  if (t.length === 0) {
    node.collapseLevels = 0;
    return 0;
  }
  if (t.length > 1) t.sort(cmp);

  const firstChild = t[0]!;
  let canCollapse = sortNode(firstChild, mode, node);

  if (node.params.length > 0 || node.prevSelfDurationMs === -2) {
    canCollapse = -2;
  } else if (
    byDuration
      ? (node.durationMs - node.selfDurationMs - firstChild.durationMs + firstChild.selfDurationMs) * 10 <=
          node.durationMs &&
        (node.durationMs !== 0 ||
          (node.childExecutions - firstChild.childExecutions + firstChild.selfExecutions) * 10 <
            node.childExecutions + node.selfExecutions) &&
        (node.selfExecutions === 0 || node.selfExecutions * 5 > firstChild.selfExecutions)
      : (node.selfDurationMs - firstChild.selfDurationMs) * 10 <= node.selfDurationMs &&
        (node.selfDurationMs !== 0 || (node.selfExecutions - firstChild.selfExecutions) * 10 < node.selfExecutions)
  ) {
    if (canCollapse >= 0) canCollapse++;
    else canCollapse--;
  } else if (byDuration && !(node.selfExecutions === 0 || node.selfExecutions * 5 > firstChild.selfExecutions)) {
    canCollapse = -1;
  } else {
    canCollapse = canCollapse < 0 ? -3 : 0;
  }

  for (let i = 1; i < t.length; i++) {
    const canCollapseChild = sortNode(t[i]!, mode, node);
    if (canCollapseChild < 0 && canCollapse > 0) canCollapse = -3;
  }

  if (!byDuration && parentNode !== undefined && parentNode.childExecutions > node.childExecutions * 5) {
    canCollapse = -1;
  }

  node.collapseLevels = canCollapse < -2 ? -3 - canCollapse : canCollapse > 0 ? canCollapse : 0;
  return canCollapse;
}
