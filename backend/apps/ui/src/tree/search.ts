import { formatDurationMs } from '../calls/format';
import { totalExecutions } from './model';
import type { TreeModel, TreeNode } from './model';

// Search within the tree, ported from the draft's framework-agnostic
// search-elements.ts: a node matches when the query occurs in its title or
// in the formatted numbers a user sees on the row; every ancestor of a match
// expands so the result is reachable.

export interface TreeSearchResult {
  matched: ReadonlySet<number>;
  /** Ancestors (and matched inner nodes) to force-expand. */
  expand: ReadonlySet<number>;
  matchCount: number;
}

function rowMatches(node: TreeNode, method: string, needle: string): boolean {
  const texts = [
    method,
    formatDurationMs(node.durationMs),
    formatDurationMs(node.selfDurationMs),
    String(node.suspensionMs),
    String(node.selfSuspensionMs),
    String(node.selfExecutions),
    String(totalExecutions(node)),
  ];
  return texts.some((t) => t.toLowerCase().includes(needle));
}

export function searchTree(model: TreeModel, query: string): TreeSearchResult | null {
  const needle = query.trim().toLowerCase();
  if (needle === '') return null;
  const matched = new Set<number>();
  const expand = new Set<number>();

  const visit = (node: TreeNode): boolean => {
    const method = model.methods[node.methodIdx] ?? '';
    const selfMatch = rowMatches(node, method, needle);
    if (selfMatch) matched.add(node.id);
    let descendantMatch = false;
    for (const child of node.children) {
      if (visit(child)) descendantMatch = true;
    }
    if (descendantMatch) expand.add(node.id);
    return selfMatch || descendantMatch;
  };

  visit(model.root);
  return { matched, expand, matchCount: matched.size };
}
