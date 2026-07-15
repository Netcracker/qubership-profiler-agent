import { formatDurationMs } from '../calls/format';
import { totalExecutions } from './model';
import type { TreeModel, TreeNode } from './model';
import type { ParamWire } from '../msgpack/tree-wire';

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

/** Matches a node's own param rows: key names and group values, binds included. */
function paramsMatch(params: readonly ParamWire[], paramKeys: readonly string[], needle: string): boolean {
  return params.some((param) => {
    const key = (paramKeys[param.paramIdx] ?? '').toLowerCase();
    if (key.includes(needle)) return true;
    return param.groups.some(
      (group) => group.value.toLowerCase().includes(needle) || paramsMatch(group.params ?? [], paramKeys, needle),
    );
  });
}

export function searchTree(model: TreeModel, query: string): TreeSearchResult | null {
  const needle = query.trim().toLowerCase();
  if (needle === '') return null;
  const matched = new Set<number>();
  const expand = new Set<number>();

  const visit = (node: TreeNode): boolean => {
    const method = model.methods[node.methodIdx] ?? '';
    const paramMatch = paramsMatch(node.params, model.paramKeys, needle);
    const selfMatch = rowMatches(node, method, needle) || paramMatch;
    if (selfMatch) matched.add(node.id);
    // A param match sits in this node's own row list, not a descendant node,
    // so this node needs expanding too, not just its ancestors.
    if (paramMatch) expand.add(node.id);
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
