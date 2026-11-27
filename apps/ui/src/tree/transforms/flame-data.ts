import type { CategoryDef, TreeNode } from '../model';

// The flame graph's input (09 §3.6): the `{name, value, children}` tree that
// d3-flame-graph lays out, built from the merged model. Each datum keeps its
// model node, so a click on a frame selects that node in the Call Tree tab,
// and the readout, the colors, and the accessible names read the node's
// numbers rather than the library's.

export interface FlameDatum {
  /** `Class.method`, without the package and the arguments. */
  name: string;
  /**
   * The node's total time in ms: the library's `value` is the frame width,
   * and with `selfValue(false)` a frame spans its children plus its own time.
   * A child is clipped to the time its parent has left, so a merged or
   * adjusted tree whose totals disagree still nests.
   */
  value: number;
  children: FlameDatum[];
  node: TreeNode;
  /** The node's own category, else the nearest ancestor's; null when none has one. */
  category: CategoryDef | null;
}

/** Looks above the root, so a subtree keeps the color it has in the whole call. */
function inheritedCategory(root: TreeNode): CategoryDef | null {
  for (let n = root.parent; n !== null; n = n.parent) {
    if (n.category !== undefined) return n.category;
  }
  return null;
}

/**
 * Builds the library's tree from `root` and its subtree. Children keep the
 * model's order, so they sit left to right in call order and the space on
 * the right of a frame is its self time. `nameOf` turns a node into the
 * frame's name; the caller caches it per method.
 */
export function toFlameData(root: TreeNode, nameOf: (node: TreeNode) => string): FlameDatum {
  const build = (node: TreeNode, value: number, inherited: CategoryDef | null): FlameDatum => {
    const category = node.category ?? inherited;
    let left = node.durationMs > 0 ? value : 0;
    const children: FlameDatum[] = [];
    for (const child of node.children) {
      const childValue = Math.min(Math.max(0, child.durationMs), left);
      left -= childValue;
      children.push(build(child, childValue, category));
    }
    return { name: nameOf(node), value, children, node, category };
  };
  // A root without a positive duration still spans the graph; its
  // descendants then get no width and are culled.
  return build(root, root.durationMs > 0 ? root.durationMs : 1, inheritedCategory(root));
}
