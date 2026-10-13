import type { CategoryDef, TreeNode } from '../model';

// Flame graph layout: one block per node, placed under its parent. Offsets
// and widths are shares (0..1) of the root's total duration, so the view
// multiplies by its pixel width and never needs the durations. Children
// follow the model's child order, left to right from the parent's left
// edge; the space they leave on the right is the parent's self time.

export interface FlameBlock {
  node: TreeNode;
  /** Left edge, as a share of the root's duration. */
  offset: number;
  /** Width, as a share of the root's duration. */
  width: number;
  /** Row below the root; the root is 0, also after re-rooting. */
  depth: number;
  /** The node's own category, else the nearest ancestor's; null when none has one. */
  category: CategoryDef | null;
}

export interface FlameLayout {
  /** Pre-order: a block precedes the blocks of its subtree. */
  blocks: FlameBlock[];
  /** Rows the blocks occupy: the deepest block's depth plus one. */
  rows: number;
}

export interface FlameOptions {
  /**
   * Blocks narrower than this share are left out together with their
   * subtrees. The caller passes the share of one pixel (1 / width in px).
   * Space of a culled block stays reserved, so its siblings do not move.
   */
  minWidth?: number;
}

/** Looks above the root, so a re-rooted graph keeps the color it had in the whole call. */
function inheritedCategory(root: TreeNode): CategoryDef | null {
  for (let n = root.parent; n !== null; n = n.parent) {
    if (n.category !== undefined) return n.category;
  }
  return null;
}

/**
 * Lays out the subtree of `root` as flame graph blocks. Pass the tree's root
 * for the whole call, or any node to re-root: that node then spans the full
 * width and sits at depth 0.
 *
 * A child is clipped to the width its parent has left, so a tree whose
 * totals disagree (a merged or adjusted one) still nests. A root without a
 * positive duration spans the full width and gives its descendants no width.
 */
export function flameLayout(root: TreeNode, options: FlameOptions = {}): FlameLayout {
  const minWidth = options.minWidth ?? 0;
  const perMs = root.durationMs > 0 ? 1 / root.durationMs : 0;
  const blocks: FlameBlock[] = [];
  let rows = 0;

  const place = (node: TreeNode, depth: number, offset: number, width: number, inherited: CategoryDef | null): void => {
    if (width < minWidth) return;
    const category = node.category ?? inherited;
    blocks.push({ node, offset, width, depth, category });
    if (depth >= rows) rows = depth + 1;
    const end = offset + width;
    let cursor = offset;
    for (const child of node.children) {
      const share = child.durationMs > 0 ? child.durationMs * perMs : 0;
      const childWidth = Math.min(share, Math.max(0, end - cursor));
      place(child, depth + 1, cursor, childWidth, category);
      cursor += childWidth;
    }
  };
  place(root, 0, 0, 1, inheritedCategory(root));

  return { blocks, rows };
}
