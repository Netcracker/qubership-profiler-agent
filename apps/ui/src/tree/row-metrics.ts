import type { TreeNode } from './model';

// What a tree row derives from its numbers (09 §3.3): which of them stand out,
// and the two parts of the share bar. Everything is a share of the root call,
// never an absolute time: 4 s is normal for one call and a disaster for another.

/** Self time is orange from this share of the call. */
export const SELF_WARN_SHARE = 0.1;
/** Self time is red from this share of the call. */
export const SELF_CRIT_SHARE = 0.25;
/** A total is bold from this share of the call. */
export const TOTAL_BOLD_SHARE = 0.5;
/** A call count is orange from this many calls. */
export const CALLS_WARN = 10;
/** A call count is red from this many calls. */
export const CALLS_CRIT = 100;

/** Width of the share bar in pixels; a full bar is the whole call. */
export const SHARE_BAR_WIDTH = 100;

export type Heat = 'none' | 'warn' | 'crit';

export interface RowHighlight {
  totalBold: boolean;
  self: Heat;
  calls: Heat;
}

/**
 * The share of the call that `ms` takes, from 0 to 1. A merged or adjusted tree
 * can hold a node longer than its root, so the result is clamped. A root
 * without a duration gives every node a share of 0.
 */
export function shareOf(ms: number, rootDurationMs: number): number {
  if (rootDurationMs <= 0 || ms <= 0) return 0;
  return Math.min(1, ms / rootDurationMs);
}

export function rowHighlight(node: TreeNode, rootDurationMs: number): RowHighlight {
  const selfShare = shareOf(node.selfDurationMs, rootDurationMs);
  const calls = node.selfExecutions;
  return {
    totalBold: shareOf(node.durationMs, rootDurationMs) >= TOTAL_BOLD_SHARE,
    self: selfShare >= SELF_CRIT_SHARE ? 'crit' : selfShare >= SELF_WARN_SHARE ? 'warn' : 'none',
    calls: calls >= CALLS_CRIT ? 'crit' : calls >= CALLS_WARN ? 'warn' : 'none',
  };
}

export interface ShareBar {
  /** The solid part: the method's own time. */
  selfWidth: number;
  /** The light part: time in the methods it calls. */
  childrenWidth: number;
}

/**
 * The two widths of the share bar, in pixels. Together they are the node's
 * share of the call. Any self time gets at least one pixel, so a method that
 * does a little work of its own never looks like a pure pass-through.
 */
export function shareBar(node: TreeNode, rootDurationMs: number): ShareBar {
  const totalWidth = Math.round(shareOf(node.durationMs, rootDurationMs) * SHARE_BAR_WIDTH);
  const selfWidth = Math.max(
    node.selfDurationMs > 0 ? 1 : 0,
    Math.round(shareOf(node.selfDurationMs, rootDurationMs) * SHARE_BAR_WIDTH),
  );
  return { selfWidth, childrenWidth: Math.max(0, totalWidth - selfWidth) };
}

/** A share as a whole percentage, the way the row prints it. */
export function formatShare(share: number): string {
  return `${Math.round(share * 100)}%`;
}
