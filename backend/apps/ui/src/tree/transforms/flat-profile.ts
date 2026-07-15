import type { ParamWire } from '../../msgpack/tree-wire';
import type { CategoryDef, TreeModel, TreeNode } from '../model';
import { mergeParamsInto } from './params-merge';

// Flat self-time profile (old computeFlatProfile, profiler.mjs:6373).
// Aggregates per method within each business-category context; a method's
// totals are added once per non-recursive occurrence (the recursiveNodes
// guard), so recursion cannot inflate total time.

export interface MethodAggregate {
  methodIdx: number;
  selfDurationMs: number;
  selfSuspensionMs: number;
  selfExecutions: number;
  durationMs: number;
  suspensionMs: number;
  childExecutions: number;
  params: ParamWire[];
}

export interface CategoryProfile {
  /** null = no category assigned (the old 'unsorted'). */
  category: CategoryDef | null;
  /** Sorted by self duration descending; zero-self methods excluded. */
  methods: MethodAggregate[];
  totalSelfMs: number;
  /** Methods hidden because their self time is 0 (the old footer note). */
  zeroSelfCount: number;
}

interface Context {
  category: CategoryDef | null;
  recursive: Set<number>;
  byMethod: Map<number, MethodAggregate>;
}

export function computeFlatProfile(model: TreeModel): CategoryProfile[] {
  const contexts = new Map<string | null, Context>();

  const contextFor = (category: CategoryDef | undefined): Context => {
    const key = category?.name ?? null;
    let ctx = contexts.get(key);
    if (ctx === undefined) {
      ctx = { category: category ?? null, recursive: new Set(), byMethod: new Map() };
      contexts.set(key, ctx);
    }
    return ctx;
  };

  const append = (ctx: Context, node: TreeNode, regular: boolean): void => {
    let agg = ctx.byMethod.get(node.methodIdx);
    if (agg === undefined) {
      agg = {
        methodIdx: node.methodIdx,
        selfDurationMs: 0,
        selfSuspensionMs: 0,
        selfExecutions: 0,
        durationMs: 0,
        suspensionMs: 0,
        childExecutions: 0,
        params: [],
      };
      ctx.byMethod.set(node.methodIdx, agg);
    }
    agg.selfExecutions += node.selfExecutions;
    agg.selfDurationMs += node.selfDurationMs;
    agg.selfSuspensionMs += node.selfSuspensionMs;
    if (regular) {
      agg.childExecutions += node.childExecutions;
      agg.durationMs += node.durationMs;
      agg.suspensionMs += node.suspensionMs;
    } else {
      // Recursive occurrence: totals would double-count (old appendTiming).
      agg.childExecutions -= node.selfExecutions;
    }
    agg.params = mergeParamsInto(agg.params, node.params);
  };

  const visit = (node: TreeNode): void => {
    const ctx = contextFor(node.category);
    const regular = !ctx.recursive.has(node.methodIdx);
    append(ctx, node, regular);
    if (node.children.length === 0) return;
    if (regular) ctx.recursive.add(node.methodIdx);
    for (const child of node.children) visit(child);
    if (regular) ctx.recursive.delete(node.methodIdx);
  };
  visit(model.root);

  const profiles: CategoryProfile[] = [];
  for (const ctx of contexts.values()) {
    const all = [...ctx.byMethod.values()];
    const methods = all
      .filter((m) => m.selfDurationMs > 0)
      .sort(
        (a, b) =>
          b.selfDurationMs - a.selfDurationMs ||
          b.selfSuspensionMs - a.selfSuspensionMs ||
          b.durationMs - a.durationMs ||
          b.suspensionMs - a.suspensionMs ||
          b.selfExecutions - a.selfExecutions,
      );
    profiles.push({
      category: ctx.category,
      methods,
      totalSelfMs: all.reduce((sum, m) => sum + m.selfDurationMs, 0),
      zeroSelfCount: all.length - methods.length,
    });
  }
  // Heaviest categories first; uncategorised time last.
  profiles.sort((a, b) => {
    if ((a.category === null) !== (b.category === null)) return a.category === null ? 1 : -1;
    return b.totalSelfMs - a.totalSelfMs;
  });
  return profiles;
}
