import type { ParamGroupWire, ParamWire } from '../../msgpack/tree-wire';
import type { TreeModel, TreeNode } from '../model';
import { sortNode } from '../sort-node';

// Adjust duration (old Tree__adjustDuration_updateValue /
// Tree__makeAdjustments): config lines are `<factor> <method-pattern>`,
// where factor is a number or a fraction (`1/100`), '*' wildcards, '#'
// comments. A matched node scales itself and cascades the factor down its
// whole subtree; ancestor totals recompute from the scaled selves. "What if
// this call were 10× faster" (07 §5.3).
//
// One deliberate deviation from the source: the old walk left M_DURATION
// undefined on adjusted nodes (`var newDuration;` was never assigned —
// a latent bug). The port computes the obvious intent, child duration plus
// the scaled self.

export interface AdjustRule {
  factor: number;
  pattern: RegExp;
  patternLength: number;
}

function wildcardToRegExp(ref: string): RegExp {
  const escaped = ref.split('*').map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  return new RegExp(escaped.join('.*'));
}

export function parseAdjustConfig(text: string): AdjustRule[] {
  const rules: AdjustRule[] = [];
  for (const lineRaw of text.split('\n')) {
    const line = lineRaw.trim();
    if (line === '' || line.startsWith('#')) continue;
    const m = /(\S+)\s+(.+\S)/.exec(line);
    if (m === null) continue;
    let factor = Number(m[1]);
    if (Number.isNaN(factor)) {
      const frac = /([^/]+)\/(.+)/.exec(m[1]!);
      if (frac === null) continue;
      factor = Number(frac[1]) / Number(frac[2]);
    }
    if (Number.isNaN(factor)) continue;
    rules.push({ factor, pattern: wildcardToRegExp(m[2]!), patternLength: m[2]!.length });
  }
  // Ascending by pattern length, so the longest match assigns last and wins
  // (the old parse kept the same order).
  rules.sort((a, b) => a.patternLength - b.patternLength);
  return rules;
}

/** Resolves the per-method factor map (old Tree__adjustDuration_parsed). */
export function factorByMethod(model: TreeModel, rules: readonly AdjustRule[]): Map<number, number> {
  const map = new Map<number, number>();
  for (const rule of rules) {
    model.methods.forEach((method, idx) => {
      if (rule.pattern.test(method)) map.set(idx, rule.factor);
    });
  }
  return map;
}

/**
 * Scales matched subtrees in place and recomputes ancestor totals
 * (old Tree__makeAdjustments). Safe to re-apply with new rules: original
 * self values stash in prevSelf* on first touch.
 */
export function applyAdjustments(model: TreeModel, factors: ReadonlyMap<number, number>): void {
  let duration = 0;
  let suspension = 0;
  let calls = 0;
  let adjusted = 0;

  const walk = (node: TreeNode, k: number): void => {
    const startDuration = duration;
    const startSuspension = suspension;
    const startCalls = calls;
    const startAdjusted = adjusted;

    const factor = factors.get(node.methodIdx);
    if (factor !== undefined) {
      k *= factor;
      adjusted++;
    }

    for (let i = node.children.length - 1; i >= 0; i--) walk(node.children[i]!, k);

    const firstUpdate = node.prevSelfDurationMs === undefined;
    if (adjusted === startAdjusted && firstUpdate && k === 1) {
      duration += node.selfDurationMs;
      suspension += node.selfSuspensionMs;
      calls += node.selfExecutions;
      return;
    }

    const childDuration = duration - startDuration;
    const childSuspension = suspension - startSuspension;
    const childCalls = calls - startCalls;

    if (firstUpdate) {
      node.prevSelfDurationMs = node.selfDurationMs;
      node.prevSelfSuspensionMs = node.selfSuspensionMs;
      node.prevSelfExecutions = node.selfExecutions;
    }

    const newSelfDuration = node.prevSelfDurationMs! * k;
    const newSelfSuspension = (node.prevSelfSuspensionMs ?? 0) * k;
    const newExecutions = (node.prevSelfExecutions ?? 0) * k;
    const newDuration = childDuration + newSelfDuration;
    const newSuspension = childSuspension + newSelfSuspension;

    duration += newSelfDuration;
    suspension += newSelfSuspension;

    node.durationMs = newDuration;
    node.selfDurationMs = newSelfDuration;
    node.suspensionMs = newSuspension;
    node.selfSuspensionMs = newSelfSuspension;
    node.selfExecutions = newExecutions;
    node.childExecutions = childCalls;

    calls += newExecutions;

    // Old kTags scaling: param groups shrink with the node they annotate,
    // proportionally to the node's new (duration + suspension).
    if (node.params.length > 0) {
      let prevTotal = 0;
      for (const param of node.params) {
        for (const group of param.groups) prevTotal += prevGroupDuration(group);
      }
      if (prevTotal !== 0) {
        scaleParams(node.params, (newDuration + newSuspension) / prevTotal);
      }
    }
  };

  walk(model.root, 1);
  sortNode(model.root, 'duration');
}

// First-touch stash of the recorded group durations (old P_PREV_DURATION),
// so re-applying with different factors scales from the originals.
const groupPrevDurationMs = new WeakMap<ParamGroupWire, number>();

function prevGroupDuration(group: ParamGroupWire): number {
  let prev = groupPrevDurationMs.get(group);
  if (prev === undefined) {
    prev = group.durationMs;
    groupPrevDurationMs.set(group, prev);
  }
  return prev;
}

function scaleParams(params: readonly ParamWire[], kTags: number): void {
  for (const param of params) {
    for (const group of param.groups) {
      group.durationMs = Math.round(prevGroupDuration(group) * kTags);
      if (group.params !== undefined) scaleParams(group.params, kTags);
    }
  }
}
