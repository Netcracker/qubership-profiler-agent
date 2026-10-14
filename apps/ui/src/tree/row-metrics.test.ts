import { describe, expect, it } from 'vitest';

import { createNode } from './model';
import type { TreeNode } from './model';
import { formatShare, rowHighlight, shareBar, shareOf } from './row-metrics';

const ROOT_MS = 1000;

function node(durationMs: number, selfDurationMs: number, selfExecutions = 1): TreeNode {
  return { ...createNode(1, 0), durationMs, selfDurationMs, selfExecutions };
}

describe('rowHighlight', () => {
  it.each([
    [99, 'none'],
    [100, 'warn'],
    [249, 'warn'],
    [250, 'crit'],
  ] as const)('colors %d ms of self time in a 1 s call as %s', (selfMs, heat) => {
    expect(rowHighlight(node(1000, selfMs), ROOT_MS).self).toBe(heat);
  });

  it.each([
    [499, false],
    [500, true],
  ] as const)('makes a total of %d ms in a 1 s call bold: %s', (totalMs, bold) => {
    expect(rowHighlight(node(totalMs, 0), ROOT_MS).totalBold).toBe(bold);
  });

  it.each([
    [9, 'none'],
    [10, 'warn'],
    [99, 'warn'],
    [100, 'crit'],
  ] as const)('colors a count of %d calls as %s', (calls, heat) => {
    expect(rowHighlight(node(10, 1, calls), ROOT_MS).calls).toBe(heat);
  });

  it('judges by the share of the call, not by the absolute time', () => {
    // 300 ms is red in a 1 s call and nothing in a 10 s one.
    expect(rowHighlight(node(300, 300), 1000).self).toBe('crit');
    expect(rowHighlight(node(300, 300), 10_000).self).toBe('none');
  });

  it('highlights nothing by time when the root has no duration', () => {
    expect(rowHighlight(node(0, 0), 0)).toEqual({ totalBold: false, self: 'none', calls: 'none' });
  });
});

describe('shareBar', () => {
  it('splits the bar into the self part and the children part', () => {
    // 60% of the call, 15% of it in the method itself.
    expect(shareBar(node(600, 150), ROOT_MS)).toEqual({ selfWidth: 15, childrenWidth: 45 });
  });

  it('is all self time for a leaf', () => {
    expect(shareBar(node(400, 400), ROOT_MS)).toEqual({ selfWidth: 40, childrenWidth: 0 });
  });

  it('gives any self time at least one pixel', () => {
    expect(shareBar(node(800, 2), ROOT_MS)).toEqual({ selfWidth: 1, childrenWidth: 79 });
    expect(shareBar(node(800, 0), ROOT_MS)).toEqual({ selfWidth: 0, childrenWidth: 80 });
  });

  it('never grows past the full bar when a node outlasts the root', () => {
    expect(shareBar(node(1500, 1200), ROOT_MS)).toEqual({ selfWidth: 100, childrenWidth: 0 });
  });
});

describe('shareOf', () => {
  it('clamps to the range from 0 to 1', () => {
    expect(shareOf(250, 1000)).toBe(0.25);
    expect(shareOf(2000, 1000)).toBe(1);
    expect(shareOf(5, 0)).toBe(0);
  });

  it('prints as a whole percentage', () => {
    expect(formatShare(0.254)).toBe('25%');
    expect(formatShare(1)).toBe('100%');
  });
});
