import { describe, expect, it } from 'vitest';

import { applyLabel } from './apply-bar';

describe('applyLabel', () => {
  it('names the agent count once it is known', () => {
    expect(applyLabel(null)).toBe('Apply');
    expect(applyLabel(1)).toBe('Apply to 1 agent');
    expect(applyLabel(3)).toBe('Apply to 3 agents');
  });
});
