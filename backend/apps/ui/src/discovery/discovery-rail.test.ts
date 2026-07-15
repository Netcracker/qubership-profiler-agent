import { describe, expect, it } from 'vitest';

import { selectionEquals } from './discovery-rail';

describe('selectionEquals', () => {
  it('ignores order', () => {
    expect(
      selectionEquals(
        { services: ['a/x', 'b/y'], pods: ['a/x/1'] },
        { services: ['b/y', 'a/x'], pods: ['a/x/1'] },
      ),
    ).toBe(true);
  });

  it('detects an added or removed member', () => {
    expect(selectionEquals({ services: ['a/x'], pods: [] }, { services: ['a/x', 'b/y'], pods: [] })).toBe(false);
    expect(selectionEquals({ services: [], pods: ['a/x/1'] }, { services: [], pods: [] })).toBe(false);
  });

  it('treats two empty selections as equal', () => {
    expect(selectionEquals({ services: [], pods: [] }, { services: [], pods: [] })).toBe(true);
  });
});
