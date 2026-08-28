import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import { BROWSER_ZONE } from '../controls/time-range';
import { getZone, setZone } from './timezone';

// jsdom here exposes no localStorage; stub one so persistence is observable.
beforeAll(() => {
  const data = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (k: string) => data.get(k) ?? null,
    setItem: (k: string, v: string) => void data.set(k, v),
    removeItem: (k: string) => void data.delete(k),
    clear: () => data.clear(),
    key: () => null,
    length: 0,
  });
});

afterEach(() => setZone(BROWSER_ZONE));

describe('timezone store', () => {
  it('defaults to the browser zone', () => {
    expect(getZone()).toBe(BROWSER_ZONE);
  });

  it('updates the current zone', () => {
    setZone('UTC');
    expect(getZone()).toBe('UTC');
  });

  it('persists the choice to localStorage', () => {
    setZone('Asia/Tokyo');
    expect(localStorage.getItem('profiler.timezone')).toBe('Asia/Tokyo');
  });
});
