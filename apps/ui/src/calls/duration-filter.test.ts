import { describe, expect, it } from 'vitest';

import { formatDurationFilter, parseDurationFilter } from './duration-filter';

describe('parseDurationFilter', () => {
  it('reads a bare lower bound, defaulting the unit to seconds', () => {
    expect(parseDurationFilter('3')).toEqual({ minMs: 3000, maxMs: null });
    expect(parseDurationFilter('3s')).toEqual({ minMs: 3000, maxMs: null });
    expect(parseDurationFilter('99ms')).toEqual({ minMs: 99, maxMs: null });
  });

  it('treats a leading > or >= as a lower bound', () => {
    expect(parseDurationFilter('>400ms')).toEqual({ minMs: 400, maxMs: null });
    expect(parseDurationFilter('>=400ms')).toEqual({ minMs: 400, maxMs: null });
  });

  it('treats a leading < or <= as an upper bound', () => {
    expect(parseDurationFilter('<100ms')).toEqual({ minMs: null, maxMs: 100 });
    expect(parseDurationFilter('<=100ms')).toEqual({ minMs: null, maxMs: 100 });
  });

  it('treats = as an exact match', () => {
    expect(parseDurationFilter('=500ms')).toEqual({ minMs: 500, maxMs: 500 });
  });

  it('reads a range and orders the bounds', () => {
    expect(parseDurationFilter('100ms..200ms')).toEqual({ minMs: 100, maxMs: 200 });
    expect(parseDurationFilter('200ms..100ms')).toEqual({ minMs: 100, maxMs: 200 });
    expect(parseDurationFilter('1s..2s')).toEqual({ minMs: 1000, maxMs: 2000 });
  });

  it('rounds fractional milliseconds and tolerates whitespace', () => {
    expect(parseDurationFilter(' > 1.5 s ')).toEqual({ minMs: 1500, maxMs: null });
    expect(parseDurationFilter('2.5ms')).toEqual({ minMs: 3, maxMs: null });
  });

  it('maps an empty string to an unbounded filter', () => {
    expect(parseDurationFilter('')).toEqual({ minMs: null, maxMs: null });
    expect(parseDurationFilter('   ')).toEqual({ minMs: null, maxMs: null });
  });

  it('rejects text the grammar does not accept', () => {
    expect(parseDurationFilter('fast')).toBeNull();
    expect(parseDurationFilter('>')).toBeNull();
    expect(parseDurationFilter('100ms..')).toBeNull();
    expect(parseDurationFilter('10m')).toBeNull();
  });
});

describe('formatDurationFilter', () => {
  it('round-trips the bound shapes back to editable text', () => {
    expect(formatDurationFilter({ minMs: null, maxMs: null })).toBe('');
    expect(formatDurationFilter({ minMs: 400, maxMs: null })).toBe('>400ms');
    expect(formatDurationFilter({ minMs: null, maxMs: 100 })).toBe('<100ms');
    expect(formatDurationFilter({ minMs: 500, maxMs: 500 })).toBe('=500ms');
    expect(formatDurationFilter({ minMs: 100, maxMs: 200 })).toBe('100ms..200ms');
  });

  it('renders whole seconds in seconds', () => {
    expect(formatDurationFilter({ minMs: 3000, maxMs: null })).toBe('>3s');
    expect(formatDurationFilter({ minMs: 1500, maxMs: null })).toBe('>1500ms');
  });
});
