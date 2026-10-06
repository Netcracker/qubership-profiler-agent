import { afterEach, describe, expect, it } from 'vitest';

import { base64ToBytes, bytesToBase64, readRestorePayload, RESTORE_GLOBAL, RESTORE_VERSION } from './restore';
import type { RestorePayload } from './restore';

describe('base64 round-trip', () => {
  it('preserves arbitrary bytes', () => {
    const bytes = new Uint8Array([0, 1, 2, 64, 128, 250, 255]);
    expect(base64ToBytes(bytesToBase64(bytes))).toEqual(bytes);
  });

  it('preserves a buffer larger than the chunk size', () => {
    const bytes = new Uint8Array(0x8000 * 2 + 17);
    for (let i = 0; i < bytes.length; i += 1) bytes[i] = i % 256;
    expect(base64ToBytes(bytesToBase64(bytes))).toEqual(bytes);
  });
});

describe('readRestorePayload', () => {
  const valid: RestorePayload = {
    v: RESTORE_VERSION,
    pkPath: 'ns:svc:pod:1:2:3:0',
    tsMs: null,
    retentionClass: null,
    treeB64: 'AQID',
    adjustText: '',
    categoryText: '',
    tabs: [],
    activeTab: 'tree',
  };
  const set = (value: unknown): void => {
    (globalThis as Record<string, unknown>)[RESTORE_GLOBAL] = value;
  };
  afterEach(() => delete (globalThis as Record<string, unknown>)[RESTORE_GLOBAL]);

  it('returns null on a normal page load with no global', () => {
    expect(readRestorePayload()).toBeNull();
  });

  it('returns the payload when it is well-formed', () => {
    set(valid);
    expect(readRestorePayload()).toEqual(valid);
  });

  it('rejects a wrong version or a missing required field', () => {
    set({ ...valid, v: 999 });
    expect(readRestorePayload()).toBeNull();
    set({ ...valid, treeB64: undefined });
    expect(readRestorePayload()).toBeNull();
    set('not an object');
    expect(readRestorePayload()).toBeNull();
  });
});
