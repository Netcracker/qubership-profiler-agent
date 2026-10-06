import { describe, expect, it } from 'vitest';

import { MsgpackDecodeError, decodeTree, readRoot } from './decode';
import type { MsgMap } from './decode';
import { encodeTree, encodeValue, treeToValue } from './encode';
import type { TreeWire } from './tree-wire';

// A representative merged-v1 tree: nested children, params with groups,
// binds nested under their SQL, an ::other bucket, and an unresolved ref.
const FIXTURE: TreeWire = {
  v: 1,
  methods: [
    'void com.example.Service.handle(HttpRequest) (Service.java:42) [app.jar]',
    'ResultSet org.example.Dao.query(String) (Dao.java:10) [dao.jar]',
    'void java.lang.Thread.run() (Thread.java:833) [rt.jar]',
  ],
  params: ['sql', 'binds', 'request.id'],
  root: {
    methodIdx: 2,
    durationMs: 1247,
    selfDurationMs: 100,
    suspensionMs: 20,
    selfSuspensionMs: 5,
    executions: 44,
    selfExecutions: 1,
    params: [{ paramIdx: 2, groups: [{ value: 'abc123', durationMs: 1247, executions: 1 }] }],
    children: [
      {
        methodIdx: 0,
        durationMs: 1147,
        selfDurationMs: 47,
        suspensionMs: 15,
        selfSuspensionMs: 0,
        executions: 43,
        selfExecutions: 1,
        children: [
          {
            methodIdx: 1,
            durationMs: 1100,
            selfDurationMs: 1100,
            suspensionMs: 15,
            selfSuspensionMs: 15,
            executions: 42,
            selfExecutions: 42,
            params: [
              {
                paramIdx: 0,
                groups: [
                  {
                    value: 'select * from orders where id = ?',
                    durationMs: 900,
                    executions: 40,
                    params: [{ paramIdx: 1, groups: [{ value: '42', durationMs: 900, executions: 40 }] }],
                  },
                  { value: '::other', durationMs: 200, executions: 2 },
                  { value: 'sql:17:4096', durationMs: 50, executions: 1, unresolved: true },
                ],
              },
            ],
          },
        ],
      },
    ],
  },
};

describe('decodeTree', () => {
  it('round-trips the fixture through the mirror encoder', () => {
    expect(decodeTree(encodeTree(FIXTURE))).toEqual(FIXTURE);
  });

  it('ignores unknown int keys at every record level (02 §2.5.1)', () => {
    const top = treeToValue(FIXTURE);
    top.set(99, 'future field');
    const node = top.get(3) as MsgMap;
    node.set(9, 12345); // e.g. a future cpuMs
    node.set(1000, [new Map<number, never>()]);
    const param = (node.get(7) as MsgMap[])[0]!;
    param.set(17, null);
    const group = (param.get(3) as MsgMap[])[0]!;
    group.set(5, true);
    expect(decodeTree(encodeValue(top))).toEqual(FIXTURE);
  });

  it('tolerates string map keys by ignoring them', () => {
    const top = treeToValue(FIXTURE);
    top.set('vendor-extension', true);
    expect(decodeTree(encodeValue(top))).toEqual(FIXTURE);
  });

  it('rejects an unsupported version', () => {
    const top = treeToValue(FIXTURE);
    top.set(0, 2);
    expect(() => decodeTree(encodeValue(top))).toThrow(/unsupported tree version 2/);
  });

  it('rejects a node missing a required field', () => {
    const top = treeToValue(FIXTURE);
    (top.get(3) as MsgMap).delete(1); // durationMs
    expect(() => decodeTree(encodeValue(top))).toThrow(MsgpackDecodeError);
  });

  it('rejects methodIdx outside the per-tree dictionary', () => {
    const top = treeToValue(FIXTURE);
    (top.get(3) as MsgMap).set(0, FIXTURE.methods.length);
    expect(() => decodeTree(encodeValue(top))).toThrow(/methodIdx/);
  });

  it('rejects paramIdx outside the per-tree dictionary', () => {
    const top = treeToValue(FIXTURE);
    const param = ((top.get(3) as MsgMap).get(7) as MsgMap[])[0]!;
    param.set(0, FIXTURE.params.length);
    expect(() => decodeTree(encodeValue(top))).toThrow(/paramIdx/);
  });

  it('rejects a float where an integer is required', () => {
    const top = treeToValue(FIXTURE);
    (top.get(3) as MsgMap).set(1, 12.5);
    expect(() => decodeTree(encodeValue(top))).toThrow(MsgpackDecodeError);
  });

  it('rejects trailing bytes after the root value', () => {
    const bytes = encodeTree(FIXTURE);
    const padded = new Uint8Array(bytes.length + 1);
    padded.set(bytes);
    expect(() => decodeTree(padded)).toThrow(/trailing/);
  });

  it('rejects a truncated payload', () => {
    const bytes = encodeTree(FIXTURE);
    expect(() => decodeTree(bytes.subarray(0, bytes.length - 3))).toThrow(MsgpackDecodeError);
  });

  it('rejects an empty payload', () => {
    expect(() => decodeTree(new Uint8Array(0))).toThrow(MsgpackDecodeError);
  });

  it('caps header-declared lengths by the bytes remaining', () => {
    // array32 claiming 2^31 elements in a 6-byte payload must fail fast
    // instead of preallocating.
    const bytes = new Uint8Array([0xdd, 0x80, 0x00, 0x00, 0x00, 0x01]);
    expect(() => readRoot(bytes)).toThrow(/declares/);
  });

  it('bounds recursion depth instead of overflowing the stack', () => {
    // 200 nested fixarray(1) headers, then a nil.
    const bytes = new Uint8Array(201);
    bytes.fill(0x91, 0, 200);
    bytes[200] = 0xc0;
    expect(() => readRoot(bytes)).toThrow(/nesting deeper/);
  });

  it('ignores an unknown int64 field beyond the safe JS range (02 §2.5.1)', () => {
    // Forward compat: a future backend appends an int64 field the UI does not
    // know, and its value exceeds 2^53. The decode must skip it silently and
    // still return the known fields — not throw in the generic layer.
    const top = treeToValue(FIXTURE);
    top.set(42, 2n ** 60n); // unknown field, value past the safe JS range
    const node = top.get(3) as MsgMap;
    node.set(50, -(2n ** 62n)); // an unknown negative int64 too
    expect(decodeTree(encodeValue(top))).toEqual(FIXTURE);
  });

  it('rejects a known field whose int64 exceeds the safe JS range', () => {
    // The range check moved to the known-field reader: a durationMs past 2^53
    // is genuine data corruption, not forward compat, so it must still throw.
    const top = treeToValue(FIXTURE);
    (top.get(3) as MsgMap).set(1, 2n ** 60n); // node durationMs
    expect(() => decodeTree(encodeValue(top))).toThrow(/safe JS range/);
  });
});

describe('generic value layer', () => {
  it('round-trips integer edge cases', () => {
    for (const v of [0, 1, 127, 128, 255, 256, 65535, 65536, 2 ** 32 - 1, 2 ** 32, Number.MAX_SAFE_INTEGER, -1, -32, -33, -128, -129, -32768, -32769, -(2 ** 31), -(2 ** 31) - 1, -Number.MAX_SAFE_INTEGER]) {
      expect(readRoot(encodeValue(v)), `value ${v}`).toBe(v);
    }
  });

  it('round-trips strings across length encodings', () => {
    for (const len of [0, 31, 32, 255, 256, 70000]) {
      const s = 'π'.repeat(len);
      expect(readRoot(encodeValue(s))).toBe(s);
    }
  });

  it('carries a 64-bit integer beyond the safe range as a bigint, without throwing', () => {
    // uint64 2^63 and int64 -2^63: too large for a JS number, so the generic
    // layer surfaces them as bigints for the typed layer to skip or reject.
    expect(readRoot(new Uint8Array([0xcf, 0x80, 0, 0, 0, 0, 0, 0, 0]))).toBe(2n ** 63n);
    expect(readRoot(new Uint8Array([0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0]))).toBe(-(2n ** 63n));
    // A value that fits the safe range still narrows to a number.
    expect(readRoot(new Uint8Array([0xcf, 0, 0, 0, 0, 0, 0, 0, 1]))).toBe(1);
  });
});
