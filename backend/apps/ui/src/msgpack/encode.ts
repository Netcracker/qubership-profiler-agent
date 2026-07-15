import type { MsgMap, MsgValue } from './decode';
import type { ParamGroupWire, ParamWire, TreeNodeWire, TreeWire } from './tree-wire';

// MessagePack encoder mirroring decode.ts. Production code never encodes a
// tree — this exists for the MSW mock and for round-trip/fuzz tests, standing
// in for the Go encoder in backend/libs/calltree/msgpack.go.

class Writer {
  private buf = new Uint8Array(1024);
  private len = 0;

  private grow(n: number): void {
    if (this.len + n <= this.buf.length) return;
    const next = new Uint8Array(Math.max(this.buf.length * 2, this.len + n));
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }

  byte(b: number): void {
    this.grow(1);
    this.buf[this.len++] = b;
  }

  bytes(bs: Uint8Array): void {
    this.grow(bs.length);
    this.buf.set(bs, this.len);
    this.len += bs.length;
  }

  u16(v: number): void {
    this.byte((v >>> 8) & 0xff);
    this.byte(v & 0xff);
  }

  u32(v: number): void {
    this.u16(Math.floor(v / 0x10000));
    this.u16(v & 0xffff);
  }

  u64(v: bigint): void {
    this.u32(Number((v >> 32n) & 0xffffffffn));
    this.u32(Number(v & 0xffffffffn));
  }

  f64(v: number): void {
    this.grow(8);
    new DataView(this.buf.buffer).setFloat64(this.len, v);
    this.len += 8;
  }

  result(): Uint8Array {
    return this.buf.slice(0, this.len);
  }
}

const utf8 = new TextEncoder();

function writeInt(w: Writer, v: number): void {
  if (!Number.isSafeInteger(v)) {
    throw new Error(`cannot encode ${v} as a MessagePack integer`);
  }
  if (v >= 0) {
    if (v <= 0x7f) return w.byte(v);
    if (v <= 0xff) {
      w.byte(0xcc);
      return w.byte(v);
    }
    if (v <= 0xffff) {
      w.byte(0xcd);
      return w.u16(v);
    }
    if (v <= 0xffffffff) {
      w.byte(0xce);
      return w.u32(v);
    }
    w.byte(0xcf);
    return w.u64(BigInt(v));
  }
  if (v >= -32) return w.byte(0x100 + v);
  if (v >= -0x80) {
    w.byte(0xd0);
    return w.byte(v & 0xff);
  }
  if (v >= -0x8000) {
    w.byte(0xd1);
    return w.u16(v & 0xffff);
  }
  if (v >= -0x80000000) {
    w.byte(0xd2);
    return w.u32(v >>> 0);
  }
  w.byte(0xd3);
  return w.u64(BigInt.asUintN(64, BigInt(v)));
}

function writeString(w: Writer, s: string): void {
  const bs = utf8.encode(s);
  if (bs.length <= 31) {
    w.byte(0xa0 | bs.length);
  } else if (bs.length <= 0xff) {
    w.byte(0xd9);
    w.byte(bs.length);
  } else if (bs.length <= 0xffff) {
    w.byte(0xda);
    w.u16(bs.length);
  } else {
    w.byte(0xdb);
    w.u32(bs.length);
  }
  w.bytes(bs);
}

function writeArrayHeader(w: Writer, n: number): void {
  if (n <= 15) {
    w.byte(0x90 | n);
  } else if (n <= 0xffff) {
    w.byte(0xdc);
    w.u16(n);
  } else {
    w.byte(0xdd);
    w.u32(n);
  }
}

function writeMapHeader(w: Writer, n: number): void {
  if (n <= 15) {
    w.byte(0x80 | n);
  } else if (n <= 0xffff) {
    w.byte(0xde);
    w.u16(n);
  } else {
    w.byte(0xdf);
    w.u32(n);
  }
}

function writeBin(w: Writer, bs: Uint8Array): void {
  if (bs.length <= 0xff) {
    w.byte(0xc4);
    w.byte(bs.length);
  } else if (bs.length <= 0xffff) {
    w.byte(0xc5);
    w.u16(bs.length);
  } else {
    w.byte(0xc6);
    w.u32(bs.length);
  }
  w.bytes(bs);
}

function writeValue(w: Writer, v: MsgValue): void {
  if (v === null) return w.byte(0xc0);
  if (typeof v === 'boolean') return w.byte(v ? 0xc3 : 0xc2);
  if (typeof v === 'number') {
    if (Number.isInteger(v)) return writeInt(w, v);
    w.byte(0xcb);
    return w.f64(v);
  }
  if (typeof v === 'string') return writeString(w, v);
  if (v instanceof Uint8Array) return writeBin(w, v);
  if (Array.isArray(v)) {
    writeArrayHeader(w, v.length);
    for (const item of v) writeValue(w, item);
    return;
  }
  writeMapHeader(w, v.size);
  for (const [key, value] of v) {
    writeValue(w, key);
    writeValue(w, value);
  }
}

/** Encodes one generic value; inverse of decode.ts readRoot. */
export function encodeValue(v: MsgValue): Uint8Array {
  const w = new Writer();
  writeValue(w, v);
  return w.result();
}

// --- TreeWire → generic value, on the §2.5.3 field numbers ---

function nodeToValue(node: TreeNodeWire): MsgMap {
  const m: MsgMap = new Map();
  m.set(0, node.methodIdx);
  m.set(1, node.durationMs);
  m.set(2, node.selfDurationMs);
  m.set(3, node.suspensionMs);
  m.set(4, node.selfSuspensionMs);
  m.set(5, node.executions);
  m.set(6, node.selfExecutions);
  if (node.params !== undefined && node.params.length > 0) {
    m.set(7, node.params.map(paramToValue));
  }
  if (node.children !== undefined && node.children.length > 0) {
    m.set(8, node.children.map(nodeToValue));
  }
  return m;
}

function paramToValue(param: ParamWire): MsgMap {
  const m: MsgMap = new Map();
  m.set(0, param.paramIdx);
  m.set(3, param.groups.map(groupToValue));
  return m;
}

function groupToValue(group: ParamGroupWire): MsgMap {
  const m: MsgMap = new Map();
  m.set(0, group.value);
  m.set(1, group.durationMs);
  m.set(2, group.executions);
  if (group.params !== undefined && group.params.length > 0) {
    m.set(3, group.params.map(paramToValue));
  }
  if (group.unresolved === true) {
    m.set(4, true);
  }
  return m;
}

export function treeToValue(tree: TreeWire): MsgMap {
  const m: MsgMap = new Map();
  m.set(0, tree.v);
  m.set(1, tree.methods as MsgValue);
  m.set(2, tree.params as MsgValue);
  m.set(3, nodeToValue(tree.root));
  return m;
}

export function encodeTree(tree: TreeWire): Uint8Array {
  return encodeValue(treeToValue(tree));
}
