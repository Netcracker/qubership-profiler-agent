import type { ParamGroupWire, ParamWire, TreeNodeWire, TreeWire } from './tree-wire';

// Hand-written MessagePack decoder for the /tree response (02 §2.5). The
// contract deliberately avoids a schema toolchain: every record is a
// Map<int, value> with documented field numbers, and unknown int keys MUST be
// ignored (forward compatibility, §2.5.1). A generic-value layer reads any
// well-formed MessagePack, so a skipped unknown field can be of any future
// type; the typed layer on top maps field numbers to the merged-v1 model.
//
// Robustness rules (mirrors backend/libs/calltree/msgpack.go):
// - every failure throws MsgpackDecodeError — corrupted input never panics,
//   hangs, or over-allocates;
// - header-declared lengths are capped by the bytes actually remaining;
// - recursion depth is bounded;
// - trailing bytes after the root value are an error.

export class MsgpackDecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MsgpackDecodeError';
  }
}

/** Generic MessagePack value. Ext payloads surface as opaque Uint8Array. */
export type MsgValue = null | boolean | number | string | Uint8Array | MsgValue[] | MsgMap;
export type MsgMap = Map<number | string, MsgValue>;

const MAX_DEPTH = 128;

class Reader {
  private readonly view: DataView;
  private pos = 0;

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  get remaining(): number {
    return this.bytes.length - this.pos;
  }

  private need(n: number): void {
    if (this.remaining < n) {
      throw new MsgpackDecodeError(`unexpected end of payload at offset ${this.pos}: need ${n} more bytes`);
    }
  }

  u8(): number {
    this.need(1);
    return this.view.getUint8(this.pos++);
  }

  u16(): number {
    this.need(2);
    const v = this.view.getUint16(this.pos);
    this.pos += 2;
    return v;
  }

  u32(): number {
    this.need(4);
    const v = this.view.getUint32(this.pos);
    this.pos += 4;
    return v;
  }

  i8(): number {
    this.need(1);
    return this.view.getInt8(this.pos++);
  }

  i16(): number {
    this.need(2);
    const v = this.view.getInt16(this.pos);
    this.pos += 2;
    return v;
  }

  i32(): number {
    this.need(4);
    const v = this.view.getInt32(this.pos);
    this.pos += 4;
    return v;
  }

  u64(): number {
    this.need(8);
    const v = this.view.getBigUint64(this.pos);
    this.pos += 8;
    return toSafeNumber(v);
  }

  i64(): number {
    this.need(8);
    const v = this.view.getBigInt64(this.pos);
    this.pos += 8;
    return toSafeNumber(v);
  }

  f32(): number {
    this.need(4);
    const v = this.view.getFloat32(this.pos);
    this.pos += 4;
    return v;
  }

  f64(): number {
    this.need(8);
    const v = this.view.getFloat64(this.pos);
    this.pos += 8;
    return v;
  }

  take(n: number): Uint8Array {
    this.need(n);
    const out = this.bytes.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }

  /** Guards a header-declared element count against the bytes left. */
  checkCount(n: number, what: string): void {
    // Every array element or map entry costs at least one byte on the wire.
    if (n > this.remaining) {
      throw new MsgpackDecodeError(`${what} declares ${n} entries with only ${this.remaining} bytes left`);
    }
  }
}

function toSafeNumber(v: bigint): number {
  if (v > BigInt(Number.MAX_SAFE_INTEGER) || v < -BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new MsgpackDecodeError(`integer ${v} exceeds the safe JS range`);
  }
  return Number(v);
}

const utf8 = new TextDecoder('utf-8', { fatal: false });

function readString(r: Reader, len: number): string {
  return utf8.decode(r.take(len));
}

function readArray(r: Reader, len: number, depth: number): MsgValue[] {
  r.checkCount(len, 'array');
  const out: MsgValue[] = [];
  for (let i = 0; i < len; i++) out.push(readValue(r, depth));
  return out;
}

function readMap(r: Reader, len: number, depth: number): MsgMap {
  r.checkCount(len * 2, 'map');
  const out: MsgMap = new Map();
  for (let i = 0; i < len; i++) {
    const key = readValue(r, depth);
    const value = readValue(r, depth);
    // The contract keys records by int; string keys are tolerated and left
    // for the typed layer to ignore. Any other key type marks a foreign format.
    if (typeof key === 'number' || typeof key === 'string') {
      out.set(key, value);
    } else {
      throw new MsgpackDecodeError(`map key must be an integer or string, got ${describe(key)}`);
    }
  }
  return out;
}

function readValue(r: Reader, depth: number): MsgValue {
  if (depth > MAX_DEPTH) {
    throw new MsgpackDecodeError(`nesting deeper than ${MAX_DEPTH} levels`);
  }
  const b = r.u8();
  // Positive fixint / negative fixint.
  if (b <= 0x7f) return b;
  if (b >= 0xe0) return b - 0x100;
  // Fix families.
  if (b >= 0x80 && b <= 0x8f) return readMap(r, b & 0x0f, depth + 1);
  if (b >= 0x90 && b <= 0x9f) return readArray(r, b & 0x0f, depth + 1);
  if (b >= 0xa0 && b <= 0xbf) return readString(r, b & 0x1f);
  switch (b) {
    case 0xc0:
      return null;
    case 0xc2:
      return false;
    case 0xc3:
      return true;
    case 0xc4:
      return r.take(r.u8());
    case 0xc5:
      return r.take(r.u16());
    case 0xc6:
      return r.take(r.u32());
    case 0xc7:
      return skipExt(r, r.u8());
    case 0xc8:
      return skipExt(r, r.u16());
    case 0xc9:
      return skipExt(r, r.u32());
    case 0xca:
      return r.f32();
    case 0xcb:
      return r.f64();
    case 0xcc:
      return r.u8();
    case 0xcd:
      return r.u16();
    case 0xce:
      return r.u32();
    case 0xcf:
      return r.u64();
    case 0xd0:
      return r.i8();
    case 0xd1:
      return r.i16();
    case 0xd2:
      return r.i32();
    case 0xd3:
      return r.i64();
    case 0xd4:
      return skipExt(r, 1);
    case 0xd5:
      return skipExt(r, 2);
    case 0xd6:
      return skipExt(r, 4);
    case 0xd7:
      return skipExt(r, 8);
    case 0xd8:
      return skipExt(r, 16);
    case 0xd9:
      return readString(r, r.u8());
    case 0xda:
      return readString(r, r.u16());
    case 0xdb:
      return readString(r, r.u32());
    case 0xdc:
      return readArray(r, r.u16(), depth + 1);
    case 0xdd:
      return readArray(r, r.u32(), depth + 1);
    case 0xde:
      return readMap(r, r.u16(), depth + 1);
    case 0xdf:
      return readMap(r, r.u32(), depth + 1);
    default:
      throw new MsgpackDecodeError(`reserved type byte 0x${b.toString(16)}`);
  }
}

function skipExt(r: Reader, len: number): Uint8Array {
  r.u8(); // ext type tag; opaque to this decoder
  return r.take(len);
}

/** Reads one complete MessagePack value; trailing bytes are an error. */
export function readRoot(bytes: Uint8Array): MsgValue {
  const r = new Reader(bytes);
  const value = readValue(r, 0);
  if (r.remaining > 0) {
    throw new MsgpackDecodeError(`${r.remaining} trailing bytes after the root value`);
  }
  return value;
}

function describe(v: MsgValue): string {
  if (v === null) return 'nil';
  if (Array.isArray(v)) return 'array';
  if (v instanceof Map) return 'map';
  if (v instanceof Uint8Array) return 'bin';
  return typeof v;
}

// --- Typed layer: field numbers of the merged-v1 records (02 §2.5.3) ---

const enum TreeField {
  V = 0,
  Methods = 1,
  Params = 2,
  Root = 3,
}

const enum NodeField {
  MethodIdx = 0,
  DurationMs = 1,
  SelfDurationMs = 2,
  SuspensionMs = 3,
  SelfSuspensionMs = 4,
  Executions = 5,
  SelfExecutions = 6,
  Params = 7,
  Children = 8,
}

const enum ParamField {
  ParamIdx = 0,
  // 1 and 2 are reserved: the pre-R11 flat values / unresolved lists.
  Groups = 3,
}

const enum GroupField {
  Value = 0,
  DurationMs = 1,
  Executions = 2,
  Params = 3,
  Unresolved = 4,
}

function asMap(v: MsgValue | undefined, what: string): MsgMap {
  if (!(v instanceof Map)) throw new MsgpackDecodeError(`${what} must be a map, got ${v === undefined ? 'nothing' : describe(v)}`);
  return v;
}

function requireInt(m: MsgMap, key: number, what: string): number {
  const v = m.get(key);
  if (typeof v !== 'number' || !Number.isInteger(v)) {
    throw new MsgpackDecodeError(`${what} field ${key} must be an integer, got ${v === undefined ? 'nothing' : describe(v)}`);
  }
  return v;
}

function requireString(m: MsgMap, key: number, what: string): string {
  const v = m.get(key);
  if (typeof v !== 'string') {
    throw new MsgpackDecodeError(`${what} field ${key} must be a string, got ${v === undefined ? 'nothing' : describe(v)}`);
  }
  return v;
}

function requireStringArray(m: MsgMap, key: number, what: string): string[] {
  const v = m.get(key);
  if (!Array.isArray(v)) {
    throw new MsgpackDecodeError(`${what} field ${key} must be an array, got ${v === undefined ? 'nothing' : describe(v)}`);
  }
  return v.map((item, i) => {
    if (typeof item !== 'string') {
      throw new MsgpackDecodeError(`${what} field ${key}[${i}] must be a string, got ${describe(item)}`);
    }
    return item;
  });
}

function optionalArray(m: MsgMap, key: number, what: string): MsgValue[] | undefined {
  const v = m.get(key);
  if (v === undefined) return undefined;
  if (!Array.isArray(v)) throw new MsgpackDecodeError(`${what} field ${key} must be an array, got ${describe(v)}`);
  return v;
}

/** Decodes the merged-v1 tree envelope. Unknown int keys are ignored (§2.5.1). */
export function decodeTree(bytes: Uint8Array): TreeWire {
  const top = asMap(readRoot(bytes), 'tree envelope');
  const v = requireInt(top, TreeField.V, 'tree');
  if (v !== 1) {
    throw new MsgpackDecodeError(`unsupported tree version ${v}; this decoder understands 1`);
  }
  const methods = requireStringArray(top, TreeField.Methods, 'tree');
  const params = requireStringArray(top, TreeField.Params, 'tree');
  const root = decodeNode(top.get(TreeField.Root), methods.length, params.length, 0);
  return { v, methods, params, root };
}

function decodeNode(v: MsgValue | undefined, methodCount: number, paramCount: number, depth: number): TreeNodeWire {
  if (depth > MAX_DEPTH) throw new MsgpackDecodeError(`node nesting deeper than ${MAX_DEPTH}`);
  const m = asMap(v, 'node');
  const methodIdx = requireInt(m, NodeField.MethodIdx, 'node');
  if (methodIdx < 0 || methodIdx >= methodCount) {
    throw new MsgpackDecodeError(`node methodIdx ${methodIdx} is out of the dictionary (${methodCount} methods)`);
  }
  const node: TreeNodeWire = {
    methodIdx,
    durationMs: requireInt(m, NodeField.DurationMs, 'node'),
    selfDurationMs: requireInt(m, NodeField.SelfDurationMs, 'node'),
    suspensionMs: requireInt(m, NodeField.SuspensionMs, 'node'),
    selfSuspensionMs: requireInt(m, NodeField.SelfSuspensionMs, 'node'),
    executions: requireInt(m, NodeField.Executions, 'node'),
    selfExecutions: requireInt(m, NodeField.SelfExecutions, 'node'),
  };
  const params = optionalArray(m, NodeField.Params, 'node');
  if (params !== undefined && params.length > 0) {
    node.params = params.map((p) => decodeParam(p, paramCount, depth + 1));
  }
  const children = optionalArray(m, NodeField.Children, 'node');
  if (children !== undefined && children.length > 0) {
    node.children = children.map((c) => decodeNode(c, methodCount, paramCount, depth + 1));
  }
  return node;
}

function decodeParam(v: MsgValue, paramCount: number, depth: number): ParamWire {
  if (depth > MAX_DEPTH) throw new MsgpackDecodeError(`param nesting deeper than ${MAX_DEPTH}`);
  const m = asMap(v, 'param');
  const paramIdx = requireInt(m, ParamField.ParamIdx, 'param');
  if (paramIdx < 0 || paramIdx >= paramCount) {
    throw new MsgpackDecodeError(`paramIdx ${paramIdx} is out of the dictionary (${paramCount} params)`);
  }
  const groupsRaw = m.get(ParamField.Groups);
  if (!Array.isArray(groupsRaw)) {
    throw new MsgpackDecodeError(`param field ${ParamField.Groups} (groups) must be an array`);
  }
  return {
    paramIdx,
    groups: groupsRaw.map((g) => decodeGroup(g, paramCount, depth + 1)),
  };
}

function decodeGroup(v: MsgValue, paramCount: number, depth: number): ParamGroupWire {
  if (depth > MAX_DEPTH) throw new MsgpackDecodeError(`param group nesting deeper than ${MAX_DEPTH}`);
  const m = asMap(v, 'param group');
  const group: ParamGroupWire = {
    value: requireString(m, GroupField.Value, 'param group'),
    durationMs: requireInt(m, GroupField.DurationMs, 'param group'),
    executions: requireInt(m, GroupField.Executions, 'param group'),
  };
  const nested = optionalArray(m, GroupField.Params, 'param group');
  if (nested !== undefined && nested.length > 0) {
    group.params = nested.map((p) => decodeParam(p, paramCount, depth + 1));
  }
  const unresolved = m.get(GroupField.Unresolved);
  if (unresolved !== undefined) {
    if (typeof unresolved !== 'boolean') {
      throw new MsgpackDecodeError(`param group field ${GroupField.Unresolved} (unresolved) must be a boolean`);
    }
    if (unresolved) group.unresolved = true;
  }
  return group;
}
