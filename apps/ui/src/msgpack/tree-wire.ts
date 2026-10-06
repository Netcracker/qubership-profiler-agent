// Decoded shape of GET /calls/{pk}/tree — merged v1 (02 §2.5.2–§2.5.3).
// Field numbers live in decode.ts/encode.ts; these are the JS-side names.

export interface TreeWire {
  /** Format version; this decoder understands 1. */
  v: number;
  /** Per-tree method dictionary; nodes index into it. */
  methods: string[];
  /** Per-tree param-key dictionary; params index into it. */
  params: string[];
  root: TreeNodeWire;
}

/** Merged node: one node aggregates all sibling invocations of one method. */
export interface TreeNodeWire {
  methodIdx: number;
  durationMs: number;
  selfDurationMs: number;
  suspensionMs: number;
  selfSuspensionMs: number;
  executions: number;
  selfExecutions: number;
  params?: ParamWire[];
  children?: TreeNodeWire[];
}

/** Aggregated param mini-tree (08 R11): values fold into groups server-side. */
export interface ParamWire {
  paramIdx: number;
  /** Ordered durationMs descending; the `::other` bucket, when present, is last. */
  groups: ParamGroupWire[];
}

export interface ParamGroupWire {
  /**
   * Representative value: first-seen full text, the literal `::other` for the
   * overflow bucket, or the `<stream>:<seq>:<offset>` reference when unresolved.
   */
  value: string;
  durationMs: number;
  executions: number;
  /** Nested params — binds under their SQL. */
  params?: ParamWire[];
  /** The value is an unresolved big-parameter reference (02 §2.5). */
  unresolved?: boolean;
}
