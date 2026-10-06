import type { ParamGroupWire } from '../msgpack/tree-wire';
import type { TreeModel, TreeNode } from './model';
import { isSqlParamKey } from './param-value-viewer';
import { sqlSignature } from './sql-signature';

// The Parameters tab summarises the whole tree as a mini-tree (09 §3.3, 07
// §5.3), the same shape tree-view.tsx already renders per node: metadata
// rows flat, `sql` groups aggregated by normalised signature with binds
// nested under their SQL, top-N shapes by cumulative time plus an `::other`
// bucket for the rest.
//
// The backend's per-node aggregation (calltree/params.go) decides SQL-ness
// from the wire's PARAM_BIG_DEDUP type, not the param's dictionary word (02
// §2.5.3 note); that flag does not ride the wire, so this cross-node
// re-aggregation instead keys on the word itself (isSqlParamKey), same as
// the full-value viewer already does.
const OTHER_VALUE = '::other';

export const DEFAULT_TOP_SQL_GROUPS = 20;

export interface ParamValueStat {
  keyIdx: number;
  value: string;
  durationMs: number;
  executions: number;
  unresolved: boolean;
  /** How many (node, pre-signature value) contributions folded into this row. */
  nodes: number;
}

export interface SqlGroupStat extends ParamValueStat {
  /** Bind values nested under this SQL shape, grouped the same way (own signature). */
  binds: ParamValueStat[];
}

export interface ParamSummary {
  /** Non-SQL params (node.name, java.thread, web.method, …), flat, by (key, value). */
  metadata: ParamValueStat[];
  /** SQL shapes ordered by cumulative durationMs descending; `::other` last, when present. */
  sql: SqlGroupStat[];
}

function newStat(keyIdx: number, value: string): ParamValueStat {
  return { keyIdx, value, durationMs: 0, executions: 0, unresolved: false, nodes: 0 };
}

function addGroup(stat: ParamValueStat, group: ParamGroupWire): void {
  stat.durationMs += group.durationMs;
  stat.executions += group.executions;
  stat.nodes += 1;
  if (group.unresolved === true) stat.unresolved = true;
}

// Groups a value the way the backend's per-node aggregation does (params.go
// groupOf): by its normalised signature, except the `::other` sentinel and
// unresolved big-parameter references, which stay their own group — an
// unresolved reference's signature would be meaningless noise, and two
// distinct references must not collapse into one.
function groupKey(group: ParamGroupWire): string {
  if (group.value === OTHER_VALUE) return OTHER_VALUE;
  if (group.unresolved === true) return `#${group.value}`;
  return sqlSignature(group.value);
}

export function summariseParams(model: TreeModel, topSqlGroups = DEFAULT_TOP_SQL_GROUPS): ParamSummary {
  const metadata = new Map<string, ParamValueStat>();
  const sql = new Map<string, { stat: ParamValueStat; binds: Map<string, ParamValueStat> }>();

  const addMetadata = (keyIdx: number, groups: readonly ParamGroupWire[]): void => {
    for (const group of groups) {
      const mapKey = JSON.stringify([keyIdx, group.value]);
      const stat = metadata.get(mapKey);
      if (stat === undefined) {
        const created = newStat(keyIdx, group.value);
        addGroup(created, group);
        metadata.set(mapKey, created);
      } else {
        addGroup(stat, group);
      }
    }
  };

  const addBinds = (binds: Map<string, ParamValueStat>, keyIdx: number, groups: readonly ParamGroupWire[]): void => {
    for (const group of groups) {
      const key = groupKey(group);
      const stat = binds.get(key);
      if (stat === undefined) {
        const created = newStat(keyIdx, group.value);
        addGroup(created, group);
        binds.set(key, created);
      } else {
        addGroup(stat, group);
      }
    }
  };

  const addSql = (keyIdx: number, groups: readonly ParamGroupWire[]): void => {
    for (const group of groups) {
      const key = groupKey(group);
      let entry = sql.get(key);
      if (entry === undefined) {
        entry = { stat: newStat(keyIdx, group.value), binds: new Map() };
        sql.set(key, entry);
      }
      addGroup(entry.stat, group);
      for (const nested of group.params ?? []) addBinds(entry.binds, nested.paramIdx, nested.groups);
    }
  };

  const visit = (node: TreeNode): void => {
    for (const param of node.params) {
      if (isSqlParamKey(model.paramKeys[param.paramIdx] ?? '')) addSql(param.paramIdx, param.groups);
      else addMetadata(param.paramIdx, param.groups);
    }
    node.children.forEach(visit);
  };
  visit(model.root);

  const sqlStats: SqlGroupStat[] = [...sql.values()]
    .map(({ stat, binds }) => ({ ...stat, binds: [...binds.values()].sort((a, b) => b.durationMs - a.durationMs) }))
    .sort((a, b) => b.durationMs - a.durationMs);

  // The node-level `::other` bucket (already folded server-side, R11) is
  // pulled out before the top-N cut, then merged with whatever this cut
  // itself pushes past the cap — one overflow bucket, not two.
  const otherFromNodes = sqlStats.find((s) => s.value === OTHER_VALUE);
  const shapesOnly = sqlStats.filter((s) => s.value !== OTHER_VALUE);
  const shapes = shapesOnly.slice(0, topSqlGroups);
  const overflow = shapesOnly.slice(topSqlGroups);
  if (overflow.length > 0 || otherFromNodes !== undefined) {
    const keyIdx = shapes[0]?.keyIdx ?? otherFromNodes?.keyIdx ?? overflow[0]?.keyIdx ?? -1;
    const other = newStat(keyIdx, OTHER_VALUE);
    if (otherFromNodes !== undefined) {
      other.durationMs += otherFromNodes.durationMs;
      other.executions += otherFromNodes.executions;
      other.nodes += otherFromNodes.nodes;
    }
    for (const s of overflow) {
      other.durationMs += s.durationMs;
      other.executions += s.executions;
      other.nodes += s.nodes;
    }
    shapes.push({ ...other, binds: [] });
  }

  return {
    metadata: [...metadata.values()].sort((a, b) => b.durationMs - a.durationMs),
    sql: shapes,
  };
}
