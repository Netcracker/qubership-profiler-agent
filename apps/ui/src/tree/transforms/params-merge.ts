import type { ParamGroupWire, ParamWire } from '../../msgpack/tree-wire';

// Param merging shared by the 5.3 transforms (old mergeNodes tag merging,
// extended to the R11 group mini-tree): groups match by (paramIdx, value)
// and sum their duration and executions; nested binds merge recursively.

export function mergeParamsInto(dst: ParamWire[], src: readonly ParamWire[]): ParamWire[] {
  for (const srcParam of src) {
    let dstParam = dst.find((p) => p.paramIdx === srcParam.paramIdx);
    if (dstParam === undefined) {
      dstParam = { paramIdx: srcParam.paramIdx, groups: [] };
      dst.push(dstParam);
    }
    for (const group of srcParam.groups) {
      const existing = dstParam.groups.find((g) => g.value === group.value);
      if (existing === undefined) {
        dstParam.groups.push(copyGroup(group));
      } else {
        existing.durationMs += group.durationMs;
        existing.executions += group.executions;
        if (group.unresolved === true) existing.unresolved = true;
        if (group.params !== undefined) {
          existing.params = mergeParamsInto(existing.params ?? [], group.params);
        }
      }
    }
  }
  return dst;
}

function copyGroup(group: ParamGroupWire): ParamGroupWire {
  const out: ParamGroupWire = {
    value: group.value,
    durationMs: group.durationMs,
    executions: group.executions,
  };
  if (group.unresolved === true) out.unresolved = true;
  if (group.params !== undefined) out.params = mergeParamsInto([], group.params);
  return out;
}
