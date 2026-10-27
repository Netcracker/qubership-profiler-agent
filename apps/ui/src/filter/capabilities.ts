import type { FilterCapabilities } from '../api/types';
import { fieldDef } from './catalog';
import { ANY, NONE } from './condition';
import type { Condition, Operator } from './condition';

// What the running backend implements of the filter model, read from `filter_capabilities` in /config
// (02 §2.3.4). The backend builds the model in stages; the filter bar disables what this build lacks instead of
// sending it and getting an `invalid_request` back.

export interface Capabilities {
  /** Why the build cannot filter by a field, or by a field with an operator; null when it can. */
  supported: (field: string, op?: Operator) => string | null;
  /** Why the build cannot apply a condition, values included; null when it can. */
  conditionReason: (c: Condition) => string | null;
  /** The `/calls` orders the build implements. */
  orders: ReadonlySet<string>;
  /** True when `/stats` groups by this field or by `time`. */
  canGroupBy: (field: string) => boolean;
}

export function capabilitiesOf(fc: FilterCapabilities): Capabilities {
  const specials = new Set(fc.special_values);
  const groupBy = new Set(fc.group_by);

  const operatorsOf = (field: string): readonly string[] | null => {
    const def = fieldDef(field);
    if (def === null) return null;
    if (def.kind === 'param') return fc.param_operators.length > 0 ? fc.param_operators : null;
    return fc.fields[field] ?? null;
  };

  const supported = (field: string, op?: Operator): string | null => {
    const ops = operatorsOf(field);
    if (ops === null) {
      return fieldDef(field)?.kind === 'param'
        ? 'This backend cannot filter by call parameters.'
        : `This backend cannot filter by ${field}.`;
    }
    if (op !== undefined && !ops.includes(op)) return `This backend does not support ${op} on ${field}.`;
    return null;
  };

  const conditionReason = (c: Condition): string | null => {
    const reason = supported(c.field, c.op);
    if (reason !== null) return reason;
    const special = c.values.find((v) => v === NONE || v === ANY);
    // None and Any are special with = and != only; with any other operator they are plain text (02 §2.3.3).
    if (special !== undefined && (c.op === '=' || c.op === '!=') && !specials.has(special)) {
      return `This backend cannot match ${special} yet.`;
    }
    return null;
  };

  const canGroupBy = (field: string): boolean => {
    if (groupBy.has(field)) return true;
    return fc.group_by_params === true && fieldDef(field)?.kind === 'param';
  };

  return { supported, conditionReason, orders: new Set(fc.orders), canGroupBy };
}
