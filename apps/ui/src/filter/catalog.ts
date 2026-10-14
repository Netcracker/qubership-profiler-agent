import { RETENTION_CLASSES } from '../api/types';
import type { Operator } from './condition';

// The fields of the read contract's filter model (agent-access plan §5.1) and the operators each one takes. A name
// outside the catalog is a call parameter, such as `user.tenant`.

export type FieldKind = 'duration' | 'enum' | 'text' | 'id' | 'bool' | 'param';

export interface FieldDef {
  name: string;
  kind: FieldKind;
  /** The operators in the order the filter bar offers them. */
  operators: readonly Operator[];
  /** A short description for the field list. */
  hint: string;
  /** True when a call can lack the field, so `None` and `Any` make sense. */
  optional: boolean;
  /** The complete value set, for a field that has one. */
  values?: readonly string[];
}

const DURATION_OPS: readonly Operator[] = ['>', '>=', '<', '<=', 'between'];
const ENUM_OPS: readonly Operator[] = ['=', '!=', '=~', '!~', 'in', 'not in'];
const ID_OPS: readonly Operator[] = ['=', '!=', '=~'];
const TEXT_OPS: readonly Operator[] = ['=~', '!~', '=', '!='];

export const FIELDS: readonly FieldDef[] = [
  { name: 'duration', kind: 'duration', operators: DURATION_OPS, hint: 'Call duration', optional: false },
  { name: 'namespace', kind: 'enum', operators: ENUM_OPS, hint: 'Kubernetes namespace', optional: false },
  { name: 'service', kind: 'enum', operators: ENUM_OPS, hint: 'Service', optional: false },
  { name: 'pod', kind: 'enum', operators: ENUM_OPS, hint: 'Pod name', optional: false },
  { name: 'endpoint', kind: 'enum', operators: ENUM_OPS, hint: 'HTTP method and path', optional: true },
  { name: 'class', kind: 'enum', operators: ENUM_OPS, hint: 'Java class', optional: false },
  { name: 'method', kind: 'text', operators: TEXT_OPS, hint: 'Root method', optional: false },
  { name: 'error', kind: 'bool', operators: ['='], hint: 'Call failed', optional: false, values: ['true', 'false'] },
  { name: 'trace_id', kind: 'id', operators: ID_OPS, hint: 'Trace ID', optional: true },
  { name: 'request_id', kind: 'id', operators: ID_OPS, hint: 'Request ID', optional: true },
  {
    name: 'retention_class',
    kind: 'enum',
    operators: ['=', '!=', 'in', 'not in'],
    hint: 'Storage class of the call',
    optional: false,
    values: RETENTION_CLASSES,
  },
];

/** The operators of a call parameter. */
const PARAM_OPS = ENUM_OPS;

/** A call parameter name: no whitespace and no operator characters, so the text form stays unambiguous. */
const PARAM_NAME = /^[A-Za-z_][A-Za-z0-9_.-]*$/;

/** The definition of a field, a call parameter for a name outside the catalog, or null for an invalid name. */
export function fieldDef(name: string): FieldDef | null {
  const known = FIELDS.find((f) => f.name === name);
  if (known !== undefined) return known;
  if (!PARAM_NAME.test(name)) return null;
  return { name, kind: 'param', operators: PARAM_OPS, hint: 'Call parameter', optional: true };
}
