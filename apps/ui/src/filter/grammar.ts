import { parseDurationMs, parseDurationRange } from '../calls/duration-filter';
import { fieldDef } from './catalog';
import { ANY, NONE, isListOperator } from './condition';
import type { Condition, Operator } from './condition';

// The text form of a condition, as a `filter=` URL parameter carries it. The read contract (agent-access plan §5.1)
// leaves the exact grammar and escaping to its task 1.1, so this module is the one place that follows the draft and
// the one place to change when the contract lands:
//
//   <field><op><value>              op is = != =~ !~ > >= < <=      duration>100ms, namespace=prod-orders
//   <field> in <value>,<value>      also `not in`; a comma or a backslash in a value is escaped with a backslash
//   <field> between <from>..<to>    duration between 100ms..2s
//
// Whitespace around a symbol operator is allowed when parsing and never written.

/** Symbol operators, longest first, so `>=` is not read as `>` followed by `=`. */
const SYMBOL_OPS: readonly Operator[] = ['=~', '!~', '!=', '>=', '<=', '=', '>', '<'];

const HEAD = /^([^\s=!~<>]+)(?:\s+(not in|in|between)\s+|\s*)/;

function escapeListValue(value: string): string {
  return value.replace(/[\\,]/g, '\\$&');
}

/** Splits on unescaped commas and removes the escapes. Null for a dangling backslash. */
function splitListValues(text: string): string[] | null {
  const out: string[] = [];
  let current = '';
  for (let i = 0; i < text.length; i++) {
    const ch = text[i]!;
    if (ch === '\\') {
      const next = text[i + 1];
      if (next === undefined) return null;
      current += next;
      i++;
    } else if (ch === ',') {
      out.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  out.push(current.trim());
  return out;
}

/** The text form of a condition. The inverse of parseCondition for every valid condition. */
export function formatCondition(c: Condition): string {
  if (isListOperator(c.op)) return `${c.field} ${c.op} ${c.values.map(escapeListValue).join(',')}`;
  if (c.op === 'between') return `${c.field} between ${c.values[0] ?? ''}`;
  const value = c.values[0] ?? '';
  // `pod=~x` reads as a regex match, so a value that starts with an operator character keeps a space before it.
  return `${c.field}${c.op}${/^[=~]/.test(value) ? ' ' : ''}${value}`;
}

export type ParseResult = { ok: true; condition: Condition } | { ok: false; error: string };

/** Reads the text form and checks the condition against the field catalog. */
export function parseCondition(text: string): ParseResult {
  const trimmed = text.trim();
  const head = HEAD.exec(trimmed);
  if (head === null) return { ok: false, error: `"${trimmed}" does not start with a field name.` };
  const field = head[1]!;
  const rest = trimmed.slice(head[0].length);
  let condition: Condition;
  const word = head[2] as Operator | undefined;
  if (word !== undefined) {
    if (word === 'between') {
      condition = { field, op: word, values: [rest] };
    } else {
      const values = splitListValues(rest);
      if (values === null) return { ok: false, error: `The value list of "${trimmed}" ends with a backslash.` };
      condition = { field, op: word, values };
    }
  } else {
    const op = SYMBOL_OPS.find((o) => rest.startsWith(o));
    if (op === undefined) return { ok: false, error: `"${trimmed}" has no operator after the field ${field}.` };
    condition = { field, op, values: [rest.slice(op.length).trim()] };
  }
  const error = validateCondition(condition);
  return error === null ? { ok: true, condition } : { ok: false, error };
}

/** Checks a condition against the catalog. Returns a message naming the problem, or null for a valid condition. */
export function validateCondition(c: Condition): string | null {
  const def = fieldDef(c.field);
  if (def === null) return `"${c.field}" is not a field name.`;
  if (!def.operators.includes(c.op)) return `The field ${c.field} does not take the operator ${c.op}.`;
  if (c.values.length === 0) return `The filter ${c.field} ${c.op} needs a value.`;
  if (!isListOperator(c.op) && c.values.length > 1) return `The operator ${c.op} takes one value.`;
  for (const v of c.values) {
    if (v === '' || v.trim() !== v) return `The filter ${c.field} ${c.op} has an empty value.`;
    if (v === NONE || v === ANY) {
      if (!def.optional) return `Every call has a ${c.field}, so ${v} does not apply.`;
      if (c.op !== '=' && c.op !== '!=') return `${v} goes with = or != only.`;
    }
  }
  const value = c.values[0]!;
  if (def.kind === 'duration') {
    const ok = c.op === 'between' ? parseDurationRange(value) !== null : parseDurationMs(value) !== null;
    if (!ok) return c.op === 'between' ? 'Enter a range such as 100ms..2s.' : 'Enter a duration such as 500ms or 2s.';
  }
  if (def.values !== undefined && (def.kind === 'bool' || def.name === 'retention_class')) {
    const bad = c.values.find((v) => !def.values!.includes(v));
    if (bad !== undefined) return `"${bad}" is not a value of ${c.field}. Use ${def.values.join(', ')}.`;
  }
  return null;
}
