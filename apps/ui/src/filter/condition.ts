// The filter model of the read contract (agent-access plan §5.1, draft): a condition is a field, an operator, and a
// value, and all conditions of a query must match. The UI's filter tokens, the URL's `filter` parameters, and the
// CLI's `--filter` flag carry the same conditions.

/** Every operator of the contract, in the order the filter bar lists them. */
export const OPERATORS = ['=', '!=', '=~', '!~', 'in', 'not in', '>', '>=', '<', '<=', 'between'] as const;

export type Operator = (typeof OPERATORS)[number];

export function isOperator(s: string): s is Operator {
  return (OPERATORS as readonly string[]).includes(s);
}

/** The operators that take a list of values. Every other operator takes exactly one. */
export function isListOperator(op: Operator): op is 'in' | 'not in' {
  return op === 'in' || op === 'not in';
}

/** The value that matches a call without the field. */
export const NONE = 'None';
/** The value that matches a call with any value of the field. */
export const ANY = 'Any';

export interface Condition {
  field: string;
  op: Operator;
  /**
   * One value, or several for `in` and `not in`. A value has no leading or trailing whitespace. A duration value is
   * the text as written (`500ms`, `1.5s`); a `between` value is a range (`100ms..2s`).
   */
  values: readonly string[];
}

export function conditionEquals(a: Condition, b: Condition): boolean {
  return a.field === b.field && a.op === b.op && a.values.length === b.values.length && a.values.every((v, i) => v === b.values[i]);
}

const REGEX_SYNTAX = /[.^$*+?()[\]{}|\\]/g;

/** Quotes a word for `=~`, so the pattern matches the word as written. */
export function escapeRegex(word: string): string {
  return word.replace(REGEX_SYNTAX, '\\$&');
}

/**
 * Returns the literal text a pattern matches when it has no regular-expression syntax, other than backslash-escaped
 * syntax characters; returns null otherwise. `OrderService\.place` gives `OrderService.place`; `Order.*` gives null.
 */
export function regexLiteral(pattern: string): string | null {
  let out = '';
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i]!;
    if (ch === '\\') {
      const next = pattern[i + 1];
      if (next === undefined || !'.^$*+?()[]{}|\\'.includes(next)) return null;
      out += next;
      i++;
    } else if ('.^$*+?()[]{}|'.includes(ch)) {
      return null;
    } else {
      out += ch;
    }
  }
  return out;
}
