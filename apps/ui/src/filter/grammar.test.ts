import fc from 'fast-check';
import { describe, expect, it } from 'vitest';

import { FIELDS, fieldDef } from './catalog';
import type { FieldDef } from './catalog';
import { ANY, NONE, conditionEquals, escapeRegex, isListOperator, isOperator, regexLiteral } from './condition';
import type { Condition, Operator } from './condition';
import { formatCondition, parseCondition, validateCondition } from './grammar';

function parsed(text: string): Condition {
  const r = parseCondition(text);
  if (!r.ok) throw new Error(r.error);
  return r.condition;
}

/** A value with no surrounding whitespace, including commas, backslashes, and operator characters. */
const plainValue = fc
  .string({ minLength: 1, maxLength: 12, unit: fc.constantFrom('a', 'Z', '0', '-', '.', ',', '\\', '=', '~', '!', '<', ' ', '/', '*') })
  .map((s) => s.trim())
  .filter((s) => s !== '' && s !== NONE && s !== ANY);

const durationValue = fc
  .tuple(fc.integer({ min: 0, max: 100_000 }), fc.constantFrom('ms', 's'))
  .map(([n, unit]) => `${n}${unit}`);

const paramField: fc.Arbitrary<FieldDef> = fc
  .stringMatching(/^[a-z_][a-z0-9_.-]{0,10}$/)
  .filter((name) => !FIELDS.some((f) => f.name === name))
  .map((name) => fieldDef(name)!);

function conditionFor(def: FieldDef): fc.Arbitrary<Condition> {
  return fc.constantFrom(...def.operators).chain((op: Operator) => {
    let values: fc.Arbitrary<string[]>;
    if (def.kind === 'duration') {
      values = op === 'between' ? fc.tuple(durationValue, durationValue).map(([a, b]) => [`${a}..${b}`]) : durationValue.map((v) => [v]);
    } else if (def.values !== undefined) {
      const one = fc.constantFrom(...def.values);
      values = isListOperator(op) ? fc.uniqueArray(one, { minLength: 1 }) : one.map((v) => [v]);
    } else {
      const one = def.optional && (op === '=' || op === '!=') ? fc.oneof(plainValue, fc.constantFrom(NONE, ANY)) : plainValue;
      values = isListOperator(op) ? fc.array(plainValue, { minLength: 1, maxLength: 4 }) : one.map((v) => [v]);
    }
    return values.map((vs) => ({ field: def.name, op, values: vs }));
  });
}

const anyCondition = fc.oneof(fc.constantFrom(...FIELDS), paramField).chain(conditionFor);

describe('filter grammar', () => {
  it('returns the condition after formatting and parsing it', () => {
    fc.assert(
      fc.property(anyCondition, (c) => {
        expect(validateCondition(c)).toBeNull();
        expect(parsed(formatCondition(c))).toEqual(c);
      }),
      { numRuns: 500 },
    );
  });

  it.each<[string, Condition]>([
    ['pod=billing-1', { field: 'pod', op: '=', values: ['billing-1'] }],
    ['pod!=billing-1', { field: 'pod', op: '!=', values: ['billing-1'] }],
    ['method=~Order', { field: 'method', op: '=~', values: ['Order'] }],
    ['method!~health', { field: 'method', op: '!~', values: ['health'] }],
    ['namespace in prod-orders,prod-payments', { field: 'namespace', op: 'in', values: ['prod-orders', 'prod-payments'] }],
    ['service not in billing', { field: 'service', op: 'not in', values: ['billing'] }],
    ['duration>100ms', { field: 'duration', op: '>', values: ['100ms'] }],
    ['duration>=2s', { field: 'duration', op: '>=', values: ['2s'] }],
    ['duration<1.5s', { field: 'duration', op: '<', values: ['1.5s'] }],
    ['duration<=500ms', { field: 'duration', op: '<=', values: ['500ms'] }],
    ['duration between 100ms..2s', { field: 'duration', op: 'between', values: ['100ms..2s'] }],
  ])('reads %s', (text, condition) => {
    expect(parsed(text)).toEqual(condition);
    expect(formatCondition(condition)).toBe(text);
  });

  it('reads None and Any on an optional field', () => {
    expect(parsed('trace_id=None')).toEqual({ field: 'trace_id', op: '=', values: [NONE] });
    expect(parsed('endpoint!=Any')).toEqual({ field: 'endpoint', op: '!=', values: [ANY] });
  });

  it('reads a call parameter as a field', () => {
    expect(parsed('user.tenant=acme')).toEqual({ field: 'user.tenant', op: '=', values: ['acme'] });
  });

  it('allows spaces around a symbol operator', () => {
    expect(parsed(' duration > 100ms ')).toEqual({ field: 'duration', op: '>', values: ['100ms'] });
  });

  it('escapes a comma and a backslash inside an in list', () => {
    const c: Condition = { field: 'user.tenant', op: 'in', values: ['a,b', 'c\\d'] };
    expect(formatCondition(c)).toBe('user.tenant in a\\,b,c\\\\d');
    expect(parsed(formatCondition(c))).toEqual(c);
  });

  it('keeps an equality value that starts with ~ apart from a regex match', () => {
    const c: Condition = { field: 'pod', op: '=', values: ['~x'] };
    expect(formatCondition(c)).toBe('pod= ~x');
    expect(parsed('pod= ~x')).toEqual(c);
  });

  it.each([
    ['', 'does not start with a field name'],
    ['pod', 'has no operator'],
    ['duration=100ms', 'does not take the operator ='],
    ['error=yes', 'is not a value of error'],
    ['duration>fast', 'Enter a duration'],
    ['duration between 2s', 'Enter a range'],
    ['pod=None', 'does not apply'],
    ['trace_id=~None', 'goes with = or != only'],
    ['pod=', 'has an empty value'],
    ['pod in a,,b', 'has an empty value'],
    ['pod in a\\', 'ends with a backslash'],
    ['retention_class=hot', 'is not a value of retention_class'],
    ['pod=a=b', null],
  ])('rejects %j', (text, message) => {
    const r = parseCondition(text);
    if (message === null) {
      expect(r).toEqual({ ok: true, condition: { field: 'pod', op: '=', values: ['a=b'] } });
    } else {
      expect(r.ok).toBe(false);
      expect(r.ok ? '' : r.error).toContain(message);
    }
  });
});

describe('validateCondition', () => {
  // The grammar never builds these; a caller that assembles a condition by hand can.
  it.each<[Condition, string]>([
    [{ field: 'bad name', op: '=', values: ['x'] }, '"bad name" is not a field name.'],
    [{ field: 'pod', op: '=', values: [] }, 'The filter pod = needs a value.'],
    [{ field: 'pod', op: '=', values: ['a', 'b'] }, 'The operator = takes one value.'],
    [{ field: 'pod', op: 'in', values: ['a', ' b'] }, 'The filter pod in has an empty value.'],
  ])('rejects %j', (c, message) => {
    expect(validateCondition(c)).toBe(message);
  });
});

describe('isOperator', () => {
  it('accepts the operators of the contract only', () => {
    for (const op of ['=', '!=', '=~', '!~', 'in', 'not in', '>', '>=', '<', '<=', 'between']) expect(isOperator(op)).toBe(true);
    expect(isOperator('==')).toBe(false);
    expect(isOperator('IN')).toBe(false);
  });
});

describe('conditionEquals', () => {
  const c: Condition = { field: 'pod', op: 'in', values: ['a', 'b'] };

  it('compares the field, the operator, and the values in order', () => {
    expect(conditionEquals(c, { field: 'pod', op: 'in', values: ['a', 'b'] })).toBe(true);
    expect(conditionEquals(c, { ...c, field: 'service' })).toBe(false);
    expect(conditionEquals(c, { ...c, op: 'not in' })).toBe(false);
    expect(conditionEquals(c, { ...c, values: ['b', 'a'] })).toBe(false);
    expect(conditionEquals(c, { ...c, values: ['a'] })).toBe(false);
  });
});

describe('regexLiteral', () => {
  it('returns the word a quoted pattern matches', () => {
    fc.assert(fc.property(fc.string(), (word) => regexLiteral(escapeRegex(word)) === word));
  });

  it('rejects a pattern with regular-expression syntax', () => {
    expect(regexLiteral('Order.*')).toBeNull();
    expect(regexLiteral('a|b')).toBeNull();
    expect(regexLiteral('\\d+')).toBeNull();
    expect(regexLiteral('OrderService\\.place')).toBe('OrderService.place');
  });
});
