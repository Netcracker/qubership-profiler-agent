import { FIELDS, fieldDef } from '../../filter/catalog';
import { ANY, NONE, escapeRegex, isListOperator } from '../../filter/condition';
import type { Condition, Operator } from '../../filter/condition';
import { formatCondition, parseCondition } from '../../filter/grammar';

// The dropdown of the filter bar for each stage of the condition being typed: field, then operator, then value
// (CallsAlt mockup). A pure function, so the stages are testable without rendering.

/** Why a field, or an operator of a field, cannot be picked; null when it can. */
export type SupportedPredicate = (field: string, op?: Operator) => string | null;

/** Why a whole condition, values included, cannot be picked; null when it can. */
export type ConditionPredicate = (c: Condition) => string | null;

export interface ValueSuggestion {
  value: string;
  /** The number of calls with the value, when the source knows it. */
  count?: number;
}

/** The values offered for a field; the bar narrows them by the typed text. */
export type ValueSource = (field: string) => readonly ValueSuggestion[];

/** The part of a condition picked so far. */
export interface Draft {
  field: string | null;
  op: Operator | null;
  /** Values picked so far for `in` and `not in`. */
  values: readonly string[];
}

export const EMPTY_DRAFT: Draft = { field: null, op: null, values: [] };

export type ItemAction =
  | { kind: 'field'; field: string }
  | { kind: 'op'; op: Operator }
  | { kind: 'value'; value: string }
  | { kind: 'apply' }
  | { kind: 'condition'; condition: Condition };

export interface Item {
  label: string;
  hint: string;
  count?: number;
  /** The reason the item cannot be picked, or undefined when it can. */
  disabledReason?: string;
  action: ItemAction;
}

export interface StageView {
  stage: 'field' | 'op' | 'value';
  title: string;
  foot: string;
  items: Item[];
}

/** The most values listed at once; the footer says how many matched. */
export const MAX_VALUES = 8;

const OP_HINT: Record<Operator, string> = {
  '=': 'is',
  '!=': 'is not',
  '=~': 'matches regex',
  '!~': 'does not match regex',
  in: 'is one of',
  'not in': 'is none of',
  '>': 'longer than',
  '>=': 'at least',
  '<': 'shorter than',
  '<=': 'at most',
  between: 'in a range',
};

const DURATION_PRESETS = ['10ms', '100ms', '500ms', '1s', '3s', '5s'];
const RANGE_PRESETS = ['100ms..500ms', '500ms..2s', '1s..5s'];

function orNull(reason: string | null): string | undefined {
  return reason === null ? undefined : reason;
}

export interface StageInput {
  draft: Draft;
  text: string;
  /** The fields the bar lists, in order. */
  fields: readonly string[];
  supported: SupportedPredicate;
  /** Checks the values too, such as `None` on a backend that cannot match it; `supported` alone when absent. */
  conditionReason?: ConditionPredicate;
  values: ValueSource;
}

/** The fields of the catalog, in catalog order. */
export const ALL_FIELDS: readonly string[] = FIELDS.map((f) => f.name);

export function stageView({ draft, text, fields, supported, conditionReason, values }: StageInput): StageView {
  const reasonOf = (c: Condition): string | undefined => orNull(conditionReason ? conditionReason(c) : supported(c.field, c.op));
  const typed = text.trim();
  const needle = typed.toLowerCase();

  if (draft.field === null) {
    const items: Item[] = fields
      .filter((name) => name.toLowerCase().includes(needle))
      .map((name) => ({
        label: name,
        hint: fieldDef(name)?.hint ?? '',
        disabledReason: orNull(supported(name)),
        action: { kind: 'field', field: name },
      }));
    if (typed !== '') {
      // A whole condition typed at once, such as `duration>1s`.
      const parsed = parseCondition(typed);
      if (parsed.ok && fields.includes(parsed.condition.field)) {
        const c = parsed.condition;
        items.unshift({
          label: formatCondition(c),
          hint: 'Add this filter',
          disabledReason: reasonOf(c),
          action: { kind: 'condition', condition: c },
        });
      }
      // A word searches the method name, as the old search box did.
      if (fields.includes('method') && !parsed.ok && !typed.includes(' ')) {
        const c: Condition = { field: 'method', op: '=~', values: [escapeRegex(typed)] };
        items.push({
          label: `method =~ ${typed}`,
          hint: 'Search in the method name',
          disabledReason: reasonOf(c),
          action: { kind: 'condition', condition: c },
        });
      }
    }
    return {
      stage: 'field',
      title: 'Filter by',
      foot: 'Pick a field, or type a word to search in the method name. All filters must match.',
      items,
    };
  }

  const def = fieldDef(draft.field);
  if (draft.op === null) {
    return {
      stage: 'op',
      title: `Operator for ${draft.field}`,
      foot: 'Backspace returns to the field.',
      items: (def?.operators ?? [])
        .filter((op) => op.startsWith(needle))
        .map((op) => ({
          label: op,
          hint: OP_HINT[op],
          disabledReason: orNull(supported(draft.field!, op)),
          action: { kind: 'op', op },
        })),
    };
  }

  const multi = isListOperator(draft.op);
  const items: Item[] = [];
  if (multi && draft.values.length > 0 && typed === '') {
    const n = draft.values.length;
    items.push({ label: `Apply ${n} ${n === 1 ? 'value' : 'values'}`, hint: 'Enter', action: { kind: 'apply' } });
  }
  if (typed !== '') items.push({ label: `Use "${typed}"`, hint: 'as typed', action: { kind: 'value', value: typed } });

  let foot: string;
  if (def?.kind === 'duration') {
    const presets = draft.op === 'between' ? RANGE_PRESETS : DURATION_PRESETS;
    for (const p of presets.filter((v) => v.includes(needle))) items.push({ label: p, hint: '', action: { kind: 'value', value: p } });
    foot = draft.op === 'between' ? 'Type a range such as 100ms..2s.' : 'Type any duration such as 250ms or 1.5s.';
  } else {
    if (def?.optional && (draft.op === '=' || draft.op === '!=') && typed === '') {
      for (const [value, hint] of [
        [NONE, `no ${draft.field}`],
        [ANY, `any ${draft.field}`],
      ] as const) {
        const disabledReason = reasonOf({ field: draft.field, op: draft.op, values: [value] });
        items.push({ label: value, hint, disabledReason, action: { kind: 'value', value } });
      }
    }
    const known: readonly ValueSuggestion[] = def?.values?.map((value) => ({ value })) ?? values(draft.field);
    const matching = known.filter((s) => s.value.toLowerCase().includes(needle) && !draft.values.includes(s.value));
    for (const s of matching.slice(0, MAX_VALUES)) {
      items.push({ label: s.value, hint: '', count: s.count, action: { kind: 'value', value: s.value } });
    }
    const shown = matching.length > MAX_VALUES ? `Showing ${MAX_VALUES} of ${matching.length.toLocaleString('en-US')} values. ` : '';
    foot = shown + (multi ? 'Pick several values, then press Enter.' : 'Type to search, or enter your own value.');
  }
  return { stage: 'value', title: `Values for ${draft.field}`, foot, items };
}
