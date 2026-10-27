import { useEffect, useId, useMemo, useState } from 'react';
import type { KeyboardEvent } from 'react';

import { isListOperator } from '../../filter/condition';
import type { Condition } from '../../filter/condition';
import { validateCondition } from '../../filter/grammar';
import { ALL_FIELDS, EMPTY_DRAFT, stageView } from './suggestions';
import type { Draft, Item, SupportedPredicate, ValueNote, ValueSource } from './suggestions';
import styles from './filter-bar.module.css';

// The filter bar of the calls page (CallsAlt mockup): one token per condition, and an input that builds the next
// condition in three stages, field, operator, and value. The bar edits the draft only; the page decides when a
// draft runs.

/**
 * A token the page owns rather than a condition of the bar, such as "Hide system/proxy". It reads like a condition,
 * comes before the conditions, and is removed with its × or with Backspace once no condition is left before the input.
 */
export interface PresetToken {
  key: string;
  field: string;
  op: string;
  value: string;
  /** The tooltip: what the token does and where it applies. */
  title: string;
  onRemove: () => void;
}

export interface FilterBarProps {
  conditions: readonly Condition[];
  onChange: (next: Condition[]) => void;
  /** Why a field or an operator cannot be picked. Everything can be picked when it is absent. */
  supported?: SupportedPredicate;
  /** Why a token is not applied; a token with a reason renders disabled. */
  tokenReason?: (condition: Condition) => string | null;
  /**
   * A note for a token that applies, but only to the loaded rows in the browser. Such a token renders with a dashed
   * outline and the note as its tooltip.
   */
  tokenNote?: (condition: Condition) => string | null;
  /** Tokens the page owns, shown before the conditions. */
  presets?: readonly PresetToken[];
  /** Value suggestions per field. */
  values?: ValueSource;
  /** A footer note on the values of a field, such as that they are loading. */
  valueNote?: ValueNote;
  /** Called with the field whose values the bar lists, or null when it lists none, so the page can load them. */
  onValueField?: (field: string | null) => void;
  /** The fields the bar lists; every field of the catalog when absent. */
  fields?: readonly string[];
  /** Ctrl+Enter or Cmd+Enter in the input. */
  onRun?: () => void;
  placeholder?: string;
}

const ALL_SUPPORTED: SupportedPredicate = () => null;
const NO_PRESETS: readonly PresetToken[] = [];
const NO_VALUES: ValueSource = () => [];

function tokenText(c: Condition): string {
  return `${c.field} ${c.op} ${c.values.join(', ')}`;
}

export function FilterBar({
  conditions,
  onChange,
  supported = ALL_SUPPORTED,
  tokenReason,
  tokenNote,
  presets = NO_PRESETS,
  values = NO_VALUES,
  fields = ALL_FIELDS,
  onRun,
  placeholder,
  valueNote,
  onValueField,
}: FilterBarProps) {
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT);
  const [text, setText] = useState('');
  const [highlight, setHighlight] = useState(0);
  const [open, setOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const listId = useId();

  const view = useMemo(
    () => stageView({ draft, text, fields, supported, values, valueNote }),
    [draft, text, fields, supported, values, valueNote],
  );

  const valueField = draft.op !== null ? draft.field : null;
  useEffect(() => onValueField?.(valueField), [valueField, onValueField]);
  const items = view.items;
  const active = Math.max(0, Math.min(highlight, items.length - 1));

  const reset = (next: Draft = EMPTY_DRAFT, nextText = ''): void => {
    setDraft(next);
    setText(nextText);
    setHighlight(0);
    setError(null);
    setOpen(true);
  };

  const add = (c: Condition): void => {
    const problem = validateCondition(c);
    if (problem !== null) {
      setError(problem);
      setOpen(true);
      return;
    }
    onChange([...conditions, c]);
    reset();
  };

  const pick = (item: Item): void => {
    if (item.disabledReason !== undefined) return;
    const a = item.action;
    if (a.kind === 'condition') add(a.condition);
    else if (a.kind === 'field') reset({ field: a.field, op: null, values: [] });
    else if (a.kind === 'op') reset({ ...draft, op: a.op });
    else if (a.kind === 'apply') add({ field: draft.field!, op: draft.op!, values: draft.values });
    else if (isListOperator(draft.op!)) reset({ ...draft, values: [...draft.values, a.value] });
    else add({ field: draft.field!, op: draft.op!, values: [a.value] });
  };

  const stepBack = (): void => {
    if (draft.values.length > 0) reset({ ...draft, values: draft.values.slice(0, -1) });
    else if (draft.op !== null) reset({ ...draft, op: null });
    else if (draft.field !== null) reset();
    else if (conditions.length > 0) {
      // Step back into the last token, so it can be edited in place.
      const last = conditions[conditions.length - 1]!;
      onChange(conditions.slice(0, -1));
      if (isListOperator(last.op)) reset({ field: last.field, op: last.op, values: last.values });
      else reset({ field: last.field, op: last.op, values: [] }, last.values[0] ?? '');
    } else if (presets.length > 0) {
      presets[presets.length - 1]!.onRemove();
    }
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setOpen(true);
      setHighlight(Math.min(items.length - 1, active + 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setHighlight(Math.max(0, active - 1));
    } else if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      onRun?.();
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const item = items[active];
      if (open && item !== undefined) pick(item);
      else setOpen(true);
    } else if (e.key === 'Escape') {
      setOpen(false);
    } else if (e.key === 'Backspace' && text === '') {
      e.preventDefault();
      stepBack();
    }
  };

  const inputPlaceholder =
    view.stage === 'field'
      ? conditions.length > 0 || presets.length > 0
        ? 'Add a filter'
        : (placeholder ?? 'Filter calls: pick a field or type a method name')
      : view.stage === 'op'
        ? 'Operator'
        : 'Value';
  const showList = open && (items.length > 0 || error !== null);
  const optionId = (i: number): string => `${listId}-option-${i}`;

  return (
    <div className={styles.bar}>
      <svg className={styles.icon} width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
        <path d="M2 3.5h12L9.5 9v4l-3-1.5V9z" />
      </svg>
      {presets.map((p) => (
        <span key={p.key} className={`${styles.token} ${styles.tokenLocal}`} title={p.title} data-applied="browser">
          <span>{p.field}</span>
          <span>{p.op}</span>
          <span className={styles.tokenValue}>{p.value}</span>
          <button
            type="button"
            aria-label={`Remove the filter ${p.field} ${p.op} ${p.value}`}
            title="Remove this filter"
            onClick={p.onRemove}
          >
            <svg width="9" height="9" viewBox="0 0 10 10" aria-hidden="true">
              <path d="M2 2l6 6M8 2 2 8" />
            </svg>
          </button>
        </span>
      ))}
      {conditions.map((c, i) => {
        const reason = tokenReason?.(c) ?? null;
        const note = reason === null ? (tokenNote?.(c) ?? null) : null;
        const label = tokenText(c);
        return (
          <span
            key={`${i}:${label}`}
            className={`${styles.token}${reason !== null ? ` ${styles.tokenOff}` : note !== null ? ` ${styles.tokenLocal}` : ''}`}
            title={reason ?? note ?? undefined}
            data-applied={reason !== null ? 'false' : note !== null ? 'browser' : 'true'}
          >
            <span>{c.field}</span>
            <span>{c.op}</span>
            <span className={styles.tokenValue}>{c.values.join(', ')}</span>
            <button
              type="button"
              aria-label={`Remove the filter ${label}`}
              title="Remove this filter"
              onClick={() => onChange(conditions.filter((_, j) => j !== i))}
            >
              <svg width="9" height="9" viewBox="0 0 10 10" aria-hidden="true">
                <path d="M2 2l6 6M8 2 2 8" />
              </svg>
            </button>
          </span>
        );
      })}
      {draft.field !== null ? (
        <span className={styles.token}>
          <span>{draft.field}</span>
          {draft.op !== null ? <span>{draft.op}</span> : null}
          {draft.values.map((v) => (
            <span key={v} className={styles.tokenValue}>
              {v}
            </span>
          ))}
        </span>
      ) : null}
      <input
        className={`${styles.input}${error === null ? '' : ` ${styles.inputError}`}`}
        role="combobox"
        aria-label="Filter calls"
        aria-expanded={showList}
        aria-controls={listId}
        aria-autocomplete="list"
        aria-activedescendant={showList && items.length > 0 ? optionId(active) : undefined}
        aria-invalid={error !== null}
        autoComplete="off"
        spellCheck={false}
        placeholder={inputPlaceholder}
        value={text}
        onChange={(e) => {
          setText(e.target.value);
          setHighlight(0);
          setError(null);
          setOpen(true);
        }}
        onKeyDown={onKeyDown}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
      />
      {conditions.length > 0 || presets.length > 0 || draft.field !== null ? (
        <button
          type="button"
          className={styles.clear}
          title="Clear all filters"
          aria-label="Clear all filters"
          onClick={() => {
            onChange([]);
            presets.forEach((p) => p.onRemove());
            reset(EMPTY_DRAFT);
            setOpen(false);
          }}
        >
          <svg width="11" height="11" viewBox="0 0 10 10" aria-hidden="true">
            <path d="M2 2l6 6M8 2 2 8" />
          </svg>
        </button>
      ) : null}
      {showList ? (
        <div className={styles.dropdown}>
          <div className={styles.title}>{view.title}</div>
          {error !== null ? (
            <div className={styles.error} role="alert">
              {error}
            </div>
          ) : null}
          <div id={listId} role="listbox" aria-label={view.title} className={styles.list}>
            {items.map((item, i) => (
              <div
                key={`${i}:${item.label}`}
                id={optionId(i)}
                role="option"
                aria-selected={i === active}
                aria-disabled={item.disabledReason !== undefined}
                title={item.disabledReason}
                className={`${styles.option}${i === active ? ` ${styles.active}` : ''}${
                  item.disabledReason === undefined ? '' : ` ${styles.optionOff}`
                }`}
                // Keep the focus in the input: a click must not blur it and close the list first.
                onMouseDown={(e) => {
                  e.preventDefault();
                  pick(item);
                }}
                onMouseEnter={() => setHighlight(i)}
              >
                <span className={styles.label}>{item.label}</span>
                <span className={styles.hint}>{item.disabledReason === undefined ? item.hint : 'Not supported yet'}</span>
                {item.count !== undefined ? <span className={styles.count}>{item.count.toLocaleString('en-US')}</span> : null}
              </div>
            ))}
          </div>
          <div className={styles.foot}>{view.foot}</div>
        </div>
      ) : null}
    </div>
  );
}
