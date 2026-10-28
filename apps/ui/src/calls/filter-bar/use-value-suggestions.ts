import { useEffect, useState } from 'react';

import { fetchStats } from '../../api/endpoints';
import type { Capabilities } from '../../filter/capabilities';
import type { Condition } from '../../filter/condition';
import { formatCondition } from '../../filter/grammar';
import type { ValueSuggestion } from './suggestions';

// Value suggestions for the filter bar from /stats (redesign plan §4.3): the most frequent values of the field in the
// window, under the other conditions of the draft, with their call counts.

/** The most values one request lists. */
export const SUGGESTION_LIMIT = 50;
/** The pause after the last change before a request goes out. */
export const SUGGESTION_DEBOUNCE_MS = 250;

export type ValueSuggestionsState =
  | { kind: 'loading'; field: string }
  | {
      kind: 'ready';
      field: string;
      values: ValueSuggestion[];
      /** The number of distinct values; a lower bound when `approximate`. */
      distinct: number;
      /** The counts are estimates of a high-cardinality grouping (02 §2.9). */
      approximate: boolean;
      /** Some sources did not answer, so the counts may be low. */
      partial: boolean;
    }
  | { kind: 'failed'; field: string };

export interface ValueSuggestionsInput {
  /** The field whose values the bar lists, or null. */
  field: string | null;
  fromMs: number | null;
  toMs: number | null;
  /** The draft conditions; those on `field` itself are left out, so they do not hide its other values. */
  conditions: readonly Condition[];
  caps: Capabilities | null | undefined;
}

/**
 * Loads the values of the field from `/stats?group_by=<field>`, or returns null when /stats cannot group by it, so
 * the caller falls back on its own source. A newer input aborts the older request.
 */
export function useValueSuggestions({ field, fromMs, toMs, conditions, caps }: ValueSuggestionsInput): ValueSuggestionsState | null {
  const groupable = field !== null && fromMs !== null && toMs !== null && !!caps && caps.canGroupBy(field);
  const filters = groupable
    ? conditions.filter((c) => c.field !== field && caps.conditionReason(c) === null).map(formatCondition)
    : [];
  const key = groupable ? JSON.stringify([field, fromMs, toMs, filters]) : null;
  const [state, setState] = useState<ValueSuggestionsState | null>(null);

  useEffect(() => {
    if (key === null) {
      setState(null);
      return;
    }
    const f = field!;
    setState({ kind: 'loading', field: f });
    const controller = new AbortController();
    const timer = setTimeout(() => {
      fetchStats({ fromMs: fromMs!, toMs: toMs!, filters, groupBy: f, limit: SUGGESTION_LIMIT }, controller.signal)
        .then((data) => {
          const values = data.rows.map((r) => ({ value: r.key, count: r.count }));
          setState({ kind: 'ready', field: f, values, distinct: data.distinct, approximate: data.approximate, partial: data.partial });
        })
        .catch(() => {
          if (!controller.signal.aborted) setState({ kind: 'failed', field: f });
        });
    }, SUGGESTION_DEBOUNCE_MS);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
    // `key` serializes every input the request reads.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  return key === null ? null : state;
}

/** The footer note for the suggestions of a field, or null. */
export function suggestionNote(state: ValueSuggestionsState | null, field: string): string | null {
  if (state === null || state.field !== field) return null;
  if (state.kind === 'loading') return 'Loading values…';
  if (state.kind === 'failed') return 'Could not load values; enter your own.';
  const notes: string[] = [];
  if (state.distinct > state.values.length) {
    const total = `${state.approximate ? 'at least ' : ''}${state.distinct.toLocaleString('en-US')}`;
    notes.push(`The ${state.values.length} most frequent of ${total} values.`);
  }
  if (state.approximate) notes.push('The counts are estimates.');
  if (state.partial) notes.push('Some sources did not answer, so the counts may be low.');
  return notes.length > 0 ? notes.join(' ') : null;
}

/**
 * The call param names in the window, most frequent first, from `/stats?group_by=param_name`, so the bar can offer
 * them as fields. Empty when the backend cannot group by param name.
 */
export function useParamNames(fromMs: number | null, toMs: number | null, caps: Capabilities | null | undefined): string[] {
  const enabled = fromMs !== null && toMs !== null && !!caps && caps.canGroupBy('param_name');
  const [names, setNames] = useState<string[]>([]);

  useEffect(() => {
    if (!enabled) {
      setNames([]);
      return;
    }
    const controller = new AbortController();
    fetchStats({ fromMs: fromMs!, toMs: toMs!, groupBy: 'param_name', limit: SUGGESTION_LIMIT }, controller.signal)
      .then((data) => setNames(data.rows.map((r) => r.key)))
      .catch(() => {
        if (!controller.signal.aborted) setNames([]);
      });
    return () => controller.abort();
  }, [enabled, fromMs, toMs]);

  return names;
}
