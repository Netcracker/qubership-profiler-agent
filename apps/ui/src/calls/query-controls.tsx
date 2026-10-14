import { Button, Radio, Select, Space, Switch, Typography } from 'antd';
import type { ReactNode } from 'react';

import { parseDurationMs } from './duration-filter';
import type { Condition } from '../filter/condition';
import type { CallsOrder } from '../url/search-params';
import styles from './query-controls.module.css';

// The controls under the filter bar (CallsAlt mockup): the duration chips, "Errors only", "Hide system/proxy", and
// "Run query" with the hint that the draft differs from the applied query. Like the bar, they edit the draft.

export const DURATION_CHIPS = [
  { label: 'All', value: '' },
  { label: '>10ms', value: '10ms' },
  { label: '>100ms', value: '100ms' },
  { label: '>500ms', value: '500ms' },
  { label: '>3s', value: '3s' },
  { label: '>5s', value: '5s' },
];

/**
 * The chip the conditions select: "All" with no duration condition, a threshold when exactly one `duration >`
 * condition has its value, and none otherwise.
 */
export function selectedChip(conditions: readonly Condition[]): string | undefined {
  const durations = conditions.filter((c) => c.field === 'duration');
  if (durations.length === 0) return '';
  const only = durations[0]!;
  if (durations.length > 1 || only.op !== '>') return undefined;
  const ms = parseDurationMs(only.values[0] ?? '');
  return DURATION_CHIPS.find((chip) => chip.value !== '' && parseDurationMs(chip.value) === ms)?.value;
}

/** Replaces every duration condition with the chip's one; "All" removes them. */
export function withChip(conditions: readonly Condition[], value: string): Condition[] {
  const rest = conditions.filter((c) => c.field !== 'duration');
  return value === '' ? rest : [{ field: 'duration', op: '>', values: [value] }, ...rest];
}

const ERRORS_ONLY: Condition = { field: 'error', op: '=', values: ['true'] };

const isErrorsOnly = (c: Condition): boolean => c.field === 'error' && c.op === '=' && c.values[0] === 'true';

/** "Errors only" is the condition `error=true`; the switch shows whether it is present. */
export function hasErrorsOnly(conditions: readonly Condition[]): boolean {
  return conditions.some(isErrorsOnly);
}

export function withErrorsOnly(conditions: readonly Condition[], on: boolean): Condition[] {
  const rest = conditions.filter((c) => !isErrorsOnly(c));
  return on ? [...rest, ERRORS_ONLY] : rest;
}

export interface QueryControlsProps {
  conditions: readonly Condition[];
  onConditionsChange: (next: Condition[]) => void;
  hideSystem: boolean;
  onHideSystemChange: (on: boolean) => void;
  dirty: boolean;
  running: boolean;
  disabled: boolean;
  onRun: () => void;
}

export function QueryControls({
  conditions,
  onConditionsChange,
  hideSystem,
  onHideSystemChange,
  dirty,
  running,
  disabled,
  onRun,
}: QueryControlsProps) {
  return (
    <div className={styles.row}>
      <Typography.Text type="secondary">Duration</Typography.Text>
      <Radio.Group
        optionType="button"
        aria-label="Minimum duration"
        value={selectedChip(conditions)}
        onChange={(e) => onConditionsChange(withChip(conditions, e.target.value as string))}
        options={DURATION_CHIPS}
      />
      <label className={styles.switch}>
        <Switch
          size="small"
          checked={hasErrorsOnly(conditions)}
          onChange={(on) => onConditionsChange(withErrorsOnly(conditions, on))}
        />
        Errors only
      </label>
      <label className={styles.switch}>
        <Switch size="small" checked={hideSystem} onChange={onHideSystemChange} />
        Hide system/proxy
      </label>
      <div className={styles.run}>
        {dirty ? <Typography.Text type="secondary">Filters changed. Run the query to update the results.</Typography.Text> : null}
        <Button type="primary" onClick={onRun} loading={running} disabled={disabled} title="Ctrl+Enter in the filter bar also runs the query">
          Run query
        </Button>
      </div>
    </div>
  );
}

const SORT_OPTIONS: { value: CallsOrder; label: string }[] = [{ value: 'start_desc', label: 'Start time' }];

/** "Sort by": /calls sorts by start time, newest first, until the `order` parameter lands (redesign plan §4.2.1). */
export function SortBy({ value, extra }: { value: CallsOrder; extra?: ReactNode }) {
  return (
    <Space.Compact>
      <Select<CallsOrder> aria-label="Sort by" prefix="Sort by" value={value} options={SORT_OPTIONS} className={styles.sort} />
      <Button disabled title="Newest first. The backend sorts in this direction only." aria-label="Newest first">
        <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true" className={styles.sortIcon}>
          <path d="M4 3v10M1.5 10.5 4 13l2.5-2.5M9 4h5.5M9 8h4M9 12h2.5" />
        </svg>
      </Button>
      {extra}
    </Space.Compact>
  );
}
