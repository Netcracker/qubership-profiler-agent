import { DownOutlined, SettingOutlined, UpOutlined } from '@ant-design/icons';
import { Button, Checkbox, Input, Popover, Radio, Space, Switch, Typography } from 'antd';
import { useEffect, useState } from 'react';

import type { CallsSearchState } from '../url/search-params';
import { defaultColumnPrefs } from './column-prefs';
import type { ColumnPrefs } from './column-prefs';
import { buildCallColumns, DEFAULT_COLUMN_ORDER } from './columns';
import { formatDurationFilter, parseDurationFilter } from './duration-filter';
import styles from './calls-toolbar.module.css';

// Calls filter bar (09 §2.3): duration chips (>500ms default) plus a free-text
// duration expression, errors-only, hide system/proxy, method-substring query,
// column management. These narrow an already-applied window, so they commit to
// the URL — and refetch — immediately; the expensive selection + period setup
// stays Apply-gated.

export const DURATION_CHIPS = [
  { label: 'All', value: 0 },
  { label: '>10ms', value: 10 },
  { label: '>100ms', value: 100 },
  { label: '>500ms', value: 500 },
  { label: '>3s', value: 3000 },
  { label: '>5s', value: 5000 },
];

/** The chip and the text field share one filter; a preset sets a lower bound. */
function durationText(search: CallsSearchState): string {
  return formatDurationFilter({ minMs: search.durationMinMs || null, maxMs: search.durationMaxMs || null });
}

interface CallsToolbarProps {
  search: CallsSearchState;
  onSearchChange: (next: CallsSearchState) => void;
  prefs: ColumnPrefs;
  onPrefsChange: (prefs: ColumnPrefs) => void;
  disabled: boolean;
}

const COLUMN_TITLES = new Map(buildCallColumns({}).map((d) => [d.key, d.title]));

function ColumnSettings({ prefs, onPrefsChange }: Pick<CallsToolbarProps, 'prefs' | 'onPrefsChange'>) {
  const move = (index: number, delta: number): void => {
    const next = [...prefs.order];
    const target = index + delta;
    if (target < 0 || target >= next.length) return;
    const [item] = next.splice(index, 1);
    next.splice(target, 0, item!);
    onPrefsChange({ ...prefs, order: next });
  };
  // Hiding the last visible column leaves a blank table with loaded rows and
  // no visible way back, so that last checkbox stays checked and disabled.
  const visibleCount = prefs.order.length - prefs.hidden.length;
  return (
    <div className={styles.columnSettings}>
      {prefs.order.map((key, i) => {
        const visible = !prefs.hidden.includes(key);
        const title = COLUMN_TITLES.get(key) ?? key;
        return (
          <Space key={key} className={styles.rowBetween}>
            <Checkbox
              checked={visible}
              disabled={visible && visibleCount === 1}
              onChange={(e) =>
                onPrefsChange({
                  ...prefs,
                  hidden: e.target.checked ? prefs.hidden.filter((k) => k !== key) : [...prefs.hidden, key],
                })
              }
            >
              {title}
            </Checkbox>
            <Space size={0}>
              <Button
                type="text"
                size="small"
                icon={<UpOutlined />}
                disabled={i === 0}
                aria-label={`Move ${title} column up`}
                onClick={() => move(i, -1)}
              />
              <Button
                type="text"
                size="small"
                icon={<DownOutlined />}
                disabled={i === prefs.order.length - 1}
                aria-label={`Move ${title} column down`}
                onClick={() => move(i, 1)}
              />
            </Space>
          </Space>
        );
      })}
      <Button size="small" onClick={() => onPrefsChange(defaultColumnPrefs(DEFAULT_COLUMN_ORDER))}>
        Reset columns
      </Button>
    </div>
  );
}

export function CallsToolbar({ search, onSearchChange, prefs, onPrefsChange, disabled }: CallsToolbarProps) {
  // The query commits on Enter or the search button, not per keystroke.
  const [queryDraft, setQueryDraft] = useState(search.query);

  // The duration expression (old-UI grammar: >400ms, <100ms, 100ms..200ms)
  // commits on Enter or blur; a preset chip fills it too. `null` bounds mean
  // "unbounded", which the URL stores as 0.
  const [durationDraft, setDurationDraft] = useState(() => durationText(search));
  const [durationError, setDurationError] = useState(false);

  // A preset chip is "selected" only when the committed filter is exactly that
  // lower bound with no upper bound; a custom expression selects no chip.
  const selectedChip =
    search.durationMaxMs === 0 ? DURATION_CHIPS.find((c) => c.value === search.durationMinMs)?.value : undefined;

  // Resync the draft when the committed filter changes under us (a preset
  // click, back/forward, a shared link), the same way the query draft does.
  useEffect(() => {
    setDurationDraft(formatDurationFilter({ minMs: search.durationMinMs || null, maxMs: search.durationMaxMs || null }));
    setDurationError(false);
  }, [search.durationMinMs, search.durationMaxMs]);

  const commitDuration = (raw: string): void => {
    const bound = parseDurationFilter(raw);
    if (bound === null) {
      setDurationError(true);
      return;
    }
    onSearchChange({ ...search, durationMinMs: bound.minMs ?? 0, durationMaxMs: bound.maxMs ?? 0 });
  };

  // Resync the draft when the committed query changes under us (back/forward,
  // a shared link, a banner action), or the input would keep showing stale text.
  useEffect(() => {
    setQueryDraft(search.query);
  }, [search.query]);

  // An empty field reads as "no filter" to the user, so clearing it — by
  // backspacing or the input's own clear button — commits immediately instead
  // of waiting for Enter/search, unlike a non-empty edit.
  const handleQueryChange = (value: string): void => {
    setQueryDraft(value);
    if (value === '' && search.query !== '') {
      onSearchChange({ ...search, query: '' });
    }
  };

  return (
    <Space wrap className={styles.toolbar}>
      <Radio.Group
        optionType="button"
        size="small"
        value={selectedChip}
        disabled={disabled}
        onChange={(e) => onSearchChange({ ...search, durationMinMs: e.target.value as number, durationMaxMs: 0 })}
        options={DURATION_CHIPS.map((c) => ({ label: c.label, value: c.value }))}
      />
      <Input
        size="small"
        className={styles.durationInput}
        status={durationError ? 'error' : undefined}
        placeholder=">400ms · 100ms..200ms"
        aria-label="Duration filter"
        value={durationDraft}
        disabled={disabled}
        onChange={(e) => {
          setDurationDraft(e.target.value);
          setDurationError(false);
        }}
        onPressEnter={() => commitDuration(durationDraft)}
        // Blur reverts an unparseable draft to the committed value so the field
        // never sticks on invalid text the user has walked away from.
        onBlur={() => (parseDurationFilter(durationDraft) === null ? setDurationDraft(durationText(search)) : commitDuration(durationDraft))}
      />
      <Space size={4}>
        <Switch
          size="small"
          checked={search.errorOnly}
          disabled={disabled}
          onChange={(checked) => onSearchChange({ ...search, errorOnly: checked })}
        />
        <Typography.Text>Errors only</Typography.Text>
      </Space>
      <Space size={4}>
        <Switch
          size="small"
          checked={search.hideSystem}
          disabled={disabled}
          onChange={(checked) => onSearchChange({ ...search, hideSystem: checked })}
        />
        <Typography.Text>Hide system/proxy</Typography.Text>
      </Space>
      <Input.Search
        placeholder="Method substring"
        allowClear
        size="small"
        className={styles.methodInput}
        value={queryDraft}
        disabled={disabled}
        onChange={(e) => handleQueryChange(e.target.value)}
        onSearch={(value) => onSearchChange({ ...search, query: value })}
      />
      <Popover trigger="click" content={<ColumnSettings prefs={prefs} onPrefsChange={onPrefsChange} />}>
        <Button size="small" icon={<SettingOutlined />}>
          Columns
        </Button>
      </Popover>
    </Space>
  );
}
