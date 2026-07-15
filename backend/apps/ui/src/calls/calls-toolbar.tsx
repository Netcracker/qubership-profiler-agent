import { DownOutlined, SettingOutlined, UpOutlined } from '@ant-design/icons';
import { Button, Checkbox, Input, Popover, Radio, Space, Switch, Typography } from 'antd';
import { useState } from 'react';

import type { CallsSearchState } from '../url/search-params';
import type { ColumnPrefs } from './column-prefs';
import { buildCallColumns } from './columns';

// Calls filter bar (09 §2.3): duration chips (>500ms default), errors-only,
// hide system/proxy, method-substring query, column management. These narrow
// an already-applied window, so they commit to the URL — and refetch —
// immediately; the expensive selection + period setup stays Apply-gated.

export const DURATION_CHIPS = [
  { label: 'All', value: 0 },
  { label: '>10ms', value: 10 },
  { label: '>100ms', value: 100 },
  { label: '>500ms', value: 500 },
  { label: '>3s', value: 3000 },
  { label: '>5s', value: 5000 },
];

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
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 220 }}>
      {prefs.order.map((key, i) => (
        <Space key={key} style={{ justifyContent: 'space-between', width: '100%' }}>
          <Checkbox
            checked={!prefs.hidden.includes(key)}
            onChange={(e) =>
              onPrefsChange({
                ...prefs,
                hidden: e.target.checked ? prefs.hidden.filter((k) => k !== key) : [...prefs.hidden, key],
              })
            }
          >
            {COLUMN_TITLES.get(key) ?? key}
          </Checkbox>
          <Space size={0}>
            <Button type="text" size="small" icon={<UpOutlined />} disabled={i === 0} onClick={() => move(i, -1)} />
            <Button
              type="text"
              size="small"
              icon={<DownOutlined />}
              disabled={i === prefs.order.length - 1}
              onClick={() => move(i, 1)}
            />
          </Space>
        </Space>
      ))}
    </div>
  );
}

export function CallsToolbar({ search, onSearchChange, prefs, onPrefsChange, disabled }: CallsToolbarProps) {
  // The query commits on Enter or the search button, not per keystroke.
  const [queryDraft, setQueryDraft] = useState(search.query);

  return (
    <Space wrap style={{ padding: '8px 0' }}>
      <Radio.Group
        optionType="button"
        size="small"
        value={search.durationMinMs}
        disabled={disabled}
        onChange={(e) => onSearchChange({ ...search, durationMinMs: e.target.value as number })}
        options={DURATION_CHIPS.map((c) => ({ label: c.label, value: c.value }))}
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
        style={{ width: 260 }}
        value={queryDraft}
        disabled={disabled}
        onChange={(e) => setQueryDraft(e.target.value)}
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
