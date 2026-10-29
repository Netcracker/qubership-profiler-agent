import { DownOutlined, SettingOutlined, UpOutlined } from '@ant-design/icons';
import { Button, Checkbox, Popover, Space } from 'antd';

import { defaultColumnPrefs } from './column-prefs';
import type { ColumnPrefs } from './column-prefs';
import { buildCallColumns } from './columns';
import styles from './column-chooser.module.css';

const COLUMN_TITLES = new Map(buildCallColumns({}).map((d) => [d.key, d.title]));

export interface ColumnSettingsProps {
  prefs: ColumnPrefs;
  onPrefsChange: (prefs: ColumnPrefs) => void;
}

/** The list of calls columns: show, hide, move, and reset to the defaults (09 §2.3). */
export function ColumnSettings({ prefs, onPrefsChange }: ColumnSettingsProps) {
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
      <Button size="small" onClick={() => onPrefsChange(defaultColumnPrefs())}>
        Reset columns
      </Button>
    </div>
  );
}

/** The "Columns" button at the right end of the table footer that opens the column settings. */
export function ColumnChooser(props: ColumnSettingsProps) {
  return (
    <Popover trigger="click" placement="topRight" content={<ColumnSettings {...props} />}>
      <Button type="text" size="small" icon={<SettingOutlined />} title="Choose columns" aria-label="Choose columns">
        Columns
      </Button>
    </Popover>
  );
}
