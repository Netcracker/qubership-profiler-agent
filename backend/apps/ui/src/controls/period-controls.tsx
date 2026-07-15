import { Button, DatePicker, Radio, Space } from 'antd';
import dayjs from 'dayjs';
import { useState } from 'react';

// Period picker + quick ranges + Apply (09 §2.2). Edits are a draft; nothing
// refetches the calls fan-out until Apply commits the window to the URL.

export interface DraftWindow {
  fromMs: number | null;
  toMs: number | null;
}

const QUICK_RANGES = [
  { key: '15m', label: '15 min', ms: 15 * 60 * 1000 },
  { key: '1h', label: '1 h', ms: 60 * 60 * 1000 },
  { key: '2h', label: '2 h', ms: 2 * 60 * 60 * 1000 },
  { key: '4h', label: '4 h', ms: 4 * 60 * 60 * 1000 },
];

interface PeriodControlsProps {
  window: DraftWindow;
  onWindowChange: (window: DraftWindow) => void;
  onApply: () => void;
  applying: boolean;
}

export function PeriodControls({ window, onWindowChange, onApply, applying }: PeriodControlsProps) {
  // Which quick range filled the draft; manual picker edits clear it.
  const [quickKey, setQuickKey] = useState<string | null>(null);

  const value: [dayjs.Dayjs | null, dayjs.Dayjs | null] = [
    window.fromMs === null ? null : dayjs(window.fromMs),
    window.toMs === null ? null : dayjs(window.toMs),
  ];

  return (
    <Space wrap>
      <DatePicker.RangePicker
        showTime
        allowClear={false}
        value={value}
        onChange={(range) => {
          setQuickKey(null);
          onWindowChange({
            fromMs: range?.[0]?.valueOf() ?? null,
            toMs: range?.[1]?.valueOf() ?? null,
          });
        }}
      />
      <Radio.Group
        optionType="button"
        value={quickKey}
        onChange={(e) => {
          const range = QUICK_RANGES.find((r) => r.key === e.target.value);
          if (range === undefined) return;
          setQuickKey(range.key);
          const now = Date.now();
          onWindowChange({ fromMs: now - range.ms, toMs: now });
        }}
        options={QUICK_RANGES.map((r) => ({ label: r.label, value: r.key }))}
      />
      <Button
        type="primary"
        onClick={onApply}
        loading={applying}
        disabled={window.fromMs === null || window.toMs === null}
      >
        Apply
      </Button>
    </Space>
  );
}
