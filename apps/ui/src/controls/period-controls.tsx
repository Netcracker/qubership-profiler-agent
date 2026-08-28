import {
  ClockCircleOutlined,
  DoubleLeftOutlined,
  DoubleRightOutlined,
  DownOutlined,
  GlobalOutlined,
  SearchOutlined,
  ZoomInOutlined,
  ZoomOutOutlined,
} from '@ant-design/icons';
import { Button, Input, Popover, Select, Space, Typography, theme } from 'antd';
import { useEffect, useMemo, useRef, useState } from 'react';

import {
  QUICK_RANGES,
  ZOOM_IN_FACTOR,
  ZOOM_OUT_FACTOR,
  describeWindow,
  exprFromMs,
  isRelativeExpr,
  resolveTimeExpr,
  shiftWindow,
  toUrlToken,
  zoneOffsetLabel,
  zoneOptions,
  zoomWindow,
} from './time-range';
import type { DraftWindow, QuickRange, TimeRange } from './time-range';
import { setZone as commitZone, useZone } from '../ui/timezone';
import styles from './period-controls.module.css';

export type { DraftWindow, TimeRange } from './time-range';

/** The raw URL tokens a range change commits: relative expr or epoch-ms string. */
export interface RangeTokens {
  from: string;
  to: string;
}

// Grafana-style period picker (09 §2.2). The trigger opens a popover with an
// absolute From/To range, searchable quick ranges, and a zone selector; flanking
// it are Grafana's navigation controls (shift back/forward, zoom). Choosing a
// range applies it at once, the way Grafana refetches on a range change. The
// sibling Apply stays for committing rail-selection edits (still draft-gated).

interface PeriodControlsProps {
  value: TimeRange;
  /** A range change: commit the raw tokens (relative stays live in the URL). */
  onCommitRange: (tokens: RangeTokens) => void;
  /** The sibling Apply: commit the current rail-selection edits. */
  onApply: () => void;
  /** Whether the rail selection differs from the committed one — Apply's dirty state. */
  canApply: boolean;
  applying: boolean;
}

export function PeriodControls({ value, onCommitRange, onApply, canApply, applying }: PeriodControlsProps) {
  const hasWindow = value.fromMs !== null && value.toMs !== null;

  // Shifting/zooming resolves the live window, so it commits absolute ms — a
  // nudged range is no longer "the last N hours".
  const nudge = (next: DraftWindow): void => onCommitRange(windowTokens(next));
  const ms: DraftWindow = { fromMs: value.fromMs, toMs: value.toMs };

  return (
    <Space wrap size={4}>
      <Button
        icon={<DoubleLeftOutlined />}
        aria-label="Move time range backwards"
        disabled={!hasWindow}
        onClick={() => nudge(shiftWindow(ms, -1))}
      />
      <TimeRangePicker value={value} onCommit={onCommitRange} />
      <Button
        icon={<DoubleRightOutlined />}
        aria-label="Move time range forwards"
        disabled={!hasWindow}
        onClick={() => nudge(shiftWindow(ms, 1))}
      />
      <Button
        icon={<ZoomOutOutlined />}
        aria-label="Zoom out time range"
        disabled={!hasWindow}
        onClick={() => nudge(zoomWindow(ms, ZOOM_OUT_FACTOR))}
      />
      <Button
        icon={<ZoomInOutlined />}
        aria-label="Zoom in time range"
        disabled={!hasWindow}
        onClick={() => nudge(zoomWindow(ms, ZOOM_IN_FACTOR))}
      />
      {/* Enabled only when there is unwritten selection work — a dirty state, so
          the button documents itself: grey means "everything is applied". */}
      <Button type="primary" onClick={onApply} loading={applying} disabled={!hasWindow || !canApply}>
        Apply
      </Button>
    </Space>
  );
}

function windowTokens(w: DraftWindow): RangeTokens {
  return { from: w.fromMs === null ? '' : String(w.fromMs), to: w.toMs === null ? '' : String(w.toMs) };
}

const ACCENT = '#ff5b2d'; // Grafana's selected-range marker (a deliberate brand colour, not a theme token).

function TimeRangePicker({ value, onCommit }: { value: TimeRange; onCommit: (tokens: RangeTokens) => void }) {
  const { token } = theme.useToken();
  // The app-wide display zone (persisted). Changing it applies at once and is not
  // part of the cancelable draft — Escape/outside-click do not revert it.
  const zone = useZone();
  const [open, setOpen] = useState(false);
  const [fromExpr, setFromExpr] = useState(() => fieldFromToken(value.from, zone));
  const [toExpr, setToExpr] = useState(() => fieldFromToken(value.to, zone));
  const [fromError, setFromError] = useState(false);
  const [toError, setToError] = useState(false);
  const [orderError, setOrderError] = useState(false);
  const [filter, setFilter] = useState('');
  // The tokens that produced the *committed* window. The trigger label reads
  // these, not the live fields, so an unapplied field edit cannot mislabel it.
  const [applied, setApplied] = useState<RangeTokens>(() => ({ from: value.from ?? '', to: value.to ?? '' }));

  // The last tokens this picker emitted. Comparing exact strings (not resolved
  // ms) means our own commit echoing back through the URL never trips a resync.
  const emitted = useRef<{ from: string | null; to: string | null }>({ from: value.from, to: value.to });
  // Read (not depend on) the zone inside the window-resync effect; changeZone
  // handles zone changes itself, so the effect must not re-run for them.
  const zoneRef = useRef(zone);
  zoneRef.current = zone;

  // Resync the fields when the committed window changes under us (back/forward, a
  // shared link, a navigation nudge) — but not when it is our own change coming
  // back, or a relative expression the user just picked would flatten.
  useEffect(() => {
    if (value.from === emitted.current.from && value.to === emitted.current.to) return;
    const z = zoneRef.current;
    setFromExpr(fieldFromToken(value.from, z));
    setToExpr(fieldFromToken(value.to, z));
    setApplied({ from: value.from ?? '', to: value.to ?? '' });
    setFromError(false);
    setToError(false);
    setOrderError(false);
  }, [value.from, value.to]);

  // Re-seed the fields from the committed window each time the popover opens, so
  // closing it (Escape or an outside click) discards any unapplied window edit.
  // Reset on open, not close, or an Apply's own echo would be clobbered mid-commit.
  useEffect(() => {
    if (!open) return;
    setFromExpr(fieldFromToken(value.from, zone));
    setToExpr(fieldFromToken(value.to, zone));
    setFromError(false);
    setToError(false);
    setOrderError(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // The click-triggered Popover does not close on Escape on its own, and antd's
  // own inputs swallow the key before it bubbles — so listen in the capture
  // phase, ahead of them. Let an open zone dropdown consume the first Escape.
  useEffect(() => {
    if (!open) return undefined;
    const onKey = (e: KeyboardEvent): void => {
      if (e.key !== 'Escape') return;
      if (document.querySelector('.ant-select-dropdown:not(.ant-select-dropdown-hidden)')) return;
      setOpen(false);
    };
    document.addEventListener('keydown', onKey, true);
    return () => document.removeEventListener('keydown', onKey, true);
  }, [open]);

  const emit = (tokens: RangeTokens): void => {
    emitted.current = tokens;
    setApplied(tokens);
    onCommit(tokens);
    setOpen(false);
  };

  const applyAbsolute = (): void => {
    const now = Date.now();
    const fromMs = resolveTimeExpr(fromExpr, now, zone);
    const toMs = resolveTimeExpr(toExpr, now, zone);
    const badOrder = fromMs !== null && toMs !== null && fromMs >= toMs;
    setFromError(fromMs === null || badOrder);
    setToError(toMs === null || badOrder);
    setOrderError(badOrder);
    if (fromMs === null || toMs === null || badOrder) return;
    // A relative field (`now-6h`) stays relative in the URL; an absolute one commits its ms.
    emit({ from: toUrlToken(fromExpr, fromMs), to: toUrlToken(toExpr, toMs) });
  };

  const applyQuick = (range: QuickRange): void => {
    setFromExpr(range.from);
    setToExpr(range.to);
    setFromError(false);
    setToError(false);
    setOrderError(false);
    emit({ from: range.from, to: range.to });
  };

  // Changing the zone applies immediately (app-wide, persisted). Absolute fields
  // re-render at the same instant in the new zone; relative expressions are
  // zone-independent and stay verbatim.
  const changeZone = (next: string): void => {
    const now = Date.now();
    setFromExpr((expr) => reprojectExpr(expr, now, zone, next));
    setToExpr((expr) => reprojectExpr(expr, now, zone, next));
    commitZone(next);
  };

  const needle = filter.trim().toLowerCase();
  const matches = QUICK_RANGES.filter((r) => r.label.toLowerCase().includes(needle));
  // Stable when no window is set, so the zone list is not rebuilt every render.
  const referenceMs = useMemo(() => value.toMs ?? Date.now(), [value.toMs]);

  const content = (
    <div className={styles.panel}>
      <div className={styles.columns}>
        <div className={styles.absoluteCol}>
          <Typography.Text strong>Absolute time range</Typography.Text>
          <div className={styles.field}>
            <FieldLabel>From</FieldLabel>
            <TimeField value={fromExpr} error={fromError} onChange={setFromExpr} onSubmit={applyAbsolute} />
          </div>
          <div className={styles.fieldTight}>
            <FieldLabel>To</FieldLabel>
            <TimeField value={toExpr} error={toError} onChange={setToExpr} onSubmit={applyAbsolute} />
          </div>
          {orderError ? (
            <Typography.Text type="danger" className={styles.errorText}>
              “From” must be before “To”.
            </Typography.Text>
          ) : fromError || toError ? (
            <Typography.Text type="danger" className={styles.errorText}>
              Enter an absolute time or a relative expression like <Typography.Text code>now-6h</Typography.Text>.
            </Typography.Text>
          ) : null}
          <Button type="primary" block onClick={applyAbsolute} className={styles.applyBtn}>
            Apply time range
          </Button>
        </div>
        <div
          className={styles.quickCol}
          style={{ borderInlineStart: `1px solid ${token.colorBorderSecondary}` }}
        >
          <Input
            size="small"
            allowClear
            prefix={<SearchOutlined />}
            placeholder="Search quick ranges"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          <div className={styles.quickList}>
            {matches.length === 0 ? (
              <Typography.Text type="secondary" className={styles.hint}>
                No matching ranges
              </Typography.Text>
            ) : (
              matches.map((range) => (
                <QuickRangeItem
                  key={range.label}
                  range={range}
                  selected={range.from === applied.from && range.to === applied.to}
                  onSelect={applyQuick}
                />
              ))
            )}
          </div>
        </div>
      </div>
      <div className={styles.zoneRow} style={{ borderTop: `1px solid ${token.colorBorderSecondary}` }}>
        <ZoneSelect zone={zone} referenceMs={referenceMs} onChange={changeZone} />
      </div>
    </div>
  );

  return (
    <Popover trigger="click" placement="bottomLeft" open={open} onOpenChange={setOpen} content={content}>
      <Button icon={<ClockCircleOutlined />} aria-label="Time range">
        <span className={styles.triggerLabel}>
          {describeWindow(value, applied.from, applied.to, zone)}
        </span>
        <DownOutlined className={styles.caret} />
      </Button>
    </Popover>
  );
}

// Re-express a field at the same instant when the zone changes: absolute text
// moves zones, relative text (`now-6h`) and unparseable drafts stay put.
function reprojectExpr(expr: string, nowMs: number, fromZone: string, toZone: string): string {
  if (isRelativeExpr(expr)) return expr;
  const ms = resolveTimeExpr(expr, nowMs, fromZone);
  return ms === null ? expr : exprFromMs(ms, toZone);
}

// A URL token as an editable field value: a relative token stays verbatim, an
// epoch-ms token renders as an absolute timestamp in the current zone.
function fieldFromToken(token: string | null, zone: string): string {
  if (token === null || token.trim() === '') return '';
  const t = token.trim();
  if (isRelativeExpr(t)) return t;
  const ms = Number(t);
  return Number.isFinite(ms) ? exprFromMs(ms, zone) : t;
}

interface ZoneRow {
  value: string;
  label: string;
  offset: string;
}

// The zone picker is an antd Select rendered *into the popover* (not the default
// body portal) so opening its dropdown does not dismiss the popover. That buys
// search, keyboard support, option virtualisation and theme tokens for free.
function ZoneSelect({
  zone,
  referenceMs,
  onChange,
}: {
  zone: string;
  referenceMs: number;
  onChange: (zone: string) => void;
}) {
  const { token } = theme.useToken();
  // Offsets are DST-correct at referenceMs; recompute only when it moves.
  const rows = useMemo<ZoneRow[]>(
    () => zoneOptions().map((o) => ({ value: o.value, label: o.label, offset: zoneOffsetLabel(o.value, referenceMs) })),
    [referenceMs],
  );
  const selectedOffset = rows.find((r) => r.value === zone)?.offset;

  return (
    <div className={styles.zoneSelectRow}>
      <GlobalOutlined style={{ color: token.colorTextTertiary }} />
      <Select<string, ZoneRow>
        showSearch
        size="small"
        aria-label="Time zone"
        value={zone}
        onChange={onChange}
        options={rows}
        className={styles.zoneSelect}
        // Render the dropdown inside the popover so selecting a zone keeps it open.
        getPopupContainer={(node) => node.parentElement ?? document.body}
        // Match the name, or the numeric offset (`+03` finds UTC+03:00) — but not
        // the "UTC" prefix every offset shares.
        filterOption={(input, option) => {
          const q = input.trim().toLowerCase();
          return option !== undefined && (option.label.toLowerCase().includes(q) || option.offset.replace('UTC', '').includes(q));
        }}
        optionRender={(option) => (
          <div className={styles.zoneOption}>
            <span className={styles.ellipsis}>{option.data.label}</span>
            <Typography.Text type="secondary" className={styles.hint}>
              {option.data.offset}
            </Typography.Text>
          </div>
        )}
      />
      <Typography.Text type="secondary" className={styles.hintNowrap}>
        {selectedOffset}
      </Typography.Text>
    </div>
  );
}

function FieldLabel({ children }: { children: string }) {
  return (
    <Typography.Text type="secondary" className={styles.fieldLabel}>
      {children}
    </Typography.Text>
  );
}

// A From/To field. It accepts an absolute timestamp (`2026-07-13 12:00:00`) or a
// relative expression (`now`, `now-6h`); both resolve on Apply. Enter applies.
function TimeField({
  value,
  error,
  onChange,
  onSubmit,
}: {
  value: string;
  error: boolean;
  onChange: (value: string) => void;
  onSubmit: () => void;
}) {
  return (
    <Input
      value={value}
      status={error ? 'error' : undefined}
      placeholder="now-6h or 2026-07-13 12:00:00"
      onChange={(e) => onChange(e.target.value)}
      onPressEnter={onSubmit}
    />
  );
}

function QuickRangeItem({
  range,
  selected,
  onSelect,
}: {
  range: QuickRange;
  selected: boolean;
  onSelect: (range: QuickRange) => void;
}) {
  const { token } = theme.useToken();
  const [hovered, setHovered] = useState(false);
  return (
    <div
      role="button"
      tabIndex={0}
      onClick={() => onSelect(range)}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          onSelect(range);
        }
      }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      className={styles.quickItem}
      style={{
        borderInlineStart: `2px solid ${selected ? ACCENT : 'transparent'}`,
        background: selected || hovered ? token.colorFillTertiary : undefined,
      }}
    >
      {range.label}
    </div>
  );
}
