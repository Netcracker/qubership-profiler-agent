import { DownOutlined, RightOutlined, ZoomInOutlined } from '@ant-design/icons';
import { Alert, Button, Typography } from 'antd';
import { useRef, useState } from 'react';
import type { MouseEvent } from 'react';

import { useZone } from '../ui/timezone';
import styles from './calls-histogram.module.css';
import { formatCount, formatDurationMs, formatTsShort } from './format';
import { axisLabel, bucketAt, bucketStart, niceMax, selectionRange } from './histogram';
import type { StatsState } from './use-stats';

// Calls over time (redesign plan §4.3): stacked bars of clean and failed calls per bucket from
// `/stats?group_by=time`, with the "N calls · M errors" readout. A drag selects buckets and offers "Zoom in", which
// sets the time range; the selection does not narrow the table.

const COLLAPSED_KEY = 'profiler.histogram.collapsed';

/** `1 call`, `2 calls`. */
function counted(n: number, noun: string): string {
  return `${formatCount(n)} ${noun}${n === 1 ? '' : 's'}`;
}

function loadCollapsed(): boolean {
  try {
    return localStorage.getItem(COLLAPSED_KEY) === '1';
  } catch {
    return false;
  }
}

function saveCollapsed(collapsed: boolean): void {
  try {
    if (collapsed) localStorage.setItem(COLLAPSED_KEY, '1');
    else localStorage.removeItem(COLLAPSED_KEY);
  } catch {
    // Storage may be unavailable; the choice still holds for this page.
  }
}

export interface CallsHistogramProps {
  state: Exclude<StatsState, { kind: 'idle' } | { kind: 'hidden' }>;
  fromMs: number;
  toMs: number;
  /** Sets the time range to the selected buckets. */
  onZoom: (fromMs: number, toMs: number) => void;
  onRetry: () => void;
  /** "Hide system/proxy" filters the loaded rows only, so the chart says it still counts those calls. */
  hideSystem: boolean;
}

interface Selection {
  a: number;
  b: number;
}

export function CallsHistogram({ state, fromMs, toMs, onZoom, onRetry, hideSystem }: CallsHistogramProps) {
  const zone = useZone();
  const [collapsed, setCollapsed] = useState(loadCollapsed);
  const [hover, setHover] = useState<number | null>(null);
  const [selection, setSelection] = useState<Selection | null>(null);
  const dragFrom = useRef<number | null>(null);
  const plotRef = useRef<HTMLDivElement>(null);

  const data = state.kind === 'ready' ? state.data : null;
  const rows = data?.rows ?? [];
  const n = rows.length;
  const bucketMs = data?.bucket_ms ?? 0;
  const top = niceMax(Math.max(0, ...rows.map((r) => r.count)));

  const toggle = (): void => {
    saveCollapsed(!collapsed);
    setCollapsed(!collapsed);
  };

  const bucketOf = (e: MouseEvent): number => {
    const rect = plotRef.current!.getBoundingClientRect();
    return bucketAt(e.clientX - rect.left, rect.width, n);
  };
  const onMouseDown = (e: MouseEvent): void => {
    if (n === 0 || e.button !== 0) return;
    const i = bucketOf(e);
    dragFrom.current = i;
    setSelection({ a: i, b: i });
  };
  const onMouseMove = (e: MouseEvent): void => {
    if (n === 0) return;
    const i = bucketOf(e);
    setHover(i);
    if (dragFrom.current !== null) setSelection({ a: dragFrom.current, b: i });
  };
  const endDrag = (): void => {
    dragFrom.current = null;
  };

  const zoom = (): void => {
    if (selection === null) return;
    const range = selectionRange(rows, selection.a, selection.b, bucketMs, toMs);
    setSelection(null);
    onZoom(range.fromMs, range.toMs);
  };

  const hovered = hover !== null ? rows[hover] : undefined;
  const hoverText =
    hovered !== undefined
      ? `${formatTsShort(bucketStart(hovered), zone)} · ${counted(hovered.count, 'call')} · ${counted(hovered.errors, 'error')}` +
        (hovered.p95_ms !== null ? ` · p95 ${formatDurationMs(hovered.p95_ms)}` : '')
      : null;

  const readout =
    data !== null
      ? `${counted(data.totals.count, 'call')} · ${counted(data.totals.errors, 'error')}`
      : state.kind === 'loading'
        ? 'Counting calls…'
        : null;

  const [lo, hi] = selection === null ? [0, -1] : [Math.min(selection.a, selection.b), Math.max(selection.a, selection.b)];

  return (
    <section className={styles.histogram} aria-label="Calls over time">
      <div className={styles.head}>
        <Button
          type="text"
          size="small"
          icon={collapsed ? <RightOutlined /> : <DownOutlined />}
          aria-expanded={!collapsed}
          onClick={toggle}
        >
          <span className={styles.title}>Calls over time</span>
        </Button>
        {readout !== null ? <span className={`${styles.readout} pf-mono`}>{readout}</span> : null}
        {selection !== null && !collapsed ? (
          <>
            <Button size="small" icon={<ZoomInOutlined />} onClick={zoom}>
              Zoom in
            </Button>
            <Button size="small" type="text" onClick={() => setSelection(null)}>
              Clear
            </Button>
          </>
        ) : null}
        {hoverText !== null && !collapsed ? <span className={`${styles.hover} pf-mono`}>{hoverText}</span> : null}
      </div>
      {collapsed ? null : state.kind === 'error' ? (
        <Alert
          type="error"
          showIcon
          title="Counting the calls failed"
          description={state.message}
          action={
            <Button size="small" onClick={onRetry}>
              Retry
            </Button>
          }
        />
      ) : (
        <>
          <div className={styles.body}>
            <div className={styles.yAxis} aria-hidden="true">
              <span>{formatCount(top)}</span>
              <span>0</span>
            </div>
            <div
              ref={plotRef}
              className={styles.plot}
              data-testid="histogram-plot"
              onMouseDown={onMouseDown}
              onMouseMove={onMouseMove}
              onMouseUp={endDrag}
              onMouseLeave={() => {
                endDrag();
                setHover(null);
              }}
            >
              <svg
                viewBox={`0 0 ${Math.max(n, 1)} 100`}
                preserveAspectRatio="none"
                role="img"
                aria-label={readout ?? 'Calls over time'}
              >
                {selection !== null ? (
                  <rect className={styles.selection} x={lo} y={0} width={hi - lo + 1} height={100} data-testid="histogram-selection" />
                ) : null}
                {hover !== null ? <rect className={styles.hovered} x={hover} y={0} width={1} height={100} /> : null}
                {rows.map((r, i) => {
                  const okH = ((r.count - r.errors) / top) * 100;
                  const errH = (r.errors / top) * 100;
                  return (
                    <g key={r.key} data-testid="histogram-bar">
                      <rect className={styles.ok} x={i + 0.1} y={100 - okH} width={0.8} height={okH} />
                      <rect className={styles.err} x={i + 0.1} y={100 - okH - errH} width={0.8} height={errH} />
                    </g>
                  );
                })}
              </svg>
            </div>
            <div className={styles.xAxis} aria-hidden="true">
              <span>{axisLabel(fromMs, toMs - fromMs, zone)}</span>
              <span>{axisLabel(fromMs + (toMs - fromMs) / 2, toMs - fromMs, zone)}</span>
              <span>{axisLabel(toMs, toMs - fromMs, zone)}</span>
            </div>
          </div>
          {data?.partial ? (
            <Typography.Text type="warning" className={styles.note}>
              Some sources did not answer, so the counts may be low.
            </Typography.Text>
          ) : null}
          {hideSystem ? (
            <Typography.Text type="secondary" className={styles.note}>
              The chart counts system and proxy calls too: &quot;Hide system/proxy&quot; filters the loaded rows only.
            </Typography.Text>
          ) : null}
        </>
      )}
    </section>
  );
}
