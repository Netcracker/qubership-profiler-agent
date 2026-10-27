import { UndoOutlined } from '@ant-design/icons';
import { Button } from 'antd';
// The library's ES build imports its own stylesheet; flame-view.module.css
// restyles it for the theme.
import flamegraph from 'd3-flame-graph';
import type { FlameGraph, FlameGraphNode } from 'd3-flame-graph';
import { select } from 'd3-selection';
import { useEffect, useMemo, useRef, useState } from 'react';

import { formatCount, formatDurationMs } from '../calls/format';
import { useThemeMode } from '../ui/theme-mode';
import { useElementWidth } from '../ui/use-element-width';
import { parseMethod } from './method-info';
import { findNodeById } from './model';
import type { CategoryDef, TreeModel, TreeNode } from './model';
import { formatShare, shareOf } from './row-metrics';
import { categoryBlockColor } from './transforms/categories';
import { toFlameData } from './transforms/flame-data';
import type { FlameDatum } from './transforms/flame-data';
import styles from './flame-view.module.css';

// The Flame graph tab (09 §3.6), drawn by d3-flame-graph: an SVG icicle with
// the whole call on top. A frame's width is its share of the call, and a
// method's callees sit under it, in call order. A click zooms on a frame and
// selects its node, which the page shares with the Call Tree tab. The view
// adds what the library leaves out: category colors from the theme, keyboard
// access, accessible names, and the selection ring.

/** Height of one row of frames, in pixels. The frame's stroke draws the gap between rows. */
const CELL_HEIGHT = 28;
/** Zoom animation. Short: the graph is a tool, and a long ease delays the next click. */
const TRANSITION_MS = 300;
/** A frame this many pixels wide or narrower is not drawn, nor is anything under it. */
const MIN_FRAME_PX = 1;

type Frame = FlameGraphNode<FlameDatum>;

interface FlameViewProps {
  model: TreeModel;
  /** The selection the page shares between this tab and the Call Tree tab. */
  selectedId: number | null;
  onSelect: (id: number | null) => void;
}

/**
 * Resolves `var(--name)` to the value the current theme gives it. An SVG
 * `fill` attribute does not take `var()`, so the colors are resolved here and
 * the graph redraws on a theme change. Without a value (jsdom applies no
 * stylesheet), the `var()` text stays, which keeps the choice testable.
 */
function resolveColor(cssValue: string): string {
  const name = /^var\((--[\w-]+)\)$/.exec(cssValue)?.[1];
  if (name === undefined) return cssValue;
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value === '' ? cssValue : value;
}

export function FlameView({ model, selectedId, onSelect }: FlameViewProps) {
  const [graphRef, width] = useElementWidth<HTMLDivElement>();
  const theme = useThemeMode();
  const chartRef = useRef<FlameGraph<FlameDatum> | null>(null);
  // The node the graph is zoomed on; null shows the whole call. Node ids
  // survive a model rebuild (Adjust, Setup), so the zoom does too.
  const [zoomedId, setZoomedId] = useState<number | null>(null);
  const zoomedRef = useRef<number | null>(null);
  const [hoverText, setHoverText] = useState<string | null>(null);

  const { names, describe } = useMemo(() => {
    const cache = new Map<number, string>();
    const names = (node: TreeNode): string => {
      let name = cache.get(node.methodIdx);
      if (name === undefined) {
        const word = model.methods[node.methodIdx] ?? '';
        const info = parseMethod(word);
        name = info.bareSignature === '' ? word : info.bareSignature.replace(/\(.*$/, '');
        cache.set(node.methodIdx, name);
      }
      return name;
    };
    // Shares are of the whole call, also when the graph is zoomed: a zoom
    // changes the picture, not what the numbers mean.
    const callMs = model.root.durationMs;
    const describe = (node: TreeNode): string =>
      `${names(node)} · ${formatDurationMs(node.durationMs)} · ${formatShare(shareOf(node.durationMs, callMs))} of the call` +
      ` · self ${formatDurationMs(node.selfDurationMs)} · ×${formatCount(node.selfExecutions)}`;
    return { names, describe };
  }, [model]);

  // The library registers its callbacks once; they read the current props here.
  const latest = useRef({ selectedId, onSelect, describe });
  latest.current = { selectedId, onSelect, describe };
  /** The selection this view made last, which tells it apart from one made in the Call Tree tab. */
  const ownSelection = useRef<number | null | undefined>(undefined);
  /** True while the view drives the zoom itself, so the click callback leaves the selection alone. */
  const quiet = useRef(false);
  /** Resolved frame colors of the current theme, by CSS value. */
  const fills = useRef(new Map<string, string>());

  const setZoomed = (id: number | null): void => {
    zoomedRef.current = id;
    setZoomedId(id);
  };

  const rootFrame = (): Frame | null => {
    const el = graphRef.current;
    if (el === null || el.querySelector('svg') === null) return null;
    return (select(el).datum() as Frame | undefined) ?? null;
  };
  const findFrame = (id: number): Frame | null => rootFrame()?.descendants().find((d) => d.data.node.id === id) ?? null;

  /** Adds keyboard access, accessible names, and the selection ring. The library redraws them away on every update. */
  const decorate = (): void => {
    const el = graphRef.current;
    if (el === null) return;
    const { selectedId: selected, describe: label } = latest.current;
    select(el)
      .selectAll<SVGGElement, Frame>('g.frame')
      .classed(styles.selected ?? 'selected', (d) => d.data.node.id === selected)
      .attr('tabindex', 0)
      .attr('role', 'button')
      .attr('aria-label', (d) => label(d.data.node))
      .attr('aria-pressed', (d) => String(d.data.node.id === selected))
      .on('focus', (_event: FocusEvent, d) => setHoverText(label(d.data.node)))
      .on('blur', () => setHoverText(null))
      .on('keydown', (event: KeyboardEvent, d) => {
        // Enter and Space do what a click does: zoom on the frame and select it.
        if (event.key !== 'Enter' && event.key !== ' ') return;
        event.preventDefault();
        chartRef.current?.zoomTo(d);
      });
  };

  /** Zooms without touching the selection; the library otherwise reports a zoom as a click. */
  const zoomQuietly = (frame: Frame | null): void => {
    const chart = chartRef.current;
    if (chart === null) return;
    quiet.current = true;
    try {
      // From the whole call: a frame hidden by the current zoom is otherwise
      // left with no width.
      chart.resetZoom();
      if (frame !== null && frame.parent !== null) chart.zoomTo(frame);
    } finally {
      quiet.current = false;
    }
    setZoomed(frame === null || frame.parent === null ? null : frame.data.node.id);
    decorate();
  };

  // One chart per mount.
  useEffect(() => {
    const el = graphRef.current;
    const chart = flamegraph<FlameDatum>()
      .inverted(true)
      .sort(false)
      .selfValue(false)
      .cellHeight(CELL_HEIGHT)
      .minFrameSize(MIN_FRAME_PX)
      .transitionDuration(TRANSITION_MS)
      .resetHeightOnZoom(true)
      .getName((d) => `${d.data.name} · ${formatDurationMs(d.data.node.durationMs)}`)
      // The frame's tooltip and the hover readout.
      .setLabelHandler((d) => latest.current.describe(d.data.node))
      .setDetailsHandler((text) => setHoverText(text))
      .setColorMapper((d) => {
        const css = d.data.category === null ? 'var(--pf-color-bar-soft)' : categoryBlockColor(d.data.category);
        let fill = fills.current.get(css);
        if (fill === undefined) {
          fill = resolveColor(css);
          fills.current.set(css, fill);
        }
        return fill;
      })
      .onClick((d) => {
        if (quiet.current) return;
        const id = d.data.node.id;
        setZoomed(d.parent === null ? null : id);
        // A second click on the selected frame clears the selection, as in the tree.
        const next = latest.current.selectedId === id ? null : id;
        ownSelection.current = next;
        latest.current.onSelect(next);
        decorate();
      });
    chartRef.current = chart;
    return () => {
      chart.destroy();
      chartRef.current = null;
      if (el !== null) select(el).datum(null);
    };
    // decorate and setZoomed read refs only; the chart lives as long as the view.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // The data: rebuilt with the model, which Adjust and Setup replace.
  useEffect(() => {
    const el = graphRef.current;
    const chart = chartRef.current;
    if (el === null || chart === null) return;
    const data = toFlameData(model.root, names);
    chart.width(width);
    if (el.querySelector('svg') === null) select(el).datum(data).call(chart);
    else chart.update(data);
    const zoomed = zoomedRef.current;
    if (zoomed !== null) zoomQuietly(findFrame(zoomed));
    else decorate();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, names]);

  // Width and theme: a redraw of the same data, which keeps the zoom.
  useEffect(() => {
    const chart = chartRef.current;
    if (chart === null) return;
    fills.current = new Map();
    chart.width(width).update();
    decorate();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [width, theme]);

  // A selection made in the Call Tree tab zooms on its frame. One made here
  // is zoomed on already.
  useEffect(() => {
    if (selectedId !== ownSelection.current) {
      ownSelection.current = selectedId;
      if (selectedId !== null) {
        const frame = findFrame(selectedId);
        if (frame !== null) zoomQuietly(frame);
      }
    }
    decorate();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId]);

  // The categories on screen, in the order a walk of the call meets them.
  const legend = useMemo(() => {
    const seen = new Set<CategoryDef>();
    const visit = (node: TreeNode): void => {
      if (node.category !== undefined) seen.add(node.category);
      node.children.forEach(visit);
    };
    visit(model.root);
    return [...seen];
  }, [model]);

  const selectedNode = selectedId === null ? null : findNodeById(model.root, selectedId);
  const zoomedNode = zoomedId === null ? null : findNodeById(model.root, zoomedId);

  return (
    <div className={styles.flame}>
      <div className={styles.infoRow}>
        <span className={`pf-mono ${styles.readout}`}>
          {hoverText ??
            (selectedNode !== null
              ? describe(selectedNode)
              : 'Hover a frame for its numbers. Click one to zoom in on it and select it.')}
        </span>
        {legend.map((category) => (
          <span key={category.name} className={styles.legend}>
            <span className={styles.swatch} style={{ background: categoryBlockColor(category) }} />
            {category.name}
          </span>
        ))}
        <span className={styles.legend}>
          <span className={styles.swatch} />
          No category
        </span>
      </div>
      {zoomedNode !== null ? (
        <div className={styles.zoomRow}>
          <span>
            Zoomed in on <span className="pf-mono">{names(zoomedNode)}</span>.
          </span>
          <Button size="small" type="link" icon={<UndoOutlined />} onClick={() => zoomQuietly(null)}>
            Reset zoom
          </Button>
        </div>
      ) : null}
      <div className={styles.scroller}>
        <div ref={graphRef} className={styles.graph} />
      </div>
      <div className={styles.caption}>
        The top frame is the whole call. A frame&apos;s width is its share of the call, and its children sit under it,
        in call order. The part of a frame with nothing under it is the method&apos;s own time. Click a frame to zoom
        in on it; the frames above it then stay as a faded path back.
      </div>
    </div>
  );
}
