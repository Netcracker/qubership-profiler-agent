import { useMemo, useState } from 'react';

import { formatCount, formatDurationMs } from '../calls/format';
import { useElementWidth } from '../ui/use-element-width';
import { parseMethod } from './method-info';
import { findNodeById } from './model';
import type { CategoryDef, TreeModel, TreeNode } from './model';
import { formatShare, shareOf } from './row-metrics';
import { categoryBlockColor } from './transforms/categories';
import { flameLayout } from './transforms/flame';
import styles from './flame-view.module.css';

// The Flame graph tab (09 §3.6): the call tree as blocks. A block's width is
// its share of the call, and a method's callees sit under it, in call order.
// Blocks are plain DOM buttons, so focus, the tooltip, and the theme come for
// free; the layout leaves out every block under one pixel wide, which bounds a
// row at one block per pixel.

/** Height of one row of blocks, in pixels. The block itself is 2 px shorter. */
const ROW_HEIGHT = 28;
/** A block narrower than this many pixels shows no label; its tooltip still names it. */
const LABEL_MIN_PX = 40;

interface FlameViewProps {
  model: TreeModel;
  /** The selection the page shares between this tab and the Call Tree tab. */
  selectedId: number | null;
  onSelect: (id: number | null) => void;
}

function classNames(...names: (string | false | undefined)[]): string {
  return names.filter(Boolean).join(' ');
}

export function FlameView({ model, selectedId, onSelect }: FlameViewProps) {
  const [graphRef, width] = useElementWidth<HTMLDivElement>();
  // The node the graph is re-rooted on; null shows the whole call. Node ids
  // survive a model rebuild (Adjust, Setup), so the zoom does too.
  const [rootId, setRootId] = useState<number | null>(null);
  const [hoverId, setHoverId] = useState<number | null>(null);

  const zoomed = useMemo(
    () => (rootId === null ? null : findNodeById(model.root, rootId)),
    [model, rootId],
  );
  const root = zoomed ?? model.root;
  const layout = useMemo(() => flameLayout(root, { minWidth: 1 / width }), [root, width]);

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
    // Shares are of the whole call, also when the graph is re-rooted: a zoom
    // changes the picture, not what the numbers mean.
    const callMs = model.root.durationMs;
    const describe = (node: TreeNode): string =>
      `${names(node)} · ${formatDurationMs(node.durationMs)} · ${formatShare(shareOf(node.durationMs, callMs))} of the call` +
      ` · self ${formatDurationMs(node.selfDurationMs)} · ×${formatCount(node.selfExecutions)}`;
    return { names, describe };
  }, [model]);

  // The categories on screen, in the order the blocks meet them.
  const legend = useMemo(() => {
    const seen = new Set<CategoryDef>();
    for (const block of layout.blocks) if (block.category !== null) seen.add(block.category);
    return [...seen];
  }, [layout]);

  // Memoized apart from the hover state: moving the pointer re-renders the
  // readout, and React skips the unchanged blocks.
  const blocks = useMemo(
    () =>
      layout.blocks.map((block) => {
        const { node } = block;
        const isSelected = selectedId === node.id;
        const title = describe(node);
        const zoomIn = (): void => {
          // A double click arrives after two clicks, which select and then
          // clear. Select again, so the block that was zoomed on stays selected.
          setRootId(node === model.root ? null : node.id);
          onSelect(node.id);
        };
        return (
          <button
            key={node.id}
            type="button"
            className={classNames(styles.block, isSelected && styles.blockSelected)}
            style={{
              left: `${block.offset * 100}%`,
              width: `${block.width * 100}%`,
              top: block.depth * ROW_HEIGHT,
              background: block.category === null ? undefined : categoryBlockColor(block.category),
            }}
            title={title}
            aria-label={title}
            aria-pressed={isSelected}
            onMouseEnter={() => setHoverId(node.id)}
            onFocus={() => setHoverId(node.id)}
            onClick={() => onSelect(isSelected ? null : node.id)}
            onDoubleClick={zoomIn}
            onKeyDown={(e) => {
              // The keyboard's double click. Plain Enter is the click.
              if (e.key !== 'Enter' || !e.shiftKey) return;
              e.preventDefault();
              zoomIn();
            }}
          >
            {block.width * width >= LABEL_MIN_PX ? (
              <span className={styles.label}>
                {names(node)} · {formatDurationMs(node.durationMs)}
              </span>
            ) : null}
          </button>
        );
      }),
    [layout, width, selectedId, onSelect, model, names, describe],
  );

  const readoutId = hoverId ?? selectedId;
  const readoutNode = readoutId === null ? null : (layout.blocks.find((b) => b.node.id === readoutId)?.node ?? null);

  return (
    <div className={styles.flame}>
      <div className={styles.infoRow}>
        <span className={`pf-mono ${styles.readout}`}>
          {readoutNode !== null
            ? describe(readoutNode)
            : 'Hover a block for its numbers. Click one to select it.'}
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
      {zoomed !== null ? (
        <div className={styles.zoomRow}>
          <span>
            Zoomed in on <span className="pf-mono">{names(zoomed)}</span>.
          </span>
          <button type="button" className={styles.reset} onClick={() => setRootId(null)}>
            Reset to the whole call
          </button>
        </div>
      ) : null}
      <div className={styles.scroller}>
        <div
          ref={graphRef}
          className={styles.graph}
          style={{ height: layout.rows * ROW_HEIGHT }}
          onMouseLeave={() => setHoverId(null)}
        >
          {blocks}
        </div>
      </div>
      <div className={styles.caption}>
        The top block is the whole call. A block&apos;s width is its share of the call, and its children sit under it,
        in call order. The part of a block with nothing under it is the method&apos;s own time. Double-click a block,
        or press Shift+Enter on it, to zoom in.
      </div>
    </div>
  );
}
