import { useLayoutEffect, useRef, useState } from 'react';
import type { ReactNode, UIEvent } from 'react';

import styles from './virtual-list.module.css';

// Minimal fixed-row-height windowing: tree rows are uniform (params render
// as rows too), so no dynamic measurement is needed and jsdom can test it by
// passing an explicit height.

interface VirtualListProps<T> {
  items: readonly T[];
  rowHeight: number;
  height: number;
  overscan?: number;
  renderRow: (item: T, index: number) => ReactNode;
  itemKey: (item: T) => string | number;
  /**
   * Scrolls a row into view, once per `key`: a later change of its index (a
   * node above it opens) or a scroll by the user does not pull the list back.
   * An index of -1 means the row is not in the list yet.
   */
  ensureVisible?: { key: string | number; index: number };
}

export function VirtualList<T>({
  items,
  rowHeight,
  height,
  overscan = 10,
  renderRow,
  itemKey,
  ensureVisible,
}: VirtualListProps<T>) {
  const [scrollTop, setScrollTop] = useState(0);
  const viewportRef = useRef<HTMLDivElement | null>(null);
  const ensuredKey = useRef<string | number | null>(null);
  const ensureKey = ensureVisible?.key ?? null;
  const ensureIndex = ensureVisible?.index ?? -1;
  useLayoutEffect(() => {
    if (ensureKey === null) {
      ensuredKey.current = null;
      return;
    }
    const el = viewportRef.current;
    // A hidden tab pane has no height and ignores scrollTop, so the request
    // stays open until the list is on screen and holds the row.
    if (el === null || height <= 0 || ensureIndex < 0 || ensuredKey.current === ensureKey) return;
    ensuredKey.current = ensureKey;
    const top = ensureIndex * rowHeight;
    if (top >= el.scrollTop && top + rowHeight <= el.scrollTop + height) return;
    el.scrollTop = Math.max(0, top - (height - rowHeight) / 2);
    setScrollTop(el.scrollTop);
  }, [ensureKey, ensureIndex, height, rowHeight]);
  const first = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const last = Math.min(items.length, Math.ceil((scrollTop + height) / rowHeight) + overscan);

  return (
    <div
      ref={viewportRef}
      className={styles.viewport}
      style={{ height }}
      onScroll={(e: UIEvent<HTMLDivElement>) => setScrollTop(e.currentTarget.scrollTop)}
    >
      <div className={styles.spacer} style={{ height: items.length * rowHeight }}>
        {items.slice(first, last).map((item, i) => (
          <div
            key={itemKey(item)}
            className={styles.row}
            style={{ top: (first + i) * rowHeight, height: rowHeight }}
          >
            {renderRow(item, first + i)}
          </div>
        ))}
      </div>
    </div>
  );
}
