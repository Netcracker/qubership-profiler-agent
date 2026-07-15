import { useState } from 'react';
import type { ReactNode, UIEvent } from 'react';

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
}

export function VirtualList<T>({ items, rowHeight, height, overscan = 10, renderRow, itemKey }: VirtualListProps<T>) {
  const [scrollTop, setScrollTop] = useState(0);
  const first = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const last = Math.min(items.length, Math.ceil((scrollTop + height) / rowHeight) + overscan);

  return (
    <div
      style={{ height, overflow: 'auto', position: 'relative' }}
      onScroll={(e: UIEvent<HTMLDivElement>) => setScrollTop(e.currentTarget.scrollTop)}
    >
      <div style={{ height: items.length * rowHeight, position: 'relative' }}>
        {items.slice(first, last).map((item, i) => (
          <div
            key={itemKey(item)}
            style={{ position: 'absolute', top: (first + i) * rowHeight, height: rowHeight, left: 0, right: 0 }}
          >
            {renderRow(item, first + i)}
          </div>
        ))}
      </div>
    </div>
  );
}
