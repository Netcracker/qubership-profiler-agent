import { Table } from 'antd';
import type { TableColumnType } from 'antd';
import { useMemo } from 'react';
import type { ReactNode, ThHTMLAttributes } from 'react';

import { pkToPath } from '../api/pk';
import type { CallJSON } from '../api/types';
import { useElementHeight } from '../ui/use-element-height';
import type { ColumnPrefs } from './column-prefs';
import { buildCallColumns } from './columns';
import type { ColumnHandlers } from './columns';

// AntD 6 Table with the built-in virtual scroller: keyset pages bound the
// loaded row count, so the DOM never holds more than the fetched pages and
// the virtualiser windows those. Header drag handles resize columns — AntD 6
// has no built-in column resize.

interface ResizableCellProps extends ThHTMLAttributes<HTMLTableCellElement> {
  'data-col-key'?: string;
  onColumnResize?: (key: string, width: number) => void;
  width?: number;
}

function ResizableHeaderCell({ onColumnResize, width, children, ...rest }: ResizableCellProps) {
  const key = rest['data-col-key'];
  if (key === undefined || onColumnResize === undefined) {
    return <th {...rest}>{children}</th>;
  }
  const startResize = (down: React.MouseEvent): void => {
    down.preventDefault();
    const startX = down.clientX;
    const startWidth = width ?? (down.currentTarget.parentElement as HTMLElement).offsetWidth;
    const onMove = (e: MouseEvent): void => {
      onColumnResize(key, Math.max(50, startWidth + e.clientX - startX));
    };
    const onUp = (): void => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  };
  return (
    <th {...rest} style={{ ...rest.style, position: 'relative' }}>
      {children}
      <span
        onMouseDown={startResize}
        style={{
          position: 'absolute',
          right: -4,
          top: 0,
          bottom: 0,
          width: 9,
          cursor: 'col-resize',
          zIndex: 1,
          userSelect: 'none',
        }}
      />
    </th>
  );
}

export interface CallsTableProps {
  rows: CallJSON[];
  loading: boolean;
  prefs: ColumnPrefs;
  onPrefsChange: (prefs: ColumnPrefs) => void;
  handlers?: ColumnHandlers;
  /** Off in jsdom tests: the virtualiser needs real element sizes. */
  virtual?: boolean;
  footer?: ReactNode;
}

const HEADER_AND_FOOTER_PX = 39 + 48;

export function CallsTable({ rows, loading, prefs, onPrefsChange, handlers, virtual = true, footer }: CallsTableProps) {
  const [containerRef, containerHeight] = useElementHeight<HTMLDivElement>();

  const columns = useMemo<TableColumnType<CallJSON>[]>(() => {
    const defs = new Map(buildCallColumns(handlers ?? {}).map((d) => [d.key, d]));
    return prefs.order
      .filter((key) => !prefs.hidden.includes(key))
      .map((key) => defs.get(key))
      .filter((def) => def !== undefined)
      .map((def) => ({
        key: def.key,
        title: def.title,
        width: prefs.widths[def.key] ?? def.defaultWidth,
        align: def.align,
        ellipsis: true,
        sorter: def.compare,
        // Cross-range ranking is 08 R2; make the local scope explicit.
        showSorterTooltip: def.compare === undefined ? undefined : { title: 'Sorts the loaded pages only' },
        render: (_: unknown, call: CallJSON) => def.render(call),
        onHeaderCell: () =>
          ({
            'data-col-key': def.key,
            width: prefs.widths[def.key] ?? def.defaultWidth,
            onColumnResize: (key: string, width: number) =>
              onPrefsChange({ ...prefs, widths: { ...prefs.widths, [key]: width } }),
          }) as ResizableCellProps,
      }));
  }, [prefs, handlers, onPrefsChange]);

  const totalWidth = columns.reduce((sum, c) => sum + (typeof c.width === 'number' ? c.width : 120), 0);

  return (
    <div ref={containerRef} style={{ flex: 1, minHeight: 200 }}>
      <Table<CallJSON>
        size="small"
        rowKey={(c) => pkToPath(c.pk)}
        columns={columns}
        dataSource={rows}
        loading={loading ? { spinning: true, description: 'Querying hot replicas + cold tier.' } : false}
        pagination={false}
        virtual={virtual}
        scroll={virtual ? { x: totalWidth, y: Math.max(160, containerHeight - HEADER_AND_FOOTER_PX) } : { x: totalWidth }}
        components={{ header: { cell: ResizableHeaderCell } }}
        footer={footer === undefined ? undefined : () => footer}
      />
    </div>
  );
}
