import { ConfigProvider, Table, Typography } from 'antd';
import type { TableColumnType, TableProps } from 'antd';
import { useMemo, useState } from 'react';
import type { ReactNode, ThHTMLAttributes } from 'react';

import { pkToPath } from '../api/pk';
import type { CallJSON } from '../api/types';
import { useZone } from '../ui/timezone';
import { useElementHeight } from '../ui/use-element-height';
import { ColumnChooser } from './column-chooser';
import type { ColumnPrefs } from './column-prefs';
import { buildCallColumns } from './columns';
import type { ColumnHandlers, PickFilterField } from './columns';
import styles from './calls-table.module.css';

// AntD 6 Table with the built-in virtual scroller: keyset pages bound the
// loaded row count, so the DOM never holds more than the fetched pages and
// the virtualiser windows those. Header drag handles resize columns — AntD 6
// has no built-in column resize.

// The mockup's density: 28 px rows (20 px of text plus 4 px above and below)
// and 12 px cell padding at the sides. The compact tags are in the CSS module.
const DENSE_TABLE_THEME = { components: { Table: { cellPaddingBlockSM: 4, cellPaddingInlineSM: 12 } } };

const CHOOSER_COLUMN_WIDTH = 40;

type CallsTableChange = NonNullable<TableProps<CallJSON>['onChange']>;

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
    <th {...rest} className={[rest.className, styles.resizeCell].filter(Boolean).join(' ')}>
      {children}
      <span onMouseDown={startResize} className={styles.resizeHandle} />
    </th>
  );
}

export interface CallsTableProps {
  rows: CallJSON[];
  loading: boolean;
  prefs: ColumnPrefs;
  onPrefsChange: (prefs: ColumnPrefs) => void;
  handlers?: ColumnHandlers;
  /**
   * Called when the user clicks a row's pod or trace ID, to filter the calls by that value. Without it, the pod and
   * trace ID cells are plain text.
   */
  onPickFilter?: (field: PickFilterField, value: string) => void;
  /** Off in jsdom tests: the virtualiser needs real element sizes. */
  virtual?: boolean;
  /** The start of the footer line, such as "Load more" and the row count; the table adds its own sort note. */
  footer?: ReactNode;
}

// The header row holds the 24 px chooser button; the footer holds "Load more".
const HEADER_AND_FOOTER_PX = 33 + 48;

export function CallsTable({
  rows,
  loading,
  prefs,
  onPrefsChange,
  handlers,
  onPickFilter,
  virtual = true,
  footer,
}: CallsTableProps) {
  const [containerRef, containerHeight] = useElementHeight<HTMLDivElement>();
  const zone = useZone();
  // A column sort reorders the loaded rows only (09 §2.3), so the footer says so while one is on.
  const [columnSorted, setColumnSorted] = useState(false);

  const columns = useMemo<TableColumnType<CallJSON>[]>(() => {
    const defs = new Map(buildCallColumns({ ...handlers, onPickFilter }, zone).map((d) => [d.key, d]));
    const dataColumns: TableColumnType<CallJSON>[] = prefs.order
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
        showSorterTooltip: def.compare === undefined ? undefined : { title: `Sort the loaded rows by ${def.title}` },
        render: (_: unknown, call: CallJSON) => def.render(call),
        onHeaderCell: () =>
          ({
            'data-col-key': def.key,
            width: prefs.widths[def.key] ?? def.defaultWidth,
            onColumnResize: (key: string, width: number) =>
              onPrefsChange({ ...prefs, widths: { ...prefs.widths, [key]: width } }),
          }) as ResizableCellProps,
      }));
    return [
      ...dataColumns,
      {
        key: 'columnChooser',
        title: <ColumnChooser prefs={prefs} onPrefsChange={onPrefsChange} />,
        width: CHOOSER_COLUMN_WIDTH,
        fixed: 'right',
        align: 'center',
        render: () => null,
      },
    ];
  }, [prefs, handlers, onPickFilter, onPrefsChange, zone]);

  const totalWidth = columns.reduce((sum, c) => sum + (typeof c.width === 'number' ? c.width : 120), 0);

  const onChange: CallsTableChange = (_pagination, _filters, sorter) => {
    setColumnSorted((Array.isArray(sorter) ? sorter : [sorter]).some((s) => s.order != null));
  };

  const footerLine =
    footer === undefined && !columnSorted ? undefined : (
      <div className={styles.footer}>
        {footer}
        {columnSorted ? <Typography.Text type="secondary">Column sort applies to the loaded rows only</Typography.Text> : null}
      </div>
    );

  return (
    <div ref={containerRef} className={styles.tableContainer}>
      <ConfigProvider theme={DENSE_TABLE_THEME}>
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
          onChange={onChange}
          footer={footerLine === undefined ? undefined : () => footerLine}
        />
      </ConfigProvider>
    </div>
  );
}
