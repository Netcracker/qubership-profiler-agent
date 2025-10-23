import type { CallsTreeInfo } from '@app/store/cdt-openapi';
import { UxProgress, type UxTableNewColumn, type UxTableNewData } from '@netcracker/ux-react';
import TraceButton from '../call-tree-entity/calls-tree-table/trace-button/trace-button';
import { useAppSelector } from '@app/store/hooks';
import { selectCallsTreeState } from '@app/store/slices/calls-tree-context-slices';
import prettyMilliseconds from 'pretty-ms';
import CallsTreeSearchedElement from '../call-tree-entity/calls-tree-table/calls-tree-searched-element';
import ParamsButton from '../call-tree-entity/calls-tree-table/params-button/params-button';

export type TableData = UxTableNewData<CallsTreeInfo>;

export const DEFAULT_CALLS_COLUMNS: UxTableNewColumn<TableData>[] = [
    {
        name: 'Method',
        type: 'accessor',
        dataKey: 'info',
        cellRender: props => <CallsTreeSearchedElement text={props.row.original.info.title} />,
        minWidth: 437,
    },
    {
        id: 'params',
        name: '',
        type: 'accessor',
        accessorFn: originalRow => {
            return originalRow.params;
        },
        cellRender: props => (props.row.original.params ? <ParamsButton row={props.row} /> : null),
        maxWidth: 43,
    },
    {
        id: 'hasStackTrace',
        name: '',
        type: 'accessor',
        accessorFn: originalRow => {
            return originalRow.info;
        },
        cellRender: props =>
            props.row.original.info.hasStackTrace ? <TraceButton text={props.row.original.info.trace || ''} /> : null,
        maxWidth: 43,
    },
    {
        id: 'totalTimePerc',
        name: 'Total time, %',
        type: 'accessor',
        accessorFn: originalRow => {
            return originalRow.info;
        },
        cellRender: props => (
            <UxProgress.Bar
                className="progress-bar"
                percent={props.row.original.timePercent}
                showInfo={true}
                infoPosition="right"
            />
        ),
    },
    {
        id: 'totalTime',
        name: 'Total Time',
        type: 'accessor',
        accessorFn: originalRow => {
            return originalRow.time;
        },
        cellRender: props => <CallsTreeSearchedElement text={prettyMilliseconds(props.row.original.time.total)} />,
    },
    {
        name: 'Total Suspension',
        type: 'accessor',
        dataKey: 'suspension',
        cellRender: props => <CallsTreeSearchedElement text={props.row.original.suspension.total.toString()} />,
    },
    {
        id: 'selfTime',
        name: 'Self Time',
        type: 'accessor',
        accessorFn: originalRow => {
            return originalRow.time;
        },
        cellRender: props => <CallsTreeSearchedElement text={prettyMilliseconds(props.row.original.time?.self)} />,
    },
    {
        id: 'selfSuspension',
        name: 'Self Suspension',
        type: 'accessor',
        accessorFn: originalRow => {
            return originalRow.suspension;
        },
        cellRender: props => <CallsTreeSearchedElement text={props.row.original.suspension.self.toString()} />,
    },
    {
        name: 'Invocations',
        type: 'accessor',
        dataKey: 'invocations',
        cellRender: props => <CallsTreeSearchedElement text={props.row.original.invocations.self.toString()} />,
    },
    {
        id: 'calls',
        name: 'Calls',
        type: 'accessor',
        accessorFn: originalRow => {
            return originalRow.info;
        },
        cellRender: props => <CallsTreeSearchedElement text={props.row.original.info.calls.toString()} />,
    },
];

export const CALLS_COLUMNS_KEYS = DEFAULT_CALLS_COLUMNS.map(column => column.name);

export function useSortedCallsColumns() {
    const { columnsOrder } = useAppSelector(selectCallsTreeState);
    return [...DEFAULT_CALLS_COLUMNS].sort(
        (a, b) => columnsOrder.indexOf(a.name as string) - columnsOrder.indexOf(b.name as string)
    );
}
export function useCallsColumns() {
    const { hiddenColumns } = useAppSelector(selectCallsTreeState);
    const sortedColumns = useSortedCallsColumns();
    return sortedColumns.filter(column => !hiddenColumns.includes(column.name));
}
