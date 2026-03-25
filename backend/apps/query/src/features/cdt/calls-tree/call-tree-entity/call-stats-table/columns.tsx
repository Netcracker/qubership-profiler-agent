import type { CallStatsInfo } from '@app/store/cdt-openapi';
import { type UxTableNewColumn, type UxTableNewData } from '@netcracker/ux-react';
import prettyMilliseconds from 'pretty-ms';

export type TableData = UxTableNewData<CallStatsInfo>;

export const columnsFactory = (): UxTableNewColumn<TableData>[] => [
    {
        name: 'Name',
        type: 'accessor',
        dataKey: 'name',
        width: 115,
        cellRender: props => {
            return props.getValue();
        },
    },
    {
        name: 'method itself',
        type: 'accessor',
        dataKey: 'self',
        width: 158,
        cellRender: props => {
            if (props.row.original.total) return prettyMilliseconds(Number(props.getValue()));
            return props.getValue();
        },
    },
    {
        name: 'with children',
        type: 'accessor',
        dataKey: 'total',
        width: 131,
        cellRender: props => {
            if (props.getValue()) {
                return prettyMilliseconds(props.getValue());
            }
        },
    },
];
