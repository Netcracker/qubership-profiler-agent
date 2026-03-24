import HighlightCell from '@app/components/highlight-cell/highlight-cell';
import type { StatsInfo } from '@app/store/cdt-openapi';
import { type UxTableNewColumn, type UxTableNewData } from '@netcracker/ux-react';

export type TableData = UxTableNewData<StatsInfo>;

export const columnsFactory = (): UxTableNewColumn<TableData>[] => [
    {
        name: '',
        type: 'accessor',
        dataKey: 'name',
        width: 200,
        cellRender: props => {
            return <span style={{ display: 'inline-flex', gap: 8 }}>{props.getValue()} </span>;
        },
    },
    {
        name: '',
        type: 'accessor',
        dataKey: 'totalTime',
        width: 110,
        cellRender: props => {
            return (
                props.getValue() && (
                    <span style={{ height: 13, display: 'inline-flex', gap: 8, fontWeight: 500 }}>
                        {props.getValue()}
                        {' ms'}
                    </span>
                )
            );
        },
    },
    {
        name: '',
        type: 'accessor',
        dataKey: 'totalTimePercent',
        width: 110,
        cellRender: props => {
            return (
                props.getValue() && (
                    <HighlightCell highlight={props.getValue() > 90} >
                        {'(' + props.getValue().toFixed(2) + '%)'}
                    </HighlightCell>
                )
            );
        },
    },
];
