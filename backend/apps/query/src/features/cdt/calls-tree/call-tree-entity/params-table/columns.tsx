import type { CallParameter } from '@app/store/cdt-openapi';
import { type UxTableNewColumn, type UxTableNewData } from '@netcracker/ux-react';

export type TableData = UxTableNewData<CallParameter>;

export const columnsFactory = (): UxTableNewColumn<TableData>[] => [
    {
        name: 'Parameter',
        type: 'accessor',
        dataKey: 'id',
        cellRender: props => {
            return props.getValue();
        },
    },
];
