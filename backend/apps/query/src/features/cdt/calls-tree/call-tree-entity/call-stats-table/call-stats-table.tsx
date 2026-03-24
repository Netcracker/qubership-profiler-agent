import { useAppSelector } from '@app/store/hooks';
import { selectStatsState } from '@app/store/slices/calls-tree-context-slices';
import { UxTableNew } from '@netcracker/ux-react';
import { memo, useMemo, type FC } from 'react';
import { createCallStatsTableData, findTreeNode } from '../utils/calls-tree-operations';
import { columnsFactory, type TableData } from './columns';
import { useCallsTreeData } from '../../calls-tree-context';
import type { CallStatsInfo } from '@app/store/cdt-openapi';

const emptyCallsStats: CallStatsInfo[] = [];

const CallStatsTable: FC = () => {
    const { data, isFetching } = useCallsTreeData();

    const { selectedRowId } = useAppSelector(selectStatsState);

    const tableData = useMemo(() => {
        if (!selectedRowId || !data?.children) return emptyCallsStats;
        const node = findTreeNode(data?.children, selectedRowId);
        if (!node) return emptyCallsStats;
        return createCallStatsTableData(node);
    }, [data?.children, selectedRowId]);

    return (
        <div className="table-container">
            <UxTableNew<TableData>
                columns={columnsFactory()}
                data={tableData}
                loading={isFetching}
                className="ux-table"
            />
        </div>
    );
};

export default memo(CallStatsTable);
