import { useAppDispatch } from '@app/store/hooks';
import { callsTreeContextDataAction } from '@app/store/slices/calls-tree-context-slices';
import { UxTableNew, type UxTableNewExpandedState, type UxTableNewRow } from '@netcracker/ux-react';
import { memo, useCallback, useEffect, useState, type FC } from 'react';
import { useCallsTreeData } from '../../calls-tree-context';
import { useCallsColumns, type TableData } from '../../hooks/use-calls-tree-columns';
import { useSearchParams } from 'react-router-dom';
import { ESC_CALL_TREE_QUERY_PARAMS } from '@app/constants/query-params';
import { getExpandedRowsBySearch } from '@app/features/cdt/calls-tree/call-tree-entity/utils/search-elements';

const CallsTreeTable: FC = () => {
    const columns = useCallsColumns();
    const { data, isFetching } = useCallsTreeData();
    const dispatch = useAppDispatch();
    const [urlParams] = useSearchParams();
    const callsTreeQuery = urlParams.get(ESC_CALL_TREE_QUERY_PARAMS.callsTreeQuery) || '';
    const [expandedState, setExpandedState] = useState<UxTableNewExpandedState>({});

    const handleSelect = useCallback((row: UxTableNewRow<TableData>) => {
        if (row.getIsSelected()) dispatch(callsTreeContextDataAction.unselectRow());
        else if (row.original.info?.title) {
            if (row.id.includes('_')) {
                const firstId = row.id.split('_').at(0);
                if (firstId) dispatch(callsTreeContextDataAction.selectRow([firstId, row.original.info?.title]));
            } else {
                dispatch(callsTreeContextDataAction.selectRow([row.id, row.original.info?.title]));
            }
        }
    }, []);

    const onExpandedRowsChange = useCallback((expandedState: UxTableNewExpandedState) => {
        setExpandedState(expandedState);
    }, []);
    useEffect(() => {
        if (typeof expandedState == 'object' && data) {
            const expandedRowsState = getExpandedRowsBySearch(callsTreeQuery)(data.children).reduce(
                (acc, item) => ({ ...acc, [item]: true }),
                expandedState
            );
            setExpandedState(expandedRowsState);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [callsTreeQuery, data]);

    return (
        <UxTableNew<TableData>
            columns={columns}
            data={data?.children as TableData[]}
            treeData
            enableResizing
            expandedRows={expandedState}
            onExpandedRowsChange={onExpandedRowsChange}
            loading={isFetching}
            virtualScroll="vertical"
            rowSelection={true}
            subRowsSelection={false}
            onSelect={row => handleSelect(row)}
        />
    );
};

export default memo(CallsTreeTable);
