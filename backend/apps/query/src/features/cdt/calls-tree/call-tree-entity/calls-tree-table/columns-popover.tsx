import { CALLS_COLUMNS_KEYS, useSortedCallsColumns } from '@app/features/cdt/calls-tree/hooks/use-calls-tree-columns';
import { useAppDispatch, useAppSelector } from '@app/store/hooks';
import { callsTreeContextDataAction, selectCallsTreeState } from '@app/store/slices/calls-tree-context-slices';
import { type PropertiesItemModel } from '@netcracker/cse-ui-components';
import { PropertiesList } from '@netcracker/cse-ui-components/components/properties-list/properties-list';
import { ReactComponent as SettingsOutline20Icon } from '@netcracker/ux-assets/icons/settings/settings-outline-20.svg';
import { UxButton, UxIcon, UxPopoverNew } from '@netcracker/ux-react';
import { memo, useCallback, type Key, type ReactNode } from 'react';

const ColumnsPopover = () => {
    const { columnsOrder, hiddenColumns } = useAppSelector(selectCallsTreeState);
    const dispatch = useAppDispatch();

    const _columnsOrder = columnsOrder.length ? columnsOrder : CALLS_COLUMNS_KEYS;
    const sortedColumns = useSortedCallsColumns();

    const handleToggle = useCallback(
        (item: PropertiesItemModel<unknown>) => {
            if (hiddenColumns.includes(item.value)) {
                dispatch(callsTreeContextDataAction.setHiddenColumns(hiddenColumns.filter(c => c !== item.value)));
            } else {
                dispatch(callsTreeContextDataAction.setHiddenColumns([...hiddenColumns, item.value]));
            }
        },
        [hiddenColumns]
    );
    return (
        <UxPopoverNew
            placement={'bottom-end'}
            header={<span>Properties</span>}
            content={
                <>
                    <PropertiesList
                        // onReorder={handleReorder}
                        items={sortedColumns.map(col => ({
                            label: col.name as ReactNode,
                            value: col.name as Key,
                            data: col,
                            hidden: hiddenColumns.includes(col.name),
                        }))}
                        onToggle={handleToggle}
                    />
                </>
            }
        >
            <UxButton type="light" leftIcon={<UxIcon component={SettingsOutline20Icon} />} />
        </UxPopoverNew>
    );
};

export default memo(ColumnsPopover);
